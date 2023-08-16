#include "execution/worker/lambda_http_worker_group.hpp"

#include <curl/multi.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "herd/mapper/worker.hpp"


void LambdaWorkerGroup::LambdaWorkerGroupTaskHandle::mark_completed()
{
	long response_code = 0;
	curl_easy_getinfo(http_handle_, CURLINFO_RESPONSE_CODE, &response_code);

	status_ = response_code == 200 ? Status::COMPLETED : Status::ERROR;

	curl_easy_cleanup(http_handle_);
	curl_slist_free_all(headers_);
	headers_ = nullptr;
	http_handle_ = nullptr;

	spdlog::info("Worker completed execution with status: {} {}", response_code, response_code == 200 ? "OK" : "NOT OK");

	TaskHandle::mark_completed();
}

IWorkerGroup::TaskHandle::Status LambdaWorkerGroup::LambdaWorkerGroupTaskHandle::status() const noexcept
{
	return status_;
}

namespace
{
	std::string build_map_payload(const herd::common::MapTask& map_task)
	{
		nlohmann::json payload;
		payload["type"] = "MAP";

		{
			const auto map_task_proto = herd::mapper::to_proto(map_task);
			const auto byte_size = map_task_proto.ByteSizeLong();

			std::vector<uint8_t> data_array;
			data_array.resize(byte_size);

			map_task_proto.SerializeToArray(data_array.data(), static_cast<int>(byte_size));

			payload["data"] = data_array;
		}
		return payload.dump();
	}

	std::string build_reduce_payload(const herd::common::ReduceTask& reduce_task)
	{
		nlohmann::json payload;
		payload["type"] = "REDCUE";

		{
			const auto reduce_task_proto = herd::mapper::to_proto(reduce_task);
			const auto byte_size = reduce_task_proto.ByteSizeLong();

			std::vector<uint8_t> data_array;
			data_array.resize(byte_size);

			reduce_task_proto.SerializeToArray(data_array.data(), static_cast<int>(byte_size));

			payload["data"] = data_array;
		}

		return payload.dump();
	}

	std::string build_task_payload(const herd::common::task_t& task)
	{
		if(std::holds_alternative<herd::common::MapTask>(task))
		{
			const auto& map_task = std::get<herd::common::MapTask>(task);
			return build_map_payload(map_task);
		}
		else if(std::holds_alternative<herd::common::ReduceTask>(task))
		{
			const auto& reduce_task = std::get<herd::common::ReduceTask>(task);
			return build_reduce_payload(reduce_task);
		}
		else
		{
			assert(false && "Unsupported payload type");
			return "";
		}
	}
}

LambdaWorkerGroup::LambdaWorkerGroup(const Address& lambda_address, std::size_t concurrency_limit)
	: lambda_address_(lambda_address), concurrency_limit_(concurrency_limit)
{
	spdlog::debug("Initializing curl");
	if(curl_global_init(CURL_GLOBAL_ALL)) {
		throw std::runtime_error("Curl initialization failed");
	}

	spdlog::debug("Lambda worker - concurrency limit: {}", concurrency_limit);

	multi_handle_ = curl_multi_init();
	curl_multi_setopt(multi_handle_, CURLMOPT_MAX_TOTAL_CONNECTIONS, concurrency_limit);

	thread_ = std::jthread([group = this]()
	{
		thread_body(*group);
	});
}

void LambdaWorkerGroup::thread_body(LambdaWorkerGroup& worker_group)
{
	while(true)
	{
		if(worker_group.closed_.test())
		{
			return;
		}

		{
			int running = 0;

			if(curl_multi_perform(worker_group.multi_handle_, &running))
			{
				spdlog::error("Lambda worker - Perform. Internal error");
				return;
			}

			spdlog::debug("Lambda worker - {} calls in progress", running);

			if(curl_multi_poll(worker_group.multi_handle_, nullptr, 0, 5000, nullptr))
			{
				spdlog::error("Lambda worker - Poll. Internal error");
				return;
			}

			int message_count = 0;
			CURLMsg* message;
			while((message = curl_multi_info_read(worker_group.multi_handle_, &message_count)))
			{
				if(message->msg == CURLMSG_DONE)
				{
					const auto& handle = message->easy_handle;
					const auto& status = worker_group.statuses_[handle];

					curl_multi_remove_handle(worker_group.multi_handle_, handle);

					status->mark_completed();

					worker_group.statuses_.erase(handle);
				}
			}
		}

		{
			std::unique_lock lock(worker_group.queue_mutex_);

			while(!worker_group.handle_queue_.empty())
			{
				auto handle = worker_group.handle_queue_.front();
				worker_group.handle_queue_.pop();

				curl_multi_add_handle(worker_group.multi_handle_, handle->http_handle_);
				worker_group.statuses_.try_emplace(handle->http_handle_, handle);
			}
		}
	}
}

std::shared_ptr<IWorkerGroup::TaskHandle> LambdaWorkerGroup::schedule_task(const herd::common::task_t& task)
{
	CURL* handle = curl_easy_init();

	struct curl_slist* headers = nullptr;
	headers = curl_slist_append(headers, "Content-Type: application/json");

	const auto url = lambda_address_.hostname + ":" + std::to_string(lambda_address_.port);
	curl_easy_setopt(handle, CURLOPT_URL, url.c_str());

	curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);

	{
		const auto payload = build_task_payload(task);
		curl_easy_setopt(handle, CURLOPT_COPYPOSTFIELDS, payload.data());
	}

	auto task_handle = std::make_shared<LambdaWorkerGroupTaskHandle>();

	task_handle->headers_ = headers;
	task_handle->http_handle_ = handle;

	{
		std::unique_lock lock(queue_mutex_);
		handle_queue_.push(task_handle);
	}

	return task_handle;
}

LambdaWorkerGroup::~LambdaWorkerGroup()
{
	closed_.test_and_set();
	thread_.join();
}

size_t LambdaWorkerGroup::concurrent_workers() const noexcept
{
	return concurrency_limit_;
}
