#ifndef HERDSMAN_LAMBDA_HTTP_WORKER_GROUP_HPP
#define HERDSMAN_LAMBDA_HTTP_WORKER_GROUP_HPP

#include <curl/multi.h>

#include <thread>
#include <queue>
#include <filesystem>

#include "execution/worker/i_worker_group.hpp"
#include "filesystem_watch.hpp"

#include "utils/address.hpp"


class LambdaWorkerGroup final: public IWorkerGroup
{
public:
	class LambdaWorkerGroupTaskHandle final: public TaskHandle
	{
	public:
		Status status() const noexcept override;
		[[nodiscard]] const std::filesystem::path& outpath() const noexcept;

	private:
		friend class LambdaWorkerGroup;

		Status status_ = Status::PENDING;

		CURL* http_handle_ = nullptr;
		struct curl_slist* headers_ = nullptr;

		std::filesystem::path outpath_{};

		void mark_completed() override;
	};

	LambdaWorkerGroup(const Address& lambda_url, std::size_t concurrency_limit, const std::filesystem::path& storage_dir);
	~LambdaWorkerGroup() override;

	std::shared_ptr<TaskHandle> schedule_task(const herd::common::task_t& task) override;
	size_t concurrent_workers() const noexcept override;

private:
	std::jthread thread_;
	std::atomic_flag closed_;

	Address lambda_address_;
	std::size_t concurrency_limit_ = 1;
	std::filesystem::path storage_dir_;

	std::mutex queue_mutex_;
	std::queue<std::shared_ptr<LambdaWorkerGroupTaskHandle>> handle_queue_;

	FilesystemWatch watch_;

	std::unordered_map<void*, std::shared_ptr<LambdaWorkerGroupTaskHandle>> statuses_by_handle_;
	std::unordered_map<std::filesystem::path, std::shared_ptr<LambdaWorkerGroupTaskHandle>> statuses_by_outpath_;

	CURLM* multi_handle_;

	static void thread_body(LambdaWorkerGroup& worker_group);
};

#endif //HERDSMAN_LAMBDA_HTTP_WORKER_GROUP_HPP
