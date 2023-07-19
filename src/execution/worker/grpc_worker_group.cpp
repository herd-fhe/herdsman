#include "execution/worker/grpc_worker_group.hpp"

#include "herd/mapper/worker.hpp"
#include "spdlog/spdlog.h"


void GrpcWorkerGroup::GrpcTaskHandle::mark_completed()
{
	spdlog::info("Worker completed execution with status: {} {}", status_.ok() ? "OK" : "NOT OK", status_.error_message());
	TaskHandle::mark_completed();
}

GrpcWorkerGroup::GrpcWorkerGroup(const std::vector<Address>& worker_addresses)
{
	workers_.reserve(worker_addresses.size());
	spdlog::info("Discovered {} workers", worker_addresses.size());

	for(auto& address: worker_addresses)
	{
		const auto address_str = address.hostname + ":" + std::to_string(address.port);
		auto channel = grpc::CreateChannel(address_str, grpc::InsecureChannelCredentials());
		auto stub = herd::proto::Worker::NewStub(channel);
		workers_.emplace_back(channel, std::move(stub));
		spdlog::info("Worker: grpc - {}", address_str);
	}

	thread_ = std::jthread([group = this]()
	{
			thread_body(*group);
	});
}

GrpcWorkerGroup::~GrpcWorkerGroup()
{
	completion_queue.Shutdown();
	thread_.join();
}

void GrpcWorkerGroup::thread_body(GrpcWorkerGroup& worker_group)
{
	GrpcTaskHandle* tag = nullptr;
	bool ok = false;

	while(true)
	{
		if(!worker_group.completion_queue.Next(std::bit_cast<void**>(&tag), &ok))
		{
			return;
		}

		{
			std::unique_lock statuses_lock(worker_group.status_mutex_);

			assert(worker_group.statuses_.contains(tag));

			const auto& status = worker_group.statuses_[tag];

			status->mark_completed();
		}
	}
}

std::shared_ptr<IWorkerGroup::TaskHandle> GrpcWorkerGroup::schedule_task(const herd::common::task_t& task)
{
	std::unique_lock lock(status_mutex_);

	auto& worker = workers_[current_worker_];

	auto handle = std::make_shared<GrpcTaskHandle>();
	const auto tag = handle.get();

	if(std::holds_alternative<herd::common::MapTask>(task))
	{
		const auto& map_task = std::get<herd::common::MapTask>(task);
		const auto map_task_proto = herd::mapper::to_proto(map_task);
		auto remote_call = worker.stub->Asyncmap(&handle->context_, map_task_proto, &completion_queue);
		remote_call->Finish(&handle->response_, &handle->status_, std::bit_cast<void*>(tag));
		handle->rpc_ = std::move(remote_call);
		spdlog::info("Mapper task scheduled on worker: {}", current_worker_);
	}
	else if(std::holds_alternative<herd::common::ReduceTask>(task))
	{
		const auto& reduce_task = std::get<herd::common::ReduceTask>(task);
		const auto reduce_task_proto = herd::mapper::to_proto(reduce_task);
		auto remote_call = worker.stub->Asyncreduce(&handle->context_, reduce_task_proto, &completion_queue);
		remote_call->Finish(&handle->response_, &handle->status_, std::bit_cast<void*>(tag));
		handle->rpc_ = std::move(remote_call);
		spdlog::info("Reduce task scheduled on worker: {}", current_worker_);
	}

	statuses_.try_emplace(tag, handle);

	current_worker_ = (current_worker_ + 1) % workers_.size();

	return handle;
}

size_t GrpcWorkerGroup::concurrent_workers() const noexcept
{
	return workers_.size();
}

IWorkerGroup::TaskHandle::Status GrpcWorkerGroup::GrpcTaskHandle::status() const noexcept
{
	using enum Status;
	if(!completed())
	{
		return PENDING;
	}
	else
	{
		if(status_.ok())
		{
			return COMPLETED;
		}
		else
		{
			return ERROR;
		}
	}
}

