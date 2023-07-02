#ifndef HERDSMAN_GRPC_WORKER_GROUP_HPP
#define HERDSMAN_GRPC_WORKER_GROUP_HPP

#include <thread>

#include <grpc++/grpc++.h>

#include "worker.grpc.pb.h"

#include "execution/worker/i_worker_group.hpp"
#include "utils/address.hpp"


class GrpcWorkerGroup: public IWorkerGroup
{
public:
	class GrpcTaskHandle: public TaskHandle
	{
	public:
		Status status() const noexcept override;

	private:
		friend class GrpcWorkerGroup;

		herd::proto::Empty response_;
		grpc::ClientContext context_;
		grpc::Status status_;
		std::unique_ptr<grpc::ClientAsyncResponseReader<herd::proto::Empty>> rpc_;

		void mark_completed() override;
	};

	explicit GrpcWorkerGroup(const std::vector<Address>& worker_addresses);
	~GrpcWorkerGroup();

	std::shared_ptr<TaskHandle> schedule_task(const herd::common::task_t& task) override;
	size_t concurrent_workers() const noexcept override;

private:
	struct Worker
	{
		std::shared_ptr<grpc::Channel> channel;
		std::unique_ptr<herd::proto::Worker::Stub> stub;

		Worker(std::shared_ptr<grpc::Channel> channel, std::unique_ptr<herd::proto::Worker::Stub> stub)
		:	channel(std::move(channel)), stub(std::move(stub))
		{}
	};

	std::jthread thread_;

	std::size_t current_worker_ = 0;
	std::vector<Worker> workers_;

	grpc::CompletionQueue completion_queue;

	std::mutex status_mutex_;
	std::unordered_map<GrpcTaskHandle*, std::shared_ptr<GrpcTaskHandle>> statuses_;

	static void thread_body(GrpcWorkerGroup& worker_group);
};


#endif //HERDSMAN_GRPC_WORKER_GROUP_HPP
