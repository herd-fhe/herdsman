#ifndef HERDSMAN_LAMBDA_HTTP_WORKER_GROUP_HPP
#define HERDSMAN_LAMBDA_HTTP_WORKER_GROUP_HPP

#include <curl/multi.h>

#include <thread>
#include <queue>

#include "execution/worker/i_worker_group.hpp"

#include "utils/address.hpp"


class LambdaWorkerGroup final: public IWorkerGroup
{
public:
	class LambdaWorkerGroupTaskHandle final: public TaskHandle
	{
	public:
		Status status() const noexcept override;

	private:
		friend class LambdaWorkerGroup;

		Status status_ = Status::PENDING;

		CURL* http_handle_ = nullptr;
		struct curl_slist* headers_ = nullptr;

		void mark_completed() override;
	};

	LambdaWorkerGroup(const Address& lambda_url, std::size_t concurrency_limit);
	~LambdaWorkerGroup();

	std::shared_ptr<TaskHandle> schedule_task(const herd::common::task_t& task) override;
	size_t concurrent_workers() const noexcept override;

private:
	std::jthread thread_;
	std::atomic_flag closed_;

	Address lambda_address_;
	std::size_t concurrency_limit_ = 1;

	std::mutex queue_mutex_;
	std::queue<std::shared_ptr<LambdaWorkerGroupTaskHandle>> handle_queue_;

	std::unordered_map<void*, std::shared_ptr<LambdaWorkerGroupTaskHandle>> statuses_;

	CURLM* multi_handle_;

	static void thread_body(LambdaWorkerGroup& worker_group);
};

#endif //HERDSMAN_LAMBDA_HTTP_WORKER_GROUP_HPP
