#ifndef HERDSMAN_EXECUTOR_HPP
#define HERDSMAN_EXECUTOR_HPP

#include <thread>
#include <mutex>
#include <condition_variable>

#include "i_executor.hpp"
#include "service/execution_service.hpp"
#include "execution/executor/event/event.hpp"

namespace executor
{
	class Executor: public IExecutor
	{
	public:
		constexpr static std::size_t RETRY_LIMIT = 3;

		explicit Executor(ExecutionService& execution_service) noexcept;
		~Executor();

		void send_event(ExecutorEvent event) override;
		void set_worker_group(std::unique_ptr<IWorkerGroup> worker_group) noexcept;

	private:
		ExecutionService& execution_service_;
		std::unique_ptr<IWorkerGroup> worker_group_;

		std::jthread executor_thread_;
		std::queue<ExecutorEvent> event_queue_;

		std::mutex event_queue_mutex_;
		std::condition_variable event_queue_cv_;

		std::mutex executor_state_mutex_;
		std::size_t pending_job_count_ = 0;
		bool shutting_down_ = false;

		std::map<TaskKey, std::size_t> retry_counter_;

		static void thread_body(Executor& executor);

		void schedule_task_on_worker(TaskKey key);
		void schedule_tasks_on_workers();

		void handle_job_scheduled(const event::JobScheduled& event);
		void handle_task_completed(const event::TaskCompleted& event);

		void dispatch_message(const ExecutorEvent& event);
	};
}

#endif //HERDSMAN_EXECUTOR_HPP
