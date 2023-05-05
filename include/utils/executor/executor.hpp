#ifndef HERDSMAN_EXECUTOR_HPP
#define HERDSMAN_EXECUTOR_HPP

#include <thread>
#include <mutex>
#include <condition_variable>

#include "i_executor.hpp"
#include "service/execution_service.hpp"
#include "utils/executor/event/event.hpp"

namespace executor
{
	class Executor: public IExecutor
	{
	public:
		explicit Executor(ExecutionService& execution_service) noexcept;
		~Executor();

		void send_event(ExecutorEvent event) override;

	private:
		ExecutionService& execution_service_;

		std::jthread executor_thread_;
		std::queue<ExecutorEvent> event_queue_;

		std::mutex event_queue_mutex_;
		std::condition_variable event_queue_cv_;

		bool shutting_down_ = false;

		static void thread_body(Executor& executor);

		void handle_job_scheduled(const event::JobScheduled& event);

		void dispatch_message(const ExecutorEvent& event);
	};
}

#endif //HERDSMAN_EXECUTOR_HPP
