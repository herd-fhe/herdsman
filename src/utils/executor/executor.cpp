#include "utils/executor/executor.hpp"
#include "spdlog/spdlog.h"

#include <functional>


namespace
{
	template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
	template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;
}

namespace executor
{
	Executor::Executor(ExecutionService& execution_service) noexcept
		: execution_service_(execution_service)
	{
		executor_thread_ = std::jthread([this](){
			Executor::thread_body(*this);
		});
	}

	Executor::~Executor()
	{
		shutting_down_ = true;

		event_queue_cv_.notify_one();
		executor_thread_.join();
	}

	void Executor::send_event(ExecutorEvent event)
	{
		std::unique_lock lock(event_queue_mutex_);

		event_queue_.emplace(event);
	}

	void Executor::thread_body(Executor& executor)
	{
		spdlog::info("Starting executor thread");
		std::optional<ExecutorEvent> event;
		while(true)
		{
			event = std::nullopt;
			{
				std::unique_lock lock(executor.event_queue_mutex_);

				executor.event_queue_cv_.wait(
					lock,
					[&queue=executor.event_queue_, &shutting_down=executor.shutting_down_]
					{
						return !queue.empty() || shutting_down;
					}
				);

				if(executor.shutting_down_)
				{
					return;
				}
				else
				{
					event = std::move(executor.event_queue_.front());
					executor.event_queue_.pop();
				}
			}
			if(event.has_value())
			{
				spdlog::trace("Received message with type id {}", event->index());
				executor.dispatch_message(event.value());
			}
		}
	}

	void Executor::handle_job_scheduled(const event::JobScheduled& event)
	{
		static_cast<void>(event);
	}

	void Executor::dispatch_message(const ExecutorEvent& event)
	{
		std::visit(
			overloaded{
				[this](const event::JobScheduled& job_scheduled) { handle_job_scheduled(job_scheduled); }
			},
			event
		);
	}
}