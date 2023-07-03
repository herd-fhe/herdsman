#include "execution/executor/executor.hpp"
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

	void Executor::set_worker_group(std::unique_ptr<IWorkerGroup> worker_group) noexcept
	{
		worker_group_ = std::move(worker_group);
	}

	void Executor::send_event(ExecutorEvent event)
	{
		{
			std::unique_lock lock(event_queue_mutex_);

			event_queue_.emplace(event);
		}
		event_queue_cv_.notify_one();
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
				spdlog::debug("Received message with type id {}", event->index());
				executor.dispatch_message(event.value());
			}
		}
	}

	void Executor::schedule_task_on_worker(const TaskKey& key)
	{
		const auto task = execution_service_.task_for_task_key(key);
		auto task_handle = worker_group_->schedule_task(task);

		task_handle->set_completion_callback(
				[this, key](IWorkerGroup::TaskHandle::Status status)
				{
					this->send_event(ExecutorEvent(event::TaskCompleted{key, status}));
				}
		);
	}

	void Executor::schedule_tasks_on_workers()
	{
		std::size_t job_count = 0;
		for(; job_count < std::max(0UL, worker_group_->concurrent_workers() - pending_job_count_); ++job_count)
		{
			const auto task_entry = execution_service_.get_next_for_execution();
			if(task_entry.has_value())
			{
				schedule_task_on_worker(task_entry.value());
			}
		}

		pending_job_count_ += job_count;
	}

	void Executor::handle_job_scheduled([[maybe_unused]] const event::JobScheduled& event)
	{
		std::unique_lock lock(executor_state_mutex_);
		schedule_tasks_on_workers();
	}

	void Executor::handle_task_completed(const event::TaskCompleted& event)
	{
		std::unique_lock lock(executor_state_mutex_);

		if(event.status == IWorkerGroup::TaskHandle::Status::COMPLETED)
		{
			execution_service_.mark_task_completed(event.key);
			--pending_job_count_;
		}
		else if(event.status == IWorkerGroup::TaskHandle::Status::TIME_OUT)
		{
			if(!retry_counter_.contains(event.key))
			{
				retry_counter_.try_emplace(event.key, 0);
			}

			const std::size_t count = retry_counter_[event.key];

			if(count < RETRY_LIMIT)
			{
				schedule_task_on_worker(event.key);
				++retry_counter_[event.key];
			}
			else
			{
				execution_service_.mark_task_failed(event.key);
			}
		}

		schedule_tasks_on_workers();
	}

	void Executor::dispatch_message(const ExecutorEvent& event)
	{
		std::visit(
			overloaded{
				[this](const event::JobScheduled& job_scheduled) { handle_job_scheduled(job_scheduled); },
				[this](const event::TaskCompleted& task_completed) { handle_task_completed(task_completed); }
			},
			event
		);
	}
}