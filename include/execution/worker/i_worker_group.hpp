#ifndef HERDSMAN_I_WORKER_GROUP_HPP
#define HERDSMAN_I_WORKER_GROUP_HPP

#include <memory>
#include <mutex>
#include <functional>

#include "herd/common/model/worker/task.hpp"
#include "herd/common/uuid.hpp"


class IWorkerGroup
{
public:
	class TaskHandle
	{
	public:
		enum class Status
		{
			COMPLETED,
			PENDING,
			TIME_OUT,
			ERROR
		};

		virtual ~TaskHandle() noexcept = default;

		[[nodiscard]] bool completed() const noexcept;
		[[nodiscard]] virtual Status status() const noexcept = 0;

		void set_completion_callback(std::function<void(Status)> callback) noexcept;

	protected:
		virtual void mark_completed();

	private:
		std::function<void(Status)> callback_;

		std::mutex callback_mutex_;
		bool completed_ = false;

	};

	virtual ~IWorkerGroup() = default;

	virtual std::shared_ptr<TaskHandle> schedule_task(const herd::common::task_t& task) = 0;

	virtual std::size_t concurrent_workers() const noexcept = 0;
};

#endif //HERDSMAN_I_WORKER_GROUP_HPP
