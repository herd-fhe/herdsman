#ifndef HERDSMAN_EXECUTOR_JOB_SCHEDULED_EVENT_HPP
#define HERDSMAN_EXECUTOR_JOB_SCHEDULED_EVENT_HPP

#include "herd/common/uuid.hpp"
#include "model/task.hpp"
#include "execution/worker/i_worker_group.hpp"

namespace executor::event
{
	struct JobScheduled
	{};

	struct TaskCompleted
	{
		TaskKey key;
		IWorkerGroup::TaskHandle::Status status;
	};
}

#endif //HERDSMAN_EXECUTOR_JOB_SCHEDULED_EVENT_HPP
