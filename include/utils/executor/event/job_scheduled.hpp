#ifndef HERDSMAN_EXECUTOR_JOB_SCHEDULED_EVENT_HPP
#define HERDSMAN_EXECUTOR_JOB_SCHEDULED_EVENT_HPP

#include "herd/common/uuid.hpp"


namespace executor::event
{
	struct JobScheduled
	{
		herd::common::UUID job_uuid;
	};
}

#endif //HERDSMAN_EXECUTOR_JOB_SCHEDULED_EVENT_HPP
