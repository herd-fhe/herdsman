#ifndef HERDSMAN_EXECUTOR_EVENT_HPP
#define HERDSMAN_EXECUTOR_EVENT_HPP

#include <variant>

#include "utils/executor/event/job_scheduled.hpp"


namespace executor
{
	using ExecutorEvent = std::variant<event::JobScheduled>;
}

#endif //HERDSMAN_EXECUTOR_EVENT_HPP
