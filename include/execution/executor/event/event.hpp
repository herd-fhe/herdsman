#ifndef HERDSMAN_EXECUTOR_EVENT_HPP
#define HERDSMAN_EXECUTOR_EVENT_HPP

#include <variant>

#include "execution/executor/event/job_scheduled.hpp"


namespace executor
{
	using ExecutorEvent = std::variant<event::JobScheduled, event::TaskCompleted>;
}

#endif //HERDSMAN_EXECUTOR_EVENT_HPP
