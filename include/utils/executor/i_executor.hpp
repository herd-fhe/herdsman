#ifndef HERDSMAN_I_EXECUTOR_HPP
#define HERDSMAN_I_EXECUTOR_HPP

#include "utils/executor/event/event.hpp"


namespace executor
{
	class IExecutor
	{
	public:
		virtual ~IExecutor() = default;

		virtual void send_event(ExecutorEvent event) = 0;
	};
}

#endif //HERDSMAN_I_EXECUTOR_HPP
