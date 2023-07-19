#ifndef HERDSMAN_TASK_HPP
#define HERDSMAN_TASK_HPP

#include "herd/common/uuid.hpp"

struct TaskKey
{
	herd::common::UUID session_uuid;
	herd::common::UUID job_uuid;
	std::size_t stage_node_id;
	uint32_t part;

	std::strong_ordering operator<=>(const TaskKey& other) const
	{
		std::strong_ordering cmp = session_uuid <=> other.session_uuid;
		if(cmp == std::strong_ordering::equal)
		{
			cmp = job_uuid <=> other.job_uuid;
		}
		if(cmp == std::strong_ordering::equal)
		{
			cmp = stage_node_id <=> other.stage_node_id;
		}
		if(cmp == std::strong_ordering::equal)
		{
			cmp = part <=> other.part;
		}

		return cmp;
	}
};


#endif //HERDSMAN_TASK_HPP
