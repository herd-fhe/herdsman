#ifndef HERDSMAN_EXECUTION_PLAN_ANALYZER_HPP
#define HERDSMAN_EXECUTION_PLAN_ANALYZER_HPP

#include <unordered_set>

#include "herd/common/model/schema_type.hpp"
#include "herd/common/model/execution_plan.hpp"
#include "herd/common/uuid.hpp"


namespace execution_plan
{
	struct ResourceRequirements
	{
		using KeyIdentifier = herd::common::SchemaType;
		using DataFrameIdentifier = herd::common::UUID;

		std::unordered_set<KeyIdentifier> required_keys;
		std::unordered_set<DataFrameIdentifier> required_data_frames;
	};

	ResourceRequirements analyze_required_resources(const herd::common::ExecutionPlan& execution_plan);
}

#endif //HERDSMAN_EXECUTION_PLAN_ANALYZER_HPP
