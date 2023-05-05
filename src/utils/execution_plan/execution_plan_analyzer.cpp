#include "utils/execution_plan/execution_plan_analyzer.hpp"

#include <algorithm>


namespace execution_plan
{
	ResourceRequirements analyze_required_resources(const herd::common::ExecutionPlan& plan)
	{
		ResourceRequirements requirements {};
		std::ranges::for_each(
				plan.stages,
				[&requirements](const herd::common::ExecutionPlan::Stage& stage)
				{
					if (stage.required_data_frame.has_value())
					{
						requirements.required_data_frames.insert(stage.required_data_frame.value());
					}
					requirements.required_keys.insert(stage.schema_type);
				}
        );
		return requirements;
	}
}