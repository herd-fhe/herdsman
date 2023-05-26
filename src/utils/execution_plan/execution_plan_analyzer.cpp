#include "utils/execution_plan/execution_plan_analyzer.hpp"


namespace execution_plan
{
	ResourceRequirements analyze_required_resources(const herd::common::ExecutionPlan& plan)
	{
		ResourceRequirements requirements {};
		requirements.required_keys.insert(plan.schema_type);

		for(const auto node: plan.execution_graph)
		{
			const auto& stage = node.value();
			if(std::holds_alternative<herd::common::InputStage>(stage))
			{
				const auto& input_stage = std::get<herd::common::InputStage>(stage);
				requirements.required_data_frames.insert(input_stage.data_frame_uuid);
			}
		}

		return requirements;
	}
}