#include "service/execution_service.hpp"
#include "service/common_exceptions.hpp"

#include <algorithm>


ExecutionService::ExecutionService(KeyService& key_service, StorageService& storage_service)
	: key_service_(key_service), storage_service_(storage_service)
{
}

ExecutionService::JobDescription ExecutionService::schedule_job(const herd::common::UUID& session_uuid, const herd::common::ExecutionPlan& plan)
{
	const herd::common::UUID job_uuid = {};

	{
		std::unique_lock lock(jobs_mutex_);

		const auto resource_requirements = execution_plan::analyze_required_resources(plan);
		lock_required_resources(session_uuid, resource_requirements);

		job_descriptors_.emplace(
				session_uuid,
				std::make_shared<JobDescriptor>(
						job_uuid,
						0, herd::common::JobStatus::WAITING_FOR_EXECUTION,
						0, //todo: calculate estimated complexity
						plan)
		);
	}
	executor_->send_event(executor::event::JobScheduled());

	return JobDescription{
			job_uuid,
			plan,
			0 //todo: calculate estimated complexity
	};
}

ExecutionService::JobDescription ExecutionService::get_job_description(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid)
{
	std::shared_lock lock(jobs_mutex_);

	const auto [jobs_iter_beg, jobs_iter_end] = job_descriptors_.equal_range(session_uuid);
	const auto job_iter = std::find_if(
			jobs_iter_beg, jobs_iter_end,
			[job_uuid](const auto& entry)
			{
				return entry.second->uuid == job_uuid;
			}
	);

	if (job_iter == jobs_iter_end)
	{
		throw ObjectNotFoundException("No job with provided uuid found");
	}

	const auto& job = job_iter->second;

	return JobDescription{
			job->uuid,
			job->plan,
			0 // todo: estimated complexity
	};
}

ExecutionService::JobExecutionState ExecutionService::get_job_state(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid)
{
	std::shared_lock lock(jobs_mutex_);

	const auto [jobs_iter_beg, jobs_iter_end] = job_descriptors_.equal_range(session_uuid);
	const auto job_iter = std::find_if(
			jobs_iter_beg, jobs_iter_end,
			[job_uuid](const auto& entry)
			{
				return entry.second->uuid == job_uuid;
			}
    );

	if (job_iter == jobs_iter_end)
	{
		throw ObjectNotFoundException("No job with provided uuid found");
	}

	const auto& job = job_iter->second;

	return JobExecutionState{
			job->uuid,
			job->status,
			job->current_stage,
			std::nullopt //todo: place to inject failure reporting
	};
}

std::vector<ExecutionService::JobExecutionState> ExecutionService::get_job_states_for_session(const herd::common::UUID& session_uuid)
{
	std::shared_lock lock(jobs_mutex_);

	const auto [jobs_iter_beg, jobs_iter_end] = job_descriptors_.equal_range(session_uuid);

	std::vector<ExecutionService::JobExecutionState> execution_states;
	execution_states.reserve(static_cast<std::size_t>(std::distance(jobs_iter_beg, jobs_iter_end)));

	std::transform(
			jobs_iter_beg, jobs_iter_end,
			std::back_inserter(execution_states),
			[](const auto& entry)
			{
				const auto& job = entry.second;

				return JobExecutionState{
						job->uuid,
						job->status,
						job->current_stage,
						std::nullopt //todo: place to inject failure reporting
				};
			}
	);

	return execution_states;
}

void ExecutionService::lock_required_resources(const herd::common::UUID& session_uuid, const execution_plan::ResourceRequirements& requirements)
{
	for(const auto& key: requirements.required_keys)
	{
		key_service_.lock_key(session_uuid, key);
	}

	for(const auto& data_frame: requirements.required_data_frames)
	{
		storage_service_.lock_data_frame(session_uuid, data_frame);
	}
}

void ExecutionService::set_executor(std::shared_ptr<executor::IExecutor> executor) noexcept
{
	executor_ = std::move(executor);
}

