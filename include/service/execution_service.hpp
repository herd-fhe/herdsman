#ifndef HERDSMAN_EXECUTION_SERVICE_HPP
#define HERDSMAN_EXECUTION_SERVICE_HPP

#include <queue>
#include <unordered_map>
#include <shared_mutex>
#include <condition_variable>

#include "key_service.hpp"
#include "storage_service.hpp"
#include "i_worker_service.hpp"

#include "herd/common/uuid.hpp"
#include "herd/common/model/job.hpp"
#include "herd/common/model/executor/execution_plan.hpp"
#include "utils/execution_plan/execution_plan_analyzer.hpp"
#include "utils/executor/i_executor.hpp"


class ExecutionService
{
public:
	ExecutionService(
			KeyService& key_service, StorageService& storage_service
	);



	struct JobDescriptor
	{
		JobDescriptor(
				const herd::common::UUID& job_uuid,
				uint8_t current_job_stage, herd::common::JobStatus job_status,
				uint64_t estimated_job_complexity, const herd::common::ExecutionPlan& job_plan)
				: uuid(job_uuid), current_stage(current_job_stage),
				status(job_status), estimated_complexity(estimated_job_complexity),
				plan(job_plan)
		{}

		herd::common::UUID uuid;
		uint8_t current_stage;
		herd::common::JobStatus status;
		uint64_t estimated_complexity;

		herd::common::ExecutionPlan plan;
	};

	struct JobDescription
	{
		herd::common::UUID uuid;
		herd::common::ExecutionPlan plan;
		uint64_t estimated_complexity;
	};

	struct JobExecutionState
	{
		herd::common::UUID uuid;
		herd::common::JobStatus status;
		std::optional<uint8_t> current_stage;
		std::optional<std::string> message;
	};

	ExecutionService::JobDescription schedule_job(const herd::common::UUID& session_uuid, const herd::common::ExecutionPlan& plan);

	JobExecutionState get_job_state(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);
	std::vector<JobExecutionState> get_job_states_for_session(const herd::common::UUID& session_uuid);

	JobDescription get_job_description(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);

	void set_executor(std::shared_ptr<executor::IExecutor> executor) noexcept;

private:
	KeyService& key_service_;
	StorageService& storage_service_;

	std::shared_mutex jobs_mutex_;
	std::condition_variable jobs_cv_;

	std::multimap<herd::common::UUID, std::shared_ptr<JobDescriptor>> job_descriptors_;
	std::queue<std::shared_ptr<JobDescriptor>> jobs_queue_;

	std::shared_ptr<executor::IExecutor> executor_;

	void lock_required_resources(const herd::common::UUID& session_uuid, const execution_plan::ResourceRequirements& requirements);
};

#endif //HERDSMAN_EXECUTION_SERVICE_HPP
