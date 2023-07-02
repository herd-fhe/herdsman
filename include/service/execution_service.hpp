#ifndef HERDSMAN_EXECUTION_SERVICE_HPP
#define HERDSMAN_EXECUTION_SERVICE_HPP

#include <queue>
#include <unordered_map>
#include <shared_mutex>
#include <condition_variable>

#include "key_service.hpp"
#include "storage_service.hpp"
#include "execution/worker/i_worker_group.hpp"

#include "execution/execution_plan/execution_plan_analyzer.hpp"
#include "execution/executor/i_executor.hpp"
#include "herd/common/model/executor/execution_plan.hpp"
#include "herd/common/model/job.hpp"
#include "herd/common/uuid.hpp"
#include "model/task.hpp"


class ExecutionService
{
public:
	ExecutionService(
			KeyService& key_service, StorageService& storage_service
	);

	struct StageTask
	{
		enum class State
		{
			WAITING,
			PENDING,
			COMPLETED,
			FAILED
		};
		uint32_t partition;
		State state = State::WAITING;

		explicit StageTask(uint32_t partition)
		:	partition(partition)
		{};
	};

	struct StageProgress
	{
		herd::common::DAG<herd::common::stage_t>::Node<true> stage_node;
		std::vector<StageTask> pending_tasks;
	};

	struct JobDescriptor
	{
		JobDescriptor(
				const herd::common::UUID& uuid,
				herd::common::JobStatus status,
				herd::common::ExecutionPlan plan
		)
		: 	uuid(uuid),
			status(status),
			plan(std::move(plan))
		{}

		herd::common::UUID uuid;
		std::vector<StageProgress> pending_stages{};
		std::unordered_map<std::size_t, std::size_t> dependency_lookup;
		std::map<std::size_t, std::tuple<herd::common::UUID, uint64_t, uint32_t>> intermediate_stage_outputs{};
		herd::common::JobStatus status;

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
		std::optional<std::string> message;
	};

	ExecutionService::JobDescription schedule_job(const herd::common::UUID& session_uuid, const herd::common::ExecutionPlan& plan);

	JobExecutionState get_job_state(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);
	std::vector<JobExecutionState> get_job_states_for_session(const herd::common::UUID& session_uuid);

	JobDescription get_job_description(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);

	void set_executor(std::shared_ptr<executor::IExecutor> executor) noexcept;

	[[nodiscard]] std::optional<TaskKey> get_next_for_execution();
	[[nodiscard]] herd::common::task_t task_for_task_key(TaskKey key) const;

	void mark_task_completed(TaskKey key);
	void mark_task_failed(TaskKey key);

private:
	KeyService& key_service_;
	StorageService& storage_service_;

	mutable std::shared_mutex jobs_mutex_;
	std::condition_variable jobs_cv_;

	std::queue<std::pair<herd::common::UUID, std::shared_ptr<JobDescriptor>>> pending_jobs_;
	std::multimap<herd::common::UUID, std::shared_ptr<JobDescriptor>> job_descriptors_;

	std::shared_ptr<executor::IExecutor> executor_;

	void lock_required_resources(const herd::common::UUID& session_uuid, const execution_plan::ResourceRequirements& requirements);
	const JobDescriptor& get_job_descriptor(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid) const;
	JobDescriptor& get_job_descriptor(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);

	void initialize_job(const herd::common::UUID& session_uuid, ExecutionService::JobDescriptor& descriptor);
	void recalculate_waiting_tasks(JobDescriptor& descriptor);

	herd::common::task_t prepare_task(const JobDescriptor& descriptor, const TaskKey& key) const;
	herd::common::task_t build_map_task(const JobDescriptor& descriptor, const TaskKey& key) const;
};

#endif //HERDSMAN_EXECUTION_SERVICE_HPP
