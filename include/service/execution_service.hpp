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
			BLOCKED,
			PENDING,
			COMPLETED,
			FAILED
		};
		uint32_t partition;
		State state;

		StageTask(uint32_t task_partition, State task_state=State::WAITING)
		:	partition(task_partition), state(task_state)
		{};
	};

	struct ReduceStageState
	{
		std::optional<std::tuple<herd::common::UUID, uint64_t, uint32_t>> hidden_stage_output;
	};

	using stage_state_t = std::variant<std::monostate, ReduceStageState>;

	struct StageProgress
	{
		herd::common::DAG<herd::common::stage_t>::Node<true> stage_node;
		std::tuple<herd::common::UUID, uint64_t, uint32_t> stage_output;

		std::vector<StageTask> pending_tasks;

		stage_state_t state;

		StageProgress(
				herd::common::DAG<herd::common::stage_t>::Node<true> progress_stage_node,
				std::tuple<herd::common::UUID, uint64_t, uint32_t> progress_stage_output,
				stage_state_t progress_state
		)
		: stage_node(std::move(progress_stage_node)), stage_output(std::move(progress_stage_output)), state(std::move(progress_state))
		{}
	};

	struct JobDescriptor
	{
		JobDescriptor(
				const herd::common::UUID& job_uuid,
				herd::common::JobStatus job_status,
				herd::common::ExecutionPlan job_plan,
				std::size_t job_concurrency_limit)
		: 	uuid(job_uuid),
			status(job_status),
			plan(std::move(job_plan)),
			concurrency_limit(job_concurrency_limit)
		{}

		herd::common::UUID uuid;
		std::vector<std::size_t> pending_stage_ids{};
		std::unordered_map<std::size_t, std::size_t> dependency_lookup;
		std::map<std::size_t, StageProgress> stage_progress{};
		herd::common::JobStatus status;

		herd::common::ExecutionPlan plan;

		std::size_t concurrency_limit;
		std::size_t running_tasks = 0;
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

	ExecutionService::JobDescription schedule_job(const herd::common::UUID& session_uuid, const herd::common::ExecutionPlan& plan, std::size_t concurrency_limit);

	JobExecutionState get_job_state(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);
	std::vector<JobExecutionState> get_job_states_for_session(const herd::common::UUID& session_uuid);

	JobDescription get_job_description(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);

	void set_executor(std::shared_ptr<executor::IExecutor> executor) noexcept;

	[[nodiscard]] std::optional<TaskKey> get_next_for_execution();
	void mark_task_running(TaskKey key);
	[[nodiscard]] herd::common::task_t task_for_task_key(const TaskKey& key) const;

	void mark_task_completed(const TaskKey& key);
	void mark_task_failed(const TaskKey& key);

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
	StageTask& get_task(JobDescriptor& descriptor, std::size_t stage_node_id, uint32_t partition);

	void initialize_job(const herd::common::UUID& dependant_node, ExecutionService::JobDescriptor& descriptor);
	void initialize_input_stage(
			const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node,
			ExecutionService::JobDescriptor& descriptor, const herd::common::UUID& session_uuid
	);
	void initialize_output_stage(
			const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node,
			ExecutionService::JobDescriptor& descriptor
	);
	void initialize_map_stage(
			const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node,
			ExecutionService::JobDescriptor& descriptor, const herd::common::UUID& session_uuid
	);
	void initialize_reduce_stage(
			const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node,
			ExecutionService::JobDescriptor& descriptor, const herd::common::UUID& session_uuid
	);

	void recalculate_waiting_tasks(JobDescriptor& descriptor);

	herd::common::task_t prepare_task(const JobDescriptor& descriptor, const TaskKey& key) const;
	herd::common::task_t build_map_task(const JobDescriptor& descriptor, const TaskKey& key) const;
	herd::common::task_t build_reduce_task(const JobDescriptor& descriptor, const TaskKey& key) const;
};

#endif //HERDSMAN_EXECUTION_SERVICE_HPP
