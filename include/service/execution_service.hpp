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
		};
		uint32_t key;
		State state;

		StageTask(uint32_t task_partition, State task_state=State::WAITING)
		:
			key(task_partition), state(task_state)
		{};
	};

	struct ReduceStageState
	{
		std::optional<std::tuple<herd::common::UUID, uint64_t, uint32_t>> hidden_stage_output;
	};

	using stage_state_t = std::variant<std::monostate, ReduceStageState>;

	struct JobDescriptor;

	class BaseStageProgress
	{
	public:
		BaseStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node,
						  ExecutionService::JobDescriptor& job_descriptor,
						  ExecutionService& execution_service)
			: stage_node_(std::move(stage_node)), job_(job_descriptor), execution_service_(execution_service)
		{}

		virtual void mark_task_completed(uint32_t task_key);
		void mark_task_running(uint32_t task_key);

		std::optional<TaskKey> get_next_for_execution();

		virtual herd::common::task_t build_task(const TaskKey& key) const;

		std::tuple<herd::common::UUID, uint64_t, uint32_t> stage_output() const;
		bool is_completed() const;

		virtual ~BaseStageProgress() = default;

	protected:
		herd::common::DAG<herd::common::stage_t>::Node<true> stage_node_;
		ExecutionService::JobDescriptor& job_;
		ExecutionService& execution_service_;

		std::tuple<herd::common::UUID, uint64_t, uint32_t> stage_output_;

		std::vector<StageTask> pending_tasks_;

		stage_state_t state;
	};

	class InputStageProgress: public BaseStageProgress
	{
	public:
		InputStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node,
							ExecutionService::JobDescriptor& job_descriptor,
							ExecutionService& execution_service);
	};

	class OutputStageProgress: public BaseStageProgress
	{
	public:
		OutputStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node,
						   ExecutionService::JobDescriptor& job_descriptor,
						   ExecutionService& execution_service);
	};

	class MapperStageProgress: public BaseStageProgress
	{
	public:
		MapperStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node,
							ExecutionService::JobDescriptor& job_descriptor,
							ExecutionService& execution_service);

		herd::common::task_t build_task(const TaskKey& key) const override;
	};

	class ReduceStageProgress: public BaseStageProgress
	{
	public:
		ReduceStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node,
							ExecutionService::JobDescriptor& job_descriptor,
							ExecutionService& execution_service);

		void mark_task_completed(uint32_t task_key) override;

		herd::common::task_t build_task(const TaskKey& key) const override;

	private:
		struct ReduceNode
		{
			std::tuple<herd::common::UUID, uint64_t, uint32_t> stage_output_;
			std::size_t unresolved_dependencies;
		};

		herd::common::DAG<ReduceNode> reduce_tree_;
	};

	struct JobDescriptor
	{
		JobDescriptor(
				const herd::common::UUID& session,
				const herd::common::UUID& job,
				herd::common::JobStatus job_status,
				herd::common::ExecutionPlan job_plan,
				std::size_t job_concurrency_limit)
		: 	session_uuid(session),
			job_uuid(job),
			status(job_status),
			plan(std::move(job_plan)),
			concurrency_limit(job_concurrency_limit)
		{}

		herd::common::UUID session_uuid;
		herd::common::UUID job_uuid;
		std::vector<std::size_t> pending_stage_ids{};
		std::unordered_map<std::size_t, std::size_t> dependency_lookup;
		std::map<std::size_t, std::unique_ptr<BaseStageProgress>> stage_progress{};
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

	void set_executor(std::shared_ptr<executor::IExecutor> executor) noexcept;

	[[nodiscard]] std::optional<TaskKey> get_next_for_execution();
	void mark_task_running(TaskKey task_key);
	[[nodiscard]] herd::common::task_t task_for_task_key(const TaskKey& key) const;

	void mark_task_completed(const TaskKey& task_key);
	void mark_task_failed(const TaskKey& key);

	StorageService& storage_service();

private:
	KeyService& key_service_;
	StorageService& storage_service_;

	mutable std::shared_mutex jobs_mutex_;
	std::condition_variable jobs_cv_;

	std::queue<std::shared_ptr<JobDescriptor>> pending_jobs_;
	std::multimap<herd::common::UUID, std::shared_ptr<JobDescriptor>> job_descriptors_;

	std::shared_ptr<executor::IExecutor> executor_;

	void lock_required_resources(const herd::common::UUID& session_uuid, const execution_plan::ResourceRequirements& requirements);
	const JobDescriptor& get_job_descriptor(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid) const;
	JobDescriptor& get_job_descriptor(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid);

	void initialize_job(ExecutionService::JobDescriptor& descriptor);

	void recalculate_available_stages(ExecutionService::JobDescriptor& descriptor);

	herd::common::task_t prepare_task(const JobDescriptor& descriptor, const TaskKey& key) const;
};

#endif //HERDSMAN_EXECUTION_SERVICE_HPP
