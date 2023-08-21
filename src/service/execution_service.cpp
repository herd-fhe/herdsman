#include "service/execution_service.hpp"
#include "service/common_exceptions.hpp"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <cassert>


ExecutionService::ExecutionService(KeyService& key_service, StorageService& storage_service)
	: key_service_(key_service), storage_service_(storage_service)
{
}

ExecutionService::JobDescription ExecutionService::schedule_job(const herd::common::UUID& session_uuid, const herd::common::ExecutionPlan& plan, std::size_t concurrency_limit)
{
	const herd::common::UUID job_uuid = {};

	{
		std::unique_lock lock(jobs_mutex_);

		const auto resource_requirements = execution_plan::analyze_required_resources(plan);
		lock_required_resources(session_uuid, resource_requirements);
		auto job_descriptor = std::make_shared<JobDescriptor>(
			job_uuid,
			herd::common::JobStatus::WAITING_FOR_EXECUTION,
			plan,
			concurrency_limit
		);

		initialize_job(session_uuid, *job_descriptor);

		job_descriptors_.emplace(
				session_uuid,
				job_descriptor
		);
		pending_jobs_.emplace(session_uuid, job_descriptor);
		recalculate_waiting_tasks(*job_descriptor);
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

std::optional<TaskKey> ExecutionService::get_next_for_execution()
{
	std::unique_lock lock(jobs_mutex_);

	if(pending_jobs_.empty())
	{
		return std::nullopt;
	}

	const auto& [session_uuid, job] = pending_jobs_.front();

	if(job->concurrency_limit != 0 && job->concurrency_limit <= job->running_tasks)
	{
		return std::nullopt;
	}

	for(const auto stage_progress_id: job->pending_stage_ids)
	{
		auto& stage_progress = job->stage_progress.at(stage_progress_id);
		for(auto& task: stage_progress.pending_tasks)
		{
			if(task.state == StageTask::State::WAITING)
			{
				TaskKey task_key = {
						session_uuid, job->uuid, stage_progress.stage_node.node_id(),
						task.partition
				};
				task.state = StageTask::State::PENDING;

				return task_key;
			}
		}
	}

	return std::nullopt;
}

void ExecutionService::mark_task_running(TaskKey key)
{
	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	auto& task = get_task(job, key.stage_node_id, key.part);

	++job.running_tasks;
	task.state = StageTask::State::PENDING;
}

const ExecutionService::JobDescriptor& ExecutionService::get_job_descriptor(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid) const
{
	const auto [jobs_iter_beg, jobs_iter_end] = job_descriptors_.equal_range(session_uuid);
	const auto job_iter = std::find_if(
			jobs_iter_beg, jobs_iter_end,
			[job_uuid](const auto& entry)
			{
				return entry.second->uuid == job_uuid;
			}
	);
	assert(job_iter != jobs_iter_end);

	return *job_iter->second;
}

ExecutionService::JobDescriptor& ExecutionService::get_job_descriptor(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid)
{
	const auto [jobs_iter_beg, jobs_iter_end] = job_descriptors_.equal_range(session_uuid);
	const auto job_iter = std::find_if(
			jobs_iter_beg, jobs_iter_end,
			[job_uuid](const auto& entry)
			{
				return entry.second->uuid == job_uuid;
			}
	);
	assert(job_iter != jobs_iter_end);

	return *job_iter->second;
}


herd::common::task_t ExecutionService::prepare_task(const JobDescriptor& descriptor, const TaskKey& key) const
{
	const auto& stage_node = descriptor.plan.execution_graph[key.stage_node_id];
	const auto& stage = stage_node.value();

	if(std::holds_alternative<herd::common::MapperStage>(stage))
	{
		return build_map_task(descriptor, key);
	}
	else if(std::holds_alternative<herd::common::ReduceStage>(stage))
	{
		return build_reduce_task(descriptor, key);
	}
	else
	{
		throw std::runtime_error("Not implemented yet");
	}
}

herd::common::task_t ExecutionService::build_map_task(const JobDescriptor& descriptor, const TaskKey& key) const
{
	const auto& stage_node = descriptor.plan.execution_graph[key.stage_node_id];
	const auto& stage = std::get<herd::common::MapperStage>(stage_node.value());

	const auto parent_stage_nodes = stage_node.parents();
	assert(parent_stage_nodes.size() == 1);
	const auto& [parent_uuid, parent_row_count, parent_partitions] = descriptor.stage_progress.at(parent_stage_nodes[0].node_id()).stage_output;
	const auto parent_partition_row_count = storage_service_.get_partition_size(key.session_uuid, parent_uuid, key.part);

	const auto [output_uuid, output_row_count, output_partitions] = descriptor.stage_progress.at(stage_node.node_id()).stage_output;

	herd::common::InputDataFramePtr input_data_frame_ptr = {
			{
					parent_uuid,
					key.part,
			},
			parent_partition_row_count
	};

	herd::common::DataFramePtr output_data_frame_ptr = {
			output_uuid,
			key.part
	};

	herd::common::CryptoKeyPtr crypto_key_ptr = {
			descriptor.plan.schema_type
	};

	return herd::common::MapTask {
		key.session_uuid,
		input_data_frame_ptr,
		output_data_frame_ptr,
		crypto_key_ptr,
		stage.circuit
	};
}

herd::common::task_t ExecutionService::build_reduce_task(const ExecutionService::JobDescriptor& descriptor, const TaskKey& key) const
{
	const auto& stage_node = descriptor.plan.execution_graph[key.stage_node_id];
	const auto& stage = std::get<herd::common::ReduceStage>(stage_node.value());

	const auto& stage_progress = descriptor.stage_progress.at(key.stage_node_id);

	const auto parent_stage_nodes = stage_node.parents();
	assert(parent_stage_nodes.size() == 1);
	const auto& [parent_uuid, parent_row_count, parent_partitions] = descriptor.stage_progress.at(parent_stage_nodes[0].node_id()).stage_output;

	std::vector<herd::common::InputDataFramePtr> input_ptrs;
	herd::common::DataFramePtr output_ptr{};

	if(stage.policy == herd::common::Policy::PARALLEL)
	{
		const auto& state = stage_progress.state;
		assert(std::holds_alternative<ReduceStageState>(state));

		const auto& reduce_state = std::get<ReduceStageState>(state);
		const auto& [hidden_uuid, hidden_size, hidden_partitions] = reduce_state.hidden_stage_output.value();

		if(key.part == parent_partitions)
		{
			for(unsigned int i = 0; i < hidden_partitions; ++i)
			{
				herd::common::InputDataFramePtr data_frame_ptr {
					{
							hidden_uuid,
							i
					},
					1
				};
				input_ptrs.emplace_back(data_frame_ptr);
			}

			output_ptr = herd::common::DataFramePtr{
					std::get<0>(stage_progress.stage_output),
					0
			};
		}
		else
		{
			const auto parent_partition_row_count = storage_service_.get_partition_size(key.session_uuid, parent_uuid, key.part);

			herd::common::InputDataFramePtr data_frame_ptr {
					{
							parent_uuid,
							key.part
					},
					parent_partition_row_count
			};
			input_ptrs.emplace_back(data_frame_ptr);

			output_ptr = herd::common::DataFramePtr{
					hidden_uuid,
					key.part
			};
		}
	}
	else if(stage.policy == herd::common::Policy::SEQUENCED)
	{
		for(unsigned int i = 0; i < parent_partitions; ++i)
		{
			const auto parent_partition_row_count = storage_service_.get_partition_size(key.session_uuid, parent_uuid, key.part);
			herd::common::InputDataFramePtr data_frame_ptr {
					{
							parent_uuid,
							i
					},
					parent_partition_row_count
			};
			input_ptrs.emplace_back(data_frame_ptr);
		}

		output_ptr = herd::common::DataFramePtr{
				std::get<0>(stage_progress.stage_output),
				0
		};
	}

	herd::common::CryptoKeyPtr crypto_key_ptr = {
			descriptor.plan.schema_type
	};

	return herd::common::ReduceTask {
			key.session_uuid,
			input_ptrs,
			output_ptr,
			crypto_key_ptr,
			stage.circuit
	};
}

herd::common::task_t ExecutionService::task_for_task_key(const TaskKey& key) const
{
	std::shared_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	return prepare_task(job, key);
}

void ExecutionService::mark_task_completed(const TaskKey& key)
{
	std::unique_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	--job.running_tasks;

	auto& task = get_task(job, key.stage_node_id, key.part);

	task.state = StageTask::State::COMPLETED;

	recalculate_waiting_tasks(job);

	if(pending_jobs_.front().second->status == herd::common::JobStatus::COMPLETED)
	{
		spdlog::info("Completed job: {}", pending_jobs_.front().second->uuid.as_string());
		pending_jobs_.pop();
	}
}

void ExecutionService::initialize_job(const herd::common::UUID& session_uuid, ExecutionService::JobDescriptor& descriptor)
{
	const auto source_nodes = descriptor.plan.execution_graph.source_nodes();
	std::queue<decltype(descriptor.plan.execution_graph)::Node<true>> visit_queue;

	for(const auto& node: source_nodes)
	{
		assert(std::holds_alternative<herd::common::InputStage>(node.value()));
		visit_queue.emplace(node);
	}

	std::unordered_set<std::size_t> visited;
	while(!visit_queue.empty())
	{
		auto node = visit_queue.front();
		visit_queue.pop();
		if(visited.contains(node.node_id()))
		{
			continue;
		}

		const bool all_dependencies_visited = std::ranges::all_of(
				node.parents(),
				[&visited](const auto& dependency_node)
				{
					return visited.contains(dependency_node.node_id());
				}
		);
		if(!all_dependencies_visited)
		{
			continue;
		}

		visited.emplace(node.node_id());
		std::ranges::for_each(node.children(), [&visit_queue](const auto& dependant_node){ visit_queue.emplace(dependant_node); });

		if(std::holds_alternative<herd::common::InputStage>(node.value()))
		{
			initialize_input_stage(node, descriptor, session_uuid);
		}
		else if(std::holds_alternative<herd::common::OutputStage>(node.value()))
		{
			initialize_output_stage(node, descriptor);
		}
		else
		{
			if(std::holds_alternative<herd::common::MapperStage>(node.value()))
			{
				initialize_map_stage(node, descriptor, session_uuid);
			}
			else if(std::holds_alternative<herd::common::ReduceStage>(node.value()))
			{
				initialize_reduce_stage(node, descriptor, session_uuid);
			}

			if(!descriptor.dependency_lookup.contains(node.node_id()))
			{
				descriptor.dependency_lookup.try_emplace(node.node_id(), 0);
			}

			for(const auto parent: node.parents())
			{
				if(!std::holds_alternative<herd::common::InputStage>(parent.value()))
				{
					++descriptor.dependency_lookup.at(node.node_id());
				}
			}
		}
	}
}

void ExecutionService::initialize_input_stage(
		const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node,
		ExecutionService::JobDescriptor& descriptor, const herd::common::UUID& session_uuid
)
{
	const auto& input_stage = std::get<herd::common::InputStage>(stage_node.value());
	const auto data_frame = storage_service_.get_data_frame(session_uuid, input_stage.data_frame_uuid);

	StageProgress progress(
			stage_node,
			std::make_tuple(input_stage.data_frame_uuid, data_frame->row_count, data_frame->partitions),
			std::monostate()
	);
	descriptor.stage_progress.try_emplace(stage_node.node_id(), progress);
}

void ExecutionService::initialize_output_stage(
		const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node,
		ExecutionService::JobDescriptor& descriptor
)
{
	//todo: rename data frame
	const auto parent_stages = stage_node.parents();
	assert(parent_stages.size() == 1);

	auto input_data_frame_entry = descriptor.stage_progress.at(parent_stages[0].node_id()).stage_output;

	StageProgress progress(
			stage_node,
			input_data_frame_entry,
			std::monostate()
	);

	descriptor.stage_progress.try_emplace(stage_node.node_id(), progress);
}

void ExecutionService::initialize_map_stage(
		const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node,
		ExecutionService::JobDescriptor& descriptor, const herd::common::UUID& session_uuid
)
{
	const auto& mapper_stage = std::get<herd::common::MapperStage>(stage_node.value());
	const auto parent_stages = stage_node.parents();
	assert(parent_stages.size() == 1);

	const auto& [input_data_frame, row_count, partitions] = descriptor.stage_progress.at(parent_stages[0].node_id()).stage_output;
	const auto name = "intermediate-" + descriptor.uuid.as_string() + "-" + std::to_string(stage_node.node_id());

	std::vector<herd::common::ColumnMeta> columns;
	columns.reserve(mapper_stage.circuit.output.size());
	for(const auto & output_column : mapper_stage.circuit.output)
	{
			columns.emplace_back(output_column.name, output_column.data_type);
	}

	const auto intermediate_data_frame = storage_service_.create_data_frame(
			session_uuid, name,
			descriptor.plan.schema_type, columns,
			row_count,
			partitions);

	StageProgress progress(
			stage_node,
			std::make_tuple(intermediate_data_frame, row_count, partitions),
			std::monostate()
	);

	descriptor.stage_progress.try_emplace(stage_node.node_id(), progress);
}

void ExecutionService::initialize_reduce_stage(const herd::common::DAG<herd::common::stage_t>::Node<true>& stage_node, ExecutionService::JobDescriptor& descriptor, const herd::common::UUID& session_uuid)
{
	const auto& reduce_stage = std::get<herd::common::ReduceStage>(stage_node.value());
	const auto parent_stages = stage_node.parents();
	assert(parent_stages.size() == 1);

	const auto& [input_data_frame, row_count, partitions] = descriptor.stage_progress.at(parent_stages[0].node_id()).stage_output;
	const auto name = "intermediate-reduce" + descriptor.uuid.as_string() + "-" + std::to_string(stage_node.node_id());

	std::vector<herd::common::ColumnMeta> columns;
	columns.reserve(reduce_stage.circuit.output.size());
	for(const auto & output_column : reduce_stage.circuit.output)
	{
			columns.emplace_back(output_column.name, output_column.data_type);
	}

	const auto intermediate_data_frame = storage_service_.create_data_frame(
			session_uuid, name,
			descriptor.plan.schema_type, columns,
			1,
			1
	);

	StageProgress progress(
			stage_node,
			std::make_tuple(intermediate_data_frame, 1, 1),
			ReduceStageState{
					std::nullopt
			}
	);

	if(reduce_stage.policy == herd::common::Policy::PARALLEL)
	{
			const auto intermediate_hidden_frame = storage_service_.create_data_frame(
					session_uuid, "hidden-" + name,
					descriptor.plan.schema_type,
					columns,
					partitions,
					partitions
			);

			progress.state = ReduceStageState{
					std::make_tuple(intermediate_hidden_frame, partitions, partitions)
			};
	}

	descriptor.stage_progress.try_emplace(stage_node.node_id(), progress);
}

void ExecutionService::recalculate_waiting_tasks(ExecutionService::JobDescriptor& descriptor)
{
	//complete ended stages
	std::vector<std::size_t> completed_stages;
	for(auto stage_id: descriptor.pending_stage_ids)
	{
		auto& stage = descriptor.stage_progress.at(stage_id);
		const auto stage_completed = std::ranges::all_of(
				stage.pending_tasks,
				[](const StageTask& task)
				{
					return task.state == StageTask::State::COMPLETED || task.state == StageTask::State::BLOCKED;
				}
		);

		if(stage_completed)
		{
			const auto blocked_tasks_present = std::ranges::any_of(
				stage.pending_tasks,
				[](const StageTask& task)
				{
					return task.state == StageTask::State::BLOCKED;
				}
			);

			if(blocked_tasks_present)
			{
				for(auto& task: stage.pending_tasks)
				{
					if(task.state == StageTask::State::BLOCKED)
					{
						task.state = StageTask::State::WAITING;
					}
				}
			}
			else
			{
				completed_stages.emplace_back(stage.stage_node.node_id());

				for(const auto& child: stage.stage_node.children())
				{
					if(descriptor.dependency_lookup.contains(child.node_id()))
					{
						--descriptor.dependency_lookup.at(child.node_id());
					}
				}
			}
		}
	}

	std::erase_if(
			descriptor.pending_stage_ids,
			[completed_stages](std::size_t stage_id)
			{
				return std::ranges::find(completed_stages, stage_id) != std::end(completed_stages);
			}
 	);

	// start stages with resolved dependencies
	std::vector<std::size_t> starting_stages;
	for(const auto& [stage_id, dependencies]: descriptor.dependency_lookup)
	{
		if(dependencies == 0)
		{
			starting_stages.emplace_back(stage_id);
		}
	}

	std::ranges::for_each(starting_stages, [&descriptor](const std::size_t index){ descriptor.dependency_lookup.erase(index); });
	for(const auto stage_node_id: starting_stages)
	{
		const auto stage_node = descriptor.plan.execution_graph[stage_node_id];
		auto& progress = descriptor.stage_progress.at(stage_node_id);

		if(std::holds_alternative<herd::common::MapperStage>(stage_node.value()))
		{
			const auto parent_stages = stage_node.parents();
			assert(parent_stages.size() == 1);

			const auto [parent_uuid, row_count, partitions] = descriptor.stage_progress.at(parent_stages[0].node_id()).stage_output;
			for(std::uint32_t i = 0; i < partitions; ++i)
			{
				progress.pending_tasks.emplace_back(i);
			}
		}
		else if(std::holds_alternative<herd::common::ReduceStage>(stage_node.value()))
		{
			const auto& reduce_stage = std::get<herd::common::ReduceStage>(stage_node.value());
			const auto parent_stages = stage_node.parents();
			assert(parent_stages.size() == 1);

			const auto [parent_uuid, row_count, partitions] = descriptor.stage_progress.at(parent_stages[0].node_id()).stage_output;
			if(reduce_stage.policy == herd::common::Policy::PARALLEL)
			{
				for(std::uint32_t i = 0; i < partitions; ++i)
				{
					progress.pending_tasks.emplace_back(i);
				}
				progress.pending_tasks.emplace_back(partitions, StageTask::State::BLOCKED);
			}
			else if(reduce_stage.policy == herd::common::Policy::SEQUENCED)
			{
				progress.pending_tasks.emplace_back(0);
			}
		}

		descriptor.pending_stage_ids.emplace_back(stage_node_id);
	}

	if(descriptor.pending_stage_ids.empty())
	{
		descriptor.status = herd::common::JobStatus::COMPLETED;
	}
}

void ExecutionService::mark_task_failed(const TaskKey& key)
{
	std::unique_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	auto& task = get_task(job, key.stage_node_id, key.part);

	task.state = StageTask::State::FAILED;
	job.status = herd::common::JobStatus::FAILED;

	if(pending_jobs_.front().first == key.job_uuid)
	{
		pending_jobs_.pop();
	}
}

ExecutionService::StageTask& ExecutionService::get_task(ExecutionService::JobDescriptor& descriptor, std::size_t stage_node_id, uint32_t partition)
{
	auto& stage_progress = descriptor.stage_progress.at(stage_node_id);
	auto task_iter = std::ranges::find_if(
			stage_progress.pending_tasks,
			[partition](const StageTask& task)
			{
				return task.partition == partition;
			}
	);
	assert(task_iter != std::end(stage_progress.pending_tasks));

	return *task_iter;
}
