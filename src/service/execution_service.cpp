#include "service/execution_service.hpp"
#include "service/common_exceptions.hpp"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <cassert>


namespace
{
	std::vector<herd::common::ColumnMeta> build_columns_for_output(const std::vector<herd::common::Circuit::OutputColumn>& outputs)
	{
		std::vector<herd::common::ColumnMeta> columns;
		columns.reserve(outputs.size());
		for(const auto & output_column : outputs)
		{
			columns.emplace_back(output_column.name, output_column.data_type);
		}

		return columns;
	}
}

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
			session_uuid,
			job_uuid,
			herd::common::JobStatus::WAITING_FOR_EXECUTION,
			plan,
			concurrency_limit
		);

		initialize_job(*job_descriptor);

		job_descriptors_.emplace(
				session_uuid,
				job_descriptor
		);
		pending_jobs_.emplace(job_descriptor);
		recalculate_available_stages(*job_descriptor);
	}
	executor_->send_event(executor::event::JobScheduled());

	return JobDescription{
			job_uuid,
			plan,
			0 //todo: calculate estimated complexity
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
				return entry.second->job_uuid == job_uuid;
			}
    );

	if (job_iter == jobs_iter_end)
	{
		throw ObjectNotFoundException("No job with provided uuid found");
	}

	const auto& job = job_iter->second;

	return JobExecutionState{
			job->job_uuid,
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
						job->job_uuid,
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

	const auto& job = pending_jobs_.front();

	if(job->concurrency_limit != 0 && job->concurrency_limit <= job->running_tasks)
	{
		return std::nullopt;
	}

	for(const auto stage_progress_id: job->pending_stage_ids)
	{
		const auto& stage_progress = job->stage_progress.at(stage_progress_id);
		const auto task = stage_progress->get_next_for_execution();
		if(task.has_value())
		{
			return task;
		}
	}

	return std::nullopt;
}

void ExecutionService::mark_task_running(TaskKey task_key)
{
	auto& job = get_job_descriptor(task_key.session_uuid, task_key.job_uuid);
	const auto& stage_progress = job.stage_progress.at(task_key.stage_node_id);

	stage_progress->mark_task_running(task_key.part);

	++job.running_tasks;
}

const ExecutionService::JobDescriptor& ExecutionService::get_job_descriptor(const herd::common::UUID& session_uuid, const herd::common::UUID& job_uuid) const
{
	const auto [jobs_iter_beg, jobs_iter_end] = job_descriptors_.equal_range(session_uuid);
	const auto job_iter = std::find_if(
			jobs_iter_beg, jobs_iter_end,
			[job_uuid](const auto& entry)
			{
				return entry.second->job_uuid == job_uuid;
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
				return entry.second->job_uuid == job_uuid;
			}
	);
	assert(job_iter != jobs_iter_end);

	return *job_iter->second;
}


herd::common::task_t ExecutionService::prepare_task(const JobDescriptor& descriptor, const TaskKey& key) const
{
	const auto& progress = descriptor.stage_progress.at(key.stage_node_id);

	return progress->build_task(key);
}

herd::common::task_t ExecutionService::task_for_task_key(const TaskKey& key) const
{
	std::shared_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	return prepare_task(job, key);
}

void ExecutionService::mark_task_completed(const TaskKey& task_key)
{
	std::unique_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(task_key.session_uuid, task_key.job_uuid);
	--job.running_tasks;

	const auto& stage_progress = job.stage_progress.at(task_key.stage_node_id);
	stage_progress->mark_task_completed(task_key.part);

	recalculate_available_stages(job);

	if(pending_jobs_.front()->status == herd::common::JobStatus::COMPLETED)
	{
		spdlog::info("Completed job: {}", pending_jobs_.front()->job_uuid.as_string());
		pending_jobs_.pop();
	}
}

void ExecutionService::initialize_job(ExecutionService::JobDescriptor& descriptor)
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

		const auto node_id = node.node_id();

		visited.emplace(node_id);
		std::ranges::for_each(node.children(), [&visit_queue](const auto& dependant_node){ visit_queue.emplace(dependant_node); });

		if(std::holds_alternative<herd::common::InputStage>(node.value()))
		{
			auto progress = std::make_unique<InputStageProgress>(node, descriptor, *this);
			descriptor.stage_progress.try_emplace(node_id, std::move(progress));
		}
		else if(std::holds_alternative<herd::common::OutputStage>(node.value()))
		{
			auto progress = std::make_unique<OutputStageProgress>(node, descriptor, *this);
			descriptor.stage_progress.try_emplace(node_id, std::move(progress));
		}
		else
		{
			if(std::holds_alternative<herd::common::MapperStage>(node.value()))
			{
				auto progress = std::make_unique<MapperStageProgress>(node, descriptor, *this);
				descriptor.stage_progress.try_emplace(node_id, std::move(progress));
			}
			else if(std::holds_alternative<herd::common::ReduceStage>(node.value()))
			{
				auto progress = std::make_unique<ReduceStageProgress>(node, descriptor, *this);
				descriptor.stage_progress.try_emplace(node_id, std::move(progress));
			}

			const auto dependencies_count = std::ranges::count_if(node.parents(), [](const decltype(node)& parent){
				return !std::holds_alternative<herd::common::InputStage>(parent.value());
			});

			descriptor.dependency_lookup.try_emplace(node_id, dependencies_count);
		}
	}
}

void ExecutionService::recalculate_available_stages(ExecutionService::JobDescriptor& descriptor)
{
	//complete ended stages
	std::unordered_set<std::size_t> completed_stages;
	for(auto stage_id: descriptor.pending_stage_ids)
	{
		const auto& stage = descriptor.stage_progress.at(stage_id);

		if(stage->is_completed())
		{
			completed_stages.insert(stage_id);

			for(const auto& child: descriptor.plan.execution_graph[stage_id].children())
			{
				if(!std::holds_alternative<herd::common::OutputStage>(child.value()))
				{
					--descriptor.dependency_lookup.at(child.node_id());
				}
			}
		}
	}

	std::erase_if(
			descriptor.pending_stage_ids,
			[completed_stages](std::size_t stage_id)
			{
				return completed_stages.contains(stage_id);
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

	job.status = herd::common::JobStatus::FAILED;

	if(pending_jobs_.front()->job_uuid == key.job_uuid)
	{
		pending_jobs_.pop();
	}
}

StorageService& ExecutionService::storage_service()
{
	return storage_service_;
}

std::tuple<herd::common::UUID, uint64_t, uint32_t> ExecutionService::BaseStageProgress::stage_output() const
{
	return stage_output_;
}

herd::common::task_t ExecutionService::BaseStageProgress::build_task([[maybe_unused]] const TaskKey& key) const
{
	return std::monostate{};
}

bool ExecutionService::BaseStageProgress::is_completed() const
{
	return pending_tasks_.empty();
}

void ExecutionService::BaseStageProgress::mark_task_completed(uint32_t task_key)
{
	std::erase_if(pending_tasks_,
		[task_key](const StageTask& task)
		{
			return task.key == task_key;
		}
	);
}

void ExecutionService::BaseStageProgress::mark_task_running(uint32_t task_key)
{
	for(auto& pending_task: pending_tasks_)
	{
		if(pending_task.key == task_key)
		{
			pending_task.state = StageTask::State::PENDING;
		}
	}
}

std::optional<TaskKey> ExecutionService::BaseStageProgress::get_next_for_execution()
{
	for(auto& task: pending_tasks_)
	{
		if(task.state == StageTask::State::WAITING)
		{
			TaskKey task_key = {
					job_.session_uuid, job_.job_uuid, stage_node_.node_id(),
					task.key};
			task.state = StageTask::State::PENDING;

			return task_key;
		}
	}
	return std::nullopt;
}

herd::common::task_t ExecutionService::MapperStageProgress::build_task(const TaskKey& key) const
{
	const auto& stage_node = job_.plan.execution_graph[key.stage_node_id];
	const auto& stage = std::get<herd::common::MapperStage>(stage_node.value());

	const auto parent_stage_nodes = stage_node.parents();
	assert(parent_stage_nodes.size() == 1);
	const auto& [parent_uuid, parent_row_count, parent_partitions] = job_.stage_progress.at(parent_stage_nodes[0].node_id())->stage_output();
	const auto parent_partition_row_count = execution_service_.storage_service().get_partition_size(key.session_uuid, parent_uuid, key.part);

	const auto [output_uuid, output_row_count, output_partitions] = job_.stage_progress.at(stage_node.node_id())->stage_output();

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
			job_.plan.schema_type
	};

	return herd::common::MapTask {
			key.session_uuid,
			input_data_frame_ptr,
			output_data_frame_ptr,
			crypto_key_ptr,
			stage.circuit
	};
}

herd::common::task_t ExecutionService::ReduceStageProgress::build_task(const TaskKey& key) const
{
	std::vector<herd::common::InputDataFramePtr> input_ptrs;
	herd::common::DataFramePtr output_ptr{};

	const auto node = reduce_tree_[key.part];
	for(const auto parent: node.parents())
	{
		auto& [uuid, row_count, part] = parent.value().stage_output_;
		herd::common::InputDataFramePtr data_frame_ptr {
				{
						uuid,
						part
				},
				row_count
		};
		input_ptrs.emplace_back(data_frame_ptr);
	}

	const auto [uuid, row_count, part] = node.value().stage_output_;
	output_ptr = herd::common::DataFramePtr{
			uuid,
			part
	};

	herd::common::CryptoKeyPtr crypto_key_ptr = {
			job_.plan.schema_type
	};

	assert(std::holds_alternative<herd::common::ReduceStage>(stage_node_.value()));
	const auto& stage = std::get<herd::common::ReduceStage>(stage_node_.value());

	return herd::common::ReduceTask {
			key.session_uuid,
			input_ptrs,
			output_ptr,
			crypto_key_ptr,
			stage.circuit
	};
}

ExecutionService::InputStageProgress::InputStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node, ExecutionService::JobDescriptor& job_descriptor, ExecutionService& execution_service):
	BaseStageProgress(stage_node, job_descriptor, execution_service)
{
	const auto& input_stage = std::get<herd::common::InputStage>(stage_node_.value());
	const auto data_frame = execution_service_.storage_service().get_data_frame(job_.session_uuid, input_stage.data_frame_uuid);

	stage_output_ = std::make_tuple(input_stage.data_frame_uuid, data_frame->row_count, data_frame->partitions);
}

ExecutionService::MapperStageProgress::MapperStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node, ExecutionService::JobDescriptor& job_descriptor, ExecutionService& execution_service):
	BaseStageProgress(stage_node, job_descriptor, execution_service)
{
	const auto& mapper_stage = std::get<herd::common::MapperStage>(stage_node_.value());
	const auto parent_stages = stage_node_.parents();

	assert(parent_stages.size() == 1);

	const auto& [input_data_frame, row_count, partitions] = job_.stage_progress.at(parent_stages[0].node_id())->stage_output();
	const auto name = "intermediate-" + job_.job_uuid.as_string() + "-" + std::to_string(stage_node_.node_id());

	const auto columns = build_columns_for_output(mapper_stage.circuit.output);

	const auto intermediate_data_frame = execution_service_.storage_service().create_data_frame(
			job_.session_uuid, name,
			job_.plan.schema_type, columns,
			row_count,
			partitions);

	stage_output_ = std::make_tuple(intermediate_data_frame, row_count, partitions);

	for(std::uint32_t i = 0; i < partitions; ++i)
	{
		pending_tasks_.emplace_back(i);
	}
}

ExecutionService::OutputStageProgress::OutputStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node, ExecutionService::JobDescriptor& job_descriptor, ExecutionService& execution_service):
	BaseStageProgress(stage_node, job_descriptor, execution_service)
{
	const auto parent_stages = stage_node_.parents();
	assert(parent_stages.size() == 1);

	stage_output_ = job_.stage_progress.at(parent_stages[0].node_id())->stage_output();
}

ExecutionService::ReduceStageProgress::ReduceStageProgress(herd::common::DAG<herd::common::stage_t>::Node<true> stage_node, ExecutionService::JobDescriptor& job_descriptor, ExecutionService& execution_service):
	BaseStageProgress(stage_node, job_descriptor, execution_service)
{
	const auto& reduce_stage = std::get<herd::common::ReduceStage>(stage_node_.value());
	const auto parent_stages = stage_node_.parents();
	assert(parent_stages.size() == 1);

	const auto& [input_data_frame, row_count, partitions] = job_.stage_progress.at(parent_stages[0].node_id())->stage_output();
	const auto name = "reduce-" + job_.job_uuid.as_string() + "-" + std::to_string(stage_node_.node_id());

	const auto columns = build_columns_for_output(reduce_stage.circuit.output);
	auto& storage_service = execution_service_.storage_service();

	const auto output_data_frame = storage_service.create_data_frame(
			job_.session_uuid, name,
			job_.plan.schema_type, columns,
			1,
			1
	);

	stage_output_ = std::make_tuple(output_data_frame, 1, 1);

	std::vector<herd::common::DAG<ReduceNode>::NodeIterator<false>> input_layer_nodes;
	for(uint32_t i = 0; i < partitions; ++i)
	{
		const auto input_partition_row_count = storage_service.get_partition_size(job_.session_uuid, input_data_frame, i);
		auto node = reduce_tree_.emplace(std::make_tuple(input_data_frame, input_partition_row_count, i), 0);
		input_layer_nodes.emplace_back(node);
	}

	if(reduce_stage.policy == herd::common::Policy::SEQUENCED)
	{
		auto output_node = reduce_tree_.emplace(std::make_tuple(output_data_frame, 1, 1), partitions);
		for(const auto& node: input_layer_nodes)
		{
			reduce_tree_.add_edge(node, output_node);
		}

		pending_tasks_.emplace_back((*output_node).node_id());
	}
	else if(reduce_stage.policy == herd::common::Policy::PARALLEL)
	{
		auto output_node = reduce_tree_.emplace(std::make_tuple(output_data_frame, 1, 1), partitions);
		const auto intermediate_hidden_frame = storage_service.create_data_frame(
				job_.session_uuid, "hidden-" + name,
				job_.plan.schema_type,
				columns,
				partitions,
				partitions
		);

		std::vector<herd::common::DAG<ReduceNode>::NodeIterator<false>> current_layer_nodes;

		for(std::size_t i = 0; i < partitions; ++i)
		{
			auto node = reduce_tree_.emplace(std::make_tuple(intermediate_hidden_frame, partitions, i), 0);
			current_layer_nodes.emplace_back(node);
			reduce_tree_.add_edge(input_layer_nodes[i], node);
			reduce_tree_.add_edge(node, output_node);

			pending_tasks_.emplace_back((*node).node_id());
		}
	}
	else if(reduce_stage.policy == herd::common::Policy::PARALLEL_FULL)
	{
		auto per_node_count = reduce_stage.per_node_count.value_or(2);

		unsigned int current_level_count = partitions;
		auto node_sum = current_level_count;

		while(current_level_count > per_node_count)
		{
			auto remaining_nodes = current_level_count % per_node_count;
			current_level_count = static_cast<unsigned int>(std::floor(static_cast<float>(current_level_count)/static_cast<float>(per_node_count)));
			current_level_count += remaining_nodes;

			node_sum += current_level_count;
		}

		const auto intermediate_hidden_frame = storage_service.create_data_frame(
				job_.session_uuid, "hidden-" + name,
				job_.plan.schema_type,
				columns,
				node_sum,
				node_sum
		);

		std::vector<herd::common::DAG<ReduceNode>::NodeIterator<false>> current_layer_nodes;

		uint32_t partition_index = 0;
		for(std::size_t i = 0; i < partitions; ++i)
		{
			auto node = reduce_tree_.emplace(std::make_tuple(intermediate_hidden_frame, 1, partition_index), 0);
			current_layer_nodes.emplace_back(node);
			reduce_tree_.add_edge(input_layer_nodes[i], node);
			pending_tasks_.emplace_back((*node).node_id());

			++partition_index;
		}

		while(current_layer_nodes.size() > per_node_count)
		{
			input_layer_nodes = current_layer_nodes;
			current_layer_nodes.clear();
			const std::size_t max_full_child_index = input_layer_nodes.size() - input_layer_nodes.size() % per_node_count;
			for(std::size_t i = 0; i < max_full_child_index; i += per_node_count)
			{
				auto node = reduce_tree_.emplace(std::make_tuple(intermediate_hidden_frame, 1, partition_index), per_node_count);
				current_layer_nodes.emplace_back(node);
				for(std::size_t j = 0; j < per_node_count; ++j)
				{
					reduce_tree_.add_edge(input_layer_nodes[i+j], node);
				}
				++partition_index;
			}

			for(std::size_t i = max_full_child_index; i < input_layer_nodes.size(); ++i)
			{
				current_layer_nodes.emplace_back(input_layer_nodes[i]);
			}
		}

		auto output_node = reduce_tree_.emplace(std::make_tuple(output_data_frame, 1, 1), current_layer_nodes.size());

		for(const auto& node: current_layer_nodes)
		{
			reduce_tree_.add_edge(node, output_node);
		}
	}
}

void ExecutionService::ReduceStageProgress::mark_task_completed(uint32_t task_key)
{
	BaseStageProgress::mark_task_completed(task_key);

	auto node = reduce_tree_[task_key];
	for(auto& child: node.children())
	{
		auto& reduce_task = child.value();
		--reduce_task.unresolved_dependencies;
		if(reduce_task.unresolved_dependencies == 0)
		{
			pending_tasks_.emplace_back(child.node_id());
		}
	}
}
