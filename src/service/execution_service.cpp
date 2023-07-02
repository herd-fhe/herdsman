#include "service/execution_service.hpp"
#include "service/common_exceptions.hpp"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <cassert>


namespace
{
	herd::common::DataType map_data_width_to_type(unsigned int width)
	{
		switch(width)
		{
			case 1:
				return herd::common::DataType::BIT;
			case 8:
				return herd::common::DataType::UINT8;
			case 16:
				return herd::common::DataType::UINT16;
			case 32:
				return herd::common::DataType::UINT32;
			case 64:
				return herd::common::DataType::UINT64;
			default:
				assert(false && "Unsupported type");
				return static_cast<herd::common::DataType>(0);
		}
	}
}

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
		auto job_descriptor = std::make_shared<JobDescriptor>(
			job_uuid,
			herd::common::JobStatus::WAITING_FOR_EXECUTION,
			plan
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
	auto& pending_stages = job->pending_stages;

	for(auto& stage_progress: pending_stages)
	{
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
	const auto parent_data_frame = std::get<0>(descriptor.intermediate_stage_outputs.at(parent_stage_nodes[0].node_id()));
	const auto parent_row_count = storage_service_.get_partition_size(key.session_uuid, parent_data_frame, key.partition);

	const auto stage_output_data_frame = std::get<0>(descriptor.intermediate_stage_outputs.at(stage_node.node_id()));

	herd::common::InputDataFramePtr input_data_frame_ptr = {
			{
					parent_data_frame,
					key.partition,
			},
			parent_row_count
	};

	herd::common::DataFramePtr output_data_frame_ptr = {
			stage_output_data_frame,
			key.partition
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

herd::common::task_t ExecutionService::task_for_task_key(TaskKey key) const
{
	std::shared_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	return prepare_task(job, key);
}

void ExecutionService::mark_task_completed(TaskKey key)
{
	std::unique_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	auto stage_iter = std::ranges::find_if(
			job.pending_stages,
			[node_id = key.stage_node_id](const StageProgress& progress)
			{
				return progress.stage_node.node_id() == node_id;
			}
	);
	assert(stage_iter != std::end(job.pending_stages));
	auto task_iter = std::ranges::find_if(
			stage_iter->pending_tasks,
			[partition = key.partition](const StageTask& task)
			{
				return task.partition == partition;
			}
	);
	assert(task_iter != std::end(stage_iter->pending_tasks));

	task_iter->state = StageTask::State::COMPLETED;

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
				[&visited](const auto& node)
				{
					return visited.contains(node.node_id());
				}
		);
		if(!all_dependencies_visited)
		{
			continue;
		}

		visited.emplace(node.node_id());
		std::ranges::for_each(node.children(), [&visit_queue](const auto& node){ visit_queue.emplace(node); });

		if(std::holds_alternative<herd::common::InputStage>(node.value()))
		{
			const auto& input_stage = std::get<herd::common::InputStage>(node.value());
			const auto data_frame = storage_service_.get_data_frame(session_uuid, input_stage.data_frame_uuid);


			descriptor.intermediate_stage_outputs.try_emplace(node.node_id(), input_stage.data_frame_uuid, data_frame->row_count, data_frame->partitions);
		}
		else
		{
			if(std::holds_alternative<herd::common::MapperStage>(node.value()))
			{
				const auto& mapper_stage = std::get<herd::common::MapperStage>(node.value());
				const auto parent_stages = node.parents();
				assert(parent_stages.size() == 1);

				const auto input_data_frame_entry = descriptor.intermediate_stage_outputs.at(parent_stages[0].node_id());
				const auto& [input_data_frame, row_count, partitions] = input_data_frame_entry;
				const auto name = "intermediate-" + descriptor.uuid.as_string() + "-" + std::to_string(node.node_id());

				std::vector<herd::common::ColumnMeta> columns;
				columns.reserve(mapper_stage.circuit.output.size());
				for(std::size_t i = 0; i < mapper_stage.circuit.output.size(); ++i)
				{
					columns.emplace_back("temp-" + std::to_string(i), map_data_width_to_type(mapper_stage.circuit.output[i]));
				}

				const auto intermediate_data_frame = storage_service_.create_data_frame(
						session_uuid, name,
						descriptor.plan.schema_type, columns,
						row_count,
						partitions);

				descriptor.intermediate_stage_outputs.try_emplace(node.node_id(), intermediate_data_frame, row_count, partitions);

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
			else if(std::holds_alternative<herd::common::OutputStage>(node.value()))
			{
				//			const auto& output_stage = std::get<herd::common::OutputStage>(node.value());
				//todo: rename data frame
				const auto parent_stages = node.parents();
				assert(parent_stages.size() == 1);

				const auto input_data_frame_entry = descriptor.intermediate_stage_outputs.at(parent_stages[0].node_id());
				const auto& [input_data_frame, row_count, partitions] = input_data_frame_entry;
				descriptor.intermediate_stage_outputs.try_emplace(node.node_id(), input_data_frame, row_count, partitions);
			}
		}
	}
}

void ExecutionService::recalculate_waiting_tasks(ExecutionService::JobDescriptor& descriptor)
{
	//complete ended stages
	std::vector<std::size_t> completed_stages;
	for(auto stage: descriptor.pending_stages)
	{
		const auto stage_completed = std::ranges::all_of(
				stage.pending_tasks,
				[](const StageTask& task)
				{
					return task.state == StageTask::State::COMPLETED;
				}
		);

		if(stage_completed)
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

	std::erase_if(
			descriptor.pending_stages,
			[completed_stages](const StageProgress& progress)
			{
				return std::ranges::find(completed_stages, progress.stage_node.node_id()) != std::end(completed_stages);
			}
 	);

	// start stages with resolved dependencies
	std::vector<std::size_t> starting_stages;
	for(const auto& entry: descriptor.dependency_lookup)
	{
		if(entry.second == 0)
		{
			starting_stages.emplace_back(entry.first);
		}
	}

	std::ranges::for_each(starting_stages, [&descriptor](const std::size_t index){ descriptor.dependency_lookup.erase(index); });
	for(const auto stage_node_id: starting_stages)
	{
		StageProgress progress{};
		const auto stage_node = descriptor.plan.execution_graph[stage_node_id];
		progress.stage_node = static_cast<decltype(descriptor.plan.execution_graph)::Node<true>>(stage_node);

		if(std::holds_alternative<herd::common::MapperStage>(stage_node.value()))
		{
			const auto parent_stages = stage_node.parents();
			assert(parent_stages.size() == 1);

			const auto partitions = std::get<2>(descriptor.intermediate_stage_outputs[parent_stages[0].node_id()]);
			for(std::uint32_t i = 0; i < partitions; ++i)
			{
				progress.pending_tasks.emplace_back(i);
			}
		}

		descriptor.pending_stages.emplace_back(progress);
	}

	if(descriptor.pending_stages.empty())
	{
		descriptor.status = herd::common::JobStatus::COMPLETED;
	}
}

void ExecutionService::mark_task_failed(TaskKey key)
{
	std::unique_lock lock(jobs_mutex_);

	auto& job = get_job_descriptor(key.session_uuid, key.job_uuid);
	auto stage_iter = std::ranges::find_if(
			job.pending_stages,
			[node_id = key.stage_node_id](const StageProgress& progress)
			{
				return progress.stage_node.node_id() == node_id;
			}
	);
	assert(stage_iter != std::end(job.pending_stages));
	auto task_iter = std::ranges::find_if(
			stage_iter->pending_tasks,
			[partition = key.partition](const StageTask& task)
			{
				return task.partition == partition;
			}
	);
	assert(task_iter != std::end(stage_iter->pending_tasks));

	task_iter->state = StageTask::State::FAILED;
	job.status = herd::common::JobStatus::FAILED;
}
