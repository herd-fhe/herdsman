#include "mapper/model_proto_mapper.hpp"

#include "herd/common/model/schema_type.hpp"


namespace
{
	herd::common::ExecutionPlan::StageType detect_stage_type(const herd::proto::ExecutionStage& stage)
	{
		if(stage.has_input())
		{
			return herd::common::ExecutionPlan::StageType::INPUT;
		}
		else if(stage.has_mapping())
		{
			return herd::common::ExecutionPlan::StageType::MAP;
		}
		else
		{
			throw mapper::MappingError("Proto schema, model mismatch");
		}
	}

	std::optional<herd::common::UUID> get_required_data_frame(const herd::proto::ExecutionStage& stage)
	{
		if(stage.has_input())
		{
			return herd::common::UUID(stage.input().data_frame_uuid());
		}
		else if(stage.has_mapping())
		{
			return std::nullopt;
		}
		else
		{
			throw mapper::MappingError("Proto schema, model mismatch");
		}
	}
}

namespace mapper
{
	herd::proto::SchemaType to_proto(herd::common::SchemaType schema_type)
	{
		switch(schema_type)
		{
			case herd::common::SchemaType::BINFHE:
				return herd::proto::BINFHE;
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}

	herd::proto::DataType to_proto(herd::common::DataType data_type)
	{
		using herd::common::DataType;
		switch(data_type)
		{
			case DataType::BIT:
				return herd::proto::BIT;
			case DataType::UINT8:
				return herd::proto::UINT8;
			case DataType::UINT16:
				return herd::proto::UINT16;
			case DataType::UINT32:
				return herd::proto::UINT32;
			case DataType::UINT64:
				return herd::proto::UINT64;
			case DataType::INT8:
				return herd::proto::INT8;
			case DataType::INT16:
				return herd::proto::INT16;
			case DataType::INT32:
				return herd::proto::INT32;
			case DataType::INT64:
				return herd::proto::INT64;
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}

	herd::proto::JobStatus to_proto(herd::common::JobStatus status)
	{
		using herd::common::JobStatus;
		switch(status)
		{
			case JobStatus::WAITING_FOR_EXECUTION:
				return herd::proto::WAITING_FOR_EXECUTION;
			case JobStatus::PENDING:
				return herd::proto::PENDING;
			case JobStatus::COMPLETED:
				return herd::proto::COMPLETED;
			case JobStatus::FAILED:
				return herd::proto::FAILED;
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}

	google::protobuf::RepeatedPtrField<herd::proto::ColumnDescriptor> to_proto(const herd::common::column_map_type& columns)
	{
		assert(std::numeric_limits<uint8_t>::max() >= columns.size() && "Columns limit");

		google::protobuf::RepeatedPtrField<herd::proto::ColumnDescriptor> proto_columns;

		proto_columns.Reserve(static_cast<int>(columns.size()));

		std::vector<std::tuple<std::string, herd::common::DataType, uint8_t>> columns_temp;
		std::ranges::transform(columns, std::back_inserter(columns_temp),
							   [](const auto& column)
							   {
								   return std::make_tuple(column.first, column.second.type, column.second.index);
							   }
        );
		std::ranges::sort(columns_temp,
						  [](const auto& rhs, const auto& lhs)
						  {
							  return std::get<2>(rhs) < std::get<2>(lhs);
						  }
        );

		for (const auto& [name, type, index]: columns_temp)
		{
			const auto proto_column = proto_columns.Add();
			proto_column->set_name(name);
			proto_column->set_type(to_proto(type));
		}

		return proto_columns;
	}

	herd::common::SchemaType to_model(herd::proto::SchemaType data_type)
	{
		switch(data_type)
		{
			case herd::proto::BINFHE:
				return herd::common::SchemaType::BINFHE;
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}

	herd::common::DataType to_model(herd::proto::DataType data_type)
	{
		using herd::common::DataType;
		switch(data_type)
		{
			case herd::proto::BIT:
				return DataType::BIT;
			case herd::proto::UINT8:
				return DataType::UINT8;
			case herd::proto::UINT16:
				return DataType::UINT16;
			case herd::proto::UINT32:
				return DataType::UINT32;
			case herd::proto::UINT64:
				return DataType::UINT64;
			case herd::proto::INT8:
				return DataType::INT8;
			case herd::proto::INT16:
				return DataType::INT16;
			case herd::proto::INT32:
				return DataType::INT32;
			case herd::proto::INT64:
				return DataType::INT64;
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}

	herd::common::column_map_type to_model(const google::protobuf::RepeatedPtrField<herd::proto::ColumnDescriptor>& columns)
	{
		herd::common::column_map_type model_columns;

		for(uint8_t index = 0; const auto& column: columns)
		{
			model_columns.try_emplace(column.name(), index, to_model(column.type()));
			++index;
		}

		return model_columns;
	}

	herd::common::ExecutionPlan to_model(const herd::proto::ExecutionPlan& plan)
	{
		using namespace herd::common;

		ExecutionPlan execution_plan;
		std::unordered_map<std::size_t, decltype(ExecutionPlan::stages)::iterator> index_to_node;

		for(std::size_t stage_id = 0; const auto& stage: plan.stages())
		{
			const auto iterator = execution_plan.stages.emplace(ExecutionPlan::Stage{
					detect_stage_type(stage),
					to_model(stage.schema_type()),
					get_required_data_frame(stage)
			});

			index_to_node.emplace(stage_id, iterator);
			++stage_id;
		}

		for(std::size_t stage_id = 0; const auto& stage: plan.stages())
		{
			for(const auto parent: stage.parent_stages())
			{
				execution_plan.stages.add_edge(index_to_node[stage_id], index_to_node[parent]);
			}
			++stage_id;
		}

		return execution_plan;
	}
}