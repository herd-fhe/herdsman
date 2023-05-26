#include "mapper/model_proto_mapper.hpp"

#include "herd/common/model/schema_type.hpp"


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

	herd::proto::Operation to_proto(herd::common::Operation operation)
	{
		switch(operation)
		{
			using enum herd::common::Operation;
			case AND:
				return herd::proto::Operation::AND;
			case OR:
				return herd::proto::Operation::OR;
			case NOT:
				return herd::proto::Operation::NOT;
			case MUX:
			case NAND:
			case XOR:
				throw MappingError("Not supported");
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}

	herd::proto::Node to_proto(const herd::common::node_t& node)
	{
		using namespace herd;
		proto::Node node_proto;

		if(std::holds_alternative<common::InputNode>(node))
		{
			auto input_proto = node_proto.mutable_input();
			const auto& input = std::get<common::InputNode>(node);

			input_proto->set_bit_index(input.bit_index);
			input_proto->set_tuple_index(input.tuple_index);
		}
		else if(std::holds_alternative<common::OutputNode>(node))
		{
			auto output_proto = node_proto.mutable_output();
			const auto& output = std::get<common::OutputNode>(node);

			output_proto->set_bit_index(output.bit_index);
			output_proto->set_tuple_index(output.tuple_index);
		}
		else if(std::holds_alternative<common::ConstantNode>(node))
		{
			auto constant_proto = node_proto.mutable_constant();
			const auto& constant = std::get<common::ConstantNode>(node);

			constant_proto->set_value(constant.value);
		}
		else if(std::holds_alternative<common::OperationNode>(node))
		{
			auto operation_proto = node_proto.mutable_operation();
			const auto& operation = std::get<common::OperationNode>(node);

			operation_proto->set_type(to_proto(operation.type));
		}
		else
		{
			throw MappingError("Proto schema, model mismatch");
		}
		return node_proto;
	}

	herd::proto::Circuit to_proto(const herd::common::Circuit& circuit)
	{
		using namespace herd;

		proto::Circuit circuit_proto{};

		const auto input_proto = circuit_proto.mutable_input();
		const auto output_proto = circuit_proto.mutable_input();
		input_proto->Add(std::begin(circuit.input), std::end(circuit.input));
		output_proto->Add(std::begin(circuit.output), std::end(circuit.output));

		const auto operations_proto = circuit_proto.mutable_nodes();
		for(const auto& operation: circuit.circuit_graph)
		{
			operations_proto->Add(to_proto(operation.value()));
		}

		const auto edges_proto = circuit_proto.mutable_edges();
		for(const auto& [start, end]: circuit.circuit_graph.edges())
		{
			const auto edge_proto = edges_proto->Add();
			edge_proto->set_start(static_cast<unsigned int>(start));
			edge_proto->set_end(static_cast<unsigned int>(end));
		}

		return circuit_proto;
	}

	herd::proto::Stage to_proto(const herd::common::stage_t& stage)
	{
		using namespace herd;

		proto::Stage stage_proto{};

		if(std::holds_alternative<common::InputStage>(stage))
		{
			const auto& input_stage = std::get<common::InputStage>(stage);
			stage_proto.mutable_input()->set_data_frame_uuid(input_stage.data_frame_uuid.as_string());
		}
		else if(std::holds_alternative<common::OutputStage>(stage))
		{
			const auto& output_stage = std::get<common::OutputStage>(stage);
			if(output_stage.name.has_value())
			{
				stage_proto.mutable_output()->set_name(output_stage.name.value());
			}
		}
		else if(std::holds_alternative<common::MapperStage>(stage))
		{
			const auto& mapper_stage = std::get<common::MapperStage>(stage);
			const auto circuit_proto = stage_proto.mutable_mapper()->mutable_circuit();
			circuit_proto->CopyFrom(to_proto(mapper_stage.circuit));
		}
		else
		{
			throw MappingError("Proto schema, model mismatch");
		}

		return stage_proto;
	}

	herd::proto::ExecutionPlan to_proto(const herd::common::ExecutionPlan& plan)
	{
		using namespace herd;

		proto::ExecutionPlan proto_plan;
		proto_plan.set_schema_type(to_proto(plan.schema_type));

		const auto execution_graph_proto = proto_plan.mutable_execution_graph();
		const auto stages_proto = execution_graph_proto->mutable_stages();

		for(const auto& stage: plan.execution_graph)
		{
			const auto& stage_details = stage.value();;
			stages_proto->Add(to_proto(stage_details));
		}

		const auto edges_proto = execution_graph_proto->mutable_edges();
		for(const auto& [start, end]: plan.execution_graph.edges())
		{
			const auto edge_proto = edges_proto->Add();
			edge_proto->set_start(static_cast<unsigned int>(start));
			edge_proto->set_end(static_cast<unsigned int>(end));
		}

		return proto_plan;
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

	herd::common::Operation to_model(herd::proto::Operation operation_proto)
	{
		using namespace herd;
		switch(operation_proto)
		{
			case proto::AND:
				return herd::common::Operation::AND;
			case proto::OR:
				return common::Operation::OR;
			case proto::NOT:
				return common::Operation::NOT;
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}

	herd::common::node_t to_model(const herd::proto::Node& node_proto)
	{
		using namespace herd;
		if(node_proto.has_input())
		{
			const auto& input_node_proto = node_proto.input();
			return common::InputNode{
					input_node_proto.tuple_index(),
					input_node_proto.bit_index()
			};
		}
		else if(node_proto.has_output())
		{
			const auto& output_node_proto = node_proto.output();
			return common::OutputNode{
					output_node_proto.tuple_index(),
					output_node_proto.bit_index()
			};
		}
		else if(node_proto.has_constant())
		{
			const auto& constant_node_proto = node_proto.constant();
			return common::ConstantNode{
					constant_node_proto.value()
			};
		}
		else if(node_proto.has_operation())
		{
			const auto& operation_node_proto = node_proto.operation();
			return common::OperationNode{
					to_model(operation_node_proto.type())
			};
		}
		else
		{
			throw MappingError("Proto schema, model mismatch");
		}
	}

	herd::common::Circuit to_model(const herd::proto::Circuit& circuit_proto)
	{
		using namespace herd;
		common::Circuit circuit{};

		circuit.input.insert(std::end(circuit.input), std::begin(circuit_proto.input()), std::end(circuit_proto.input()));
		circuit.output.insert(std::end(circuit.output), std::begin(circuit_proto.output()), std::end(circuit_proto.output()));

		auto& graph = circuit.circuit_graph;

		for(const auto& node: circuit_proto.nodes())
		{
			graph.insert(to_model(node));
		}

		for(const auto& edge: circuit_proto.edges())
		{
			const auto start = edge.start();
			const auto end = edge.end();

			const auto begin = std::begin(graph);

			graph.add_edge(begin + start, begin + end);
		}

		return circuit;
	}

	herd::common::stage_t to_model(const herd::proto::Stage& stage_proto)
	{
		using namespace herd;

		if(stage_proto.has_input())
		{
			const auto& input_stage_proto = stage_proto.input();

			common::InputStage input_stage;
			input_stage.data_frame_uuid = common::UUID(input_stage_proto.data_frame_uuid());
			return input_stage;
		}
		else if(stage_proto.has_output())
		{
			const auto& output_stage_proto = stage_proto.output();

			common::OutputStage output_stage;
			if(output_stage_proto.has_name())
			{
				output_stage.name = output_stage_proto.name();
			}

			return output_stage;
		}
		else if(stage_proto.has_mapper())
		{
			const auto& mapper_stage_proto = stage_proto.mapper();

			common::MapperStage mapper_stage;
			mapper_stage.circuit = to_model(mapper_stage_proto.circuit());
			return mapper_stage;
		}

		throw MappingError("Proto schema, model mismatch");
	}

	herd::common::ExecutionPlan to_model(const herd::proto::ExecutionPlan& execution_plan_proto)
	{
		using namespace herd;

		common::ExecutionPlan execution_plan{};

		execution_plan.schema_type = to_model(execution_plan_proto.schema_type());

		const auto execution_graph_proto = execution_plan_proto.execution_graph();
		auto& execution_graph = execution_plan.execution_graph;
		for(const auto& stage_proto: execution_graph_proto.stages())
		{
			execution_graph.insert(to_model(stage_proto));
		}

		for(const auto& edge: execution_graph_proto.edges())
		{
			const auto start = edge.start();
			const auto end = edge.end();

			const auto begin_iter = std::begin(execution_graph);

			execution_graph.add_edge(begin_iter+start, begin_iter+end);
		}

		return execution_plan;
	}
}