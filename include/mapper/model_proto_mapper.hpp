#ifndef HERDSMAN_MODEL_PROTO_MAPPER_HPP
#define HERDSMAN_MODEL_PROTO_MAPPER_HPP

#include <common.grpc.pb.h>
#include <storage.pb.h>
#include <execution.pb.h>

#include "herd/common/model/column_descriptor.hpp"
#include "herd/common/model/job.hpp"
#include "herd/common/model/schema_type.hpp"
#include "herd/common/model/execution_plan.hpp"


namespace mapper
{
	struct MappingError: public std::runtime_error
	{
		using std::runtime_error::runtime_error;
	};

	[[nodiscard]] herd::proto::SchemaType to_proto(herd::common::SchemaType schema_type);
	[[nodiscard]] herd::proto::DataType to_proto(herd::common::DataType data_type);
	[[nodiscard]] google::protobuf::RepeatedPtrField<herd::proto::ColumnDescriptor> to_proto(const herd::common::column_map_type& columns);
	[[nodiscard]] herd::proto::JobStatus to_proto(herd::common::JobStatus status);

	[[nodiscard]] herd::common::DataType to_model(herd::proto::DataType data_type);
	[[nodiscard]] herd::common::SchemaType to_model(herd::proto::SchemaType data_type);
	[[nodiscard]] herd::common::column_map_type to_model(const google::protobuf::RepeatedPtrField<herd::proto::ColumnDescriptor>& columns);
	[[nodiscard]] herd::common::ExecutionPlan to_model(const herd::proto::ExecutionPlan& plan);
}
#endif //HERDSMAN_MODEL_PROTO_MAPPER_HPP
