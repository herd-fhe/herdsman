#ifndef HERDSMAN_SCHEMA_TYPE_MAPPER_HPP
#define HERDSMAN_SCHEMA_TYPE_MAPPER_HPP

#include <common.grpc.pb.h>
#include <storage.pb.h>

#include "herd_common/column_descriptor.hpp"
#include "herd_common/schema_type.hpp"


namespace mapper
{
	struct MappingError: public std::runtime_error
	{
		using std::runtime_error::runtime_error;
	};

	herd::common::SchemaType to_model(herd::proto::SchemaType data_type);
	herd::proto::SchemaType to_proto(herd::common::SchemaType data_type);

	herd::common::DataType to_model(herd::proto::DataType data_type);
	herd::common::column_map_type to_model(const google::protobuf::RepeatedPtrField<herd::proto::ColumnDescriptor>& columns);
}
#endif //HERDSMAN_SCHEMA_TYPE_MAPPER_HPP
