#ifndef HERDSMAN_SCHEMA_TYPE_MAPPER_HPP
#define HERDSMAN_SCHEMA_TYPE_MAPPER_HPP

#include <common.grpc.pb.h>

#include "schema_type.hpp"


namespace mapper
{
	struct MappingError: public std::runtime_error
	{
		using std::runtime_error::runtime_error;
	};

	SchemaType to_model(herd::proto::SchemaType type) noexcept;
	herd::proto::SchemaType to_proto(SchemaType type);
}
#endif //HERDSMAN_SCHEMA_TYPE_MAPPER_HPP
