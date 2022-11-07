#include "mapper/schema_type_mapper.hpp"


namespace mapper
{

	SchemaType to_model(herd::proto::SchemaType type) noexcept
	{
		switch(type)
		{
			case herd::proto::BINFHE:
				return SchemaType::BINFHE;
			default:
				assert(false && "Proto schema, model mismatch");
				return static_cast<SchemaType>(0);
		}
	}

	herd::proto::SchemaType to_proto(SchemaType type)
	{
		switch(type)
		{
			case SchemaType::BINFHE:
				return herd::proto::BINFHE;
			default:
				throw MappingError("Proto schema, model mismatch");
		}
	}
}