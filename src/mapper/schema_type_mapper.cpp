#include "mapper/schema_type_mapper.hpp"

#include "herd_common/schema_type.hpp"

namespace mapper
{
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

	herd::proto::SchemaType to_proto(herd::common::SchemaType data_type)
	{
		switch(data_type)
		{
			case herd::common::SchemaType::BINFHE:
				return herd::proto::BINFHE;
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

		for (uint8_t index = 0; const auto& column: columns)
		{
			model_columns.try_emplace(column.name(), index, to_model(column.type()));

			++index;
		}

		return model_columns;
	}
}