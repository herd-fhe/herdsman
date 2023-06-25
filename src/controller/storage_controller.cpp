#include "controller/storage_controller.hpp"

#include <spdlog/spdlog.h>

#include "herd/mapper/crypto.hpp"
#include "herd/mapper/storage.hpp"
#include "herd/mapper/exception.hpp"

#include "service/common_exceptions.hpp"
#include "utils/controller_utils.hpp"


namespace
{
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
			proto_column->set_type(herd::mapper::to_proto(type));
		}

		return proto_columns;
	}
}



StorageController::StorageController(StorageService& storage_service, SessionService& session_service, KeyService& key_service) noexcept
	: storage_service_(storage_service), session_service_(session_service), key_service_(key_service)
{
}

grpc::Status StorageController::add_data_frame(::grpc::ServerContext* context, ::grpc::ServerReaderWriter<::herd::proto::DataFrameAddResponse, ::herd::proto::DataFrameAddRequest>* stream)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	herd::proto::DataFrameAddRequest message;
	if(!stream->Read(&message))
	{
		spdlog::info("Invalid message received");
		return {StatusCode::INVALID_ARGUMENT, "Invalid message received"};
	}

	if(!message.has_info())
	{
		spdlog::info("First message does not contain metadata");
		return {StatusCode::INVALID_ARGUMENT, "First message does not contain metadata"};
	}

	try
	{
		const auto type = herd::mapper::to_model(message.info().type());
		const auto session_uuid = herd::common::UUID(message.info().session_uuid());
		const auto name = message.info().name();
		const auto row_count = message.info().row_count();
		const auto partitions = message.info().partitions();
		const auto columns = herd::mapper::to_model(message.info().columns());

		if(partitions == 0 || partitions > row_count)
		{
			spdlog::info("Invalid partition size");
			return {StatusCode::INVALID_ARGUMENT, "Invalid partition size"};
		}

		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", session_uuid.as_string(), user_id);
			return {StatusCode::FAILED_PRECONDITION, "Session not found"};
		}

		if(!key_service_.schema_key_exists_for_session(session_uuid, type))
		{
			spdlog::info("There is no matching cloud key associated to session with uuid: {}", session_uuid.as_string());
			return {StatusCode::FAILED_PRECONDITION, "Cloud key not found"};
		}

		const herd::common::UUID data_frame_uuid = storage_service_.create_data_frame(
				session_uuid, name,
				type, columns,
				row_count, partitions);

		herd::proto::DataFrameAddResponse response;
		response.mutable_metadata()->set_uuid(data_frame_uuid.as_string());
		response.mutable_metadata()->set_name(name);
		response.mutable_metadata()->set_schema_type(message.info().type());
		response.mutable_metadata()->mutable_columns()->CopyFrom(message.info().columns());
		response.mutable_metadata()->set_rows_count(row_count);
		response.mutable_metadata()->set_partitions(partitions);
		stream->Write(response);

		uint32_t received_rows = 0;

		while(stream->Read(&message))
		{
			if(!message.has_data())
			{
				spdlog::info("Second metadata packet detected");
				return {StatusCode::ABORTED, "Second metadata packet detected"};
			}

			const auto& blob = message.data().blob();
			if(blob.empty())
			{
				spdlog::info("Empty packet received. Deleting stored chunks...");
				storage_service_.remove_data_frame(session_uuid, data_frame_uuid);
				return {StatusCode::ABORTED, "Empty packet received"};
			}

			received_rows += storage_service_.append_to_data_frame(session_uuid, data_frame_uuid, reinterpret_cast<const uint8_t*>(blob.data()), blob.size());

			if(received_rows > row_count)
			{
				spdlog::info("Data frame bigger than specified in metadata. Deleting stored chunks...");
				storage_service_.remove_data_frame(session_uuid, data_frame_uuid);

				return {StatusCode::ABORTED, "Data frame bigger than specified"};
			}
		}

		if(received_rows != row_count)
		{
			spdlog::info("Data frame size differ from metadata info. Deleting stored chunks...");
			storage_service_.remove_data_frame(session_uuid, data_frame_uuid);

			return {StatusCode::ABORTED, "Data frame size differ from metadata info"};
		}

		storage_service_.mark_data_frame_as_uploaded(session_uuid, data_frame_uuid);
	}
	catch(const herd::mapper::MappingError&)
	{
		spdlog::info("Failed to upload data_frame for session {} associated to user {}. Invalid key type", message.info().session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid type identifier"};
	}
	catch(const std::invalid_argument&)
	{
		spdlog::info("Failed to upload data_frame for session {} associated to user {}. Invalid identifier", message.info().session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}

	return Status::OK;
}

grpc::Status StorageController::remove_data_frame(::grpc::ServerContext* context, const ::herd::proto::DataFrameRemoveRequest* request, ::herd::proto::Empty* response)
{
	static_cast<void>(response);

	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const auto session_uuid = herd::common::UUID(request->session_uuid());
		const auto data_frame_uuid = herd::common::UUID(request->uuid());

		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", session_uuid.as_string(), user_id);
			return {StatusCode::FAILED_PRECONDITION, "Session not found"};
		}

		if(!storage_service_.data_frame_exists(session_uuid, data_frame_uuid))
		{
			spdlog::info("There is no data frame with uuid: {} associated to session with uuid: {}", data_frame_uuid.as_string(), session_uuid.as_string());
			return {StatusCode::NOT_FOUND, "Data frame not found"};
		}

		// todo: remove race condition caused by temporary unlock
		if(storage_service_.data_frame_busy(session_uuid, data_frame_uuid))
		{
			spdlog::info("Data frame uuid: {} associated to session with uuid: {} busy", data_frame_uuid.as_string(), session_uuid.as_string());
			return {StatusCode::NOT_FOUND, "Data frame busy"};
		}

		storage_service_.remove_data_frame(session_uuid, data_frame_uuid);
	}
	catch(const std::invalid_argument&)
	{
		spdlog::info("Failed to remove data_frame ({}) for session {} associated to user {}. Invalid identifier", request->uuid(), request->session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}

	return Status::OK;
}

grpc::Status StorageController::list_data_frames(::grpc::ServerContext* context, const ::herd::proto::DataFrameListRequest* request, ::herd::proto::DataFrameMetadataList* response)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const auto session_uuid = herd::common::UUID(request->session_uuid());

		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", session_uuid.as_string(), user_id);
			return {StatusCode::FAILED_PRECONDITION, "Session not found"};
		}

		std::vector<StorageService::DataFrameEntry> data_frames;

		if(!request->has_type())
		{
			data_frames = storage_service_.list_session_data_frames(session_uuid);
		}
		else
		{
			const auto type = herd::mapper::to_model(request->type());
			data_frames = storage_service_.list_session_data_frames(session_uuid, type);
		}

		//check size
		response->mutable_dataframes()->Reserve(static_cast<int>(data_frames.size())); //todo: fix casting

		for(const auto& data_frame: data_frames)
		{
			auto frame_proto = response->mutable_dataframes()->Add();
			frame_proto->set_uuid(data_frame.uuid.as_string());
			frame_proto->set_name(data_frame.name);

			frame_proto->set_schema_type(herd::mapper::to_proto(data_frame.schema_type));

			frame_proto->mutable_columns()->CopyFrom(to_proto(data_frame.columns));
			frame_proto->set_rows_count(data_frame.row_count);
			frame_proto->set_partitions(data_frame.partitions);
		}
	}
	catch(const std::invalid_argument&)
	{
		spdlog::info("Failed to list data_frames for session {} associated to user {}. Invalid identifier", request->session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}

	return Status::OK;
}

grpc::Status StorageController::download_data_frame(::grpc::ServerContext* context, const ::herd::proto::DataFrameDownloadDataRequest* request, ::herd::proto::DataFrameDownloadDataResponse* response)
{
	static_cast<void>(context);
	static_cast<void>(request);
	static_cast<void>(response);

	using namespace grpc;

	return {StatusCode::UNIMPLEMENTED, "To be implemented"};
}
