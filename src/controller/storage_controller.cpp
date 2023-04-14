#include "controller/storage_controller.hpp"

#include <spdlog/spdlog.h>

#include "mapper/schema_type_mapper.hpp"
#include "utils/controller_utils.hpp"
#include "service/common_exceptions.hpp"


StorageController::StorageController(StorageService& storage_service) noexcept
	: storage_service_(storage_service)
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
		const auto type = mapper::to_model(message.info().type());
		const auto session_uuid = UUID(message.info().session_uuid());
		const auto name = message.info().name();
		const auto rows = message.info().row_count();
		const auto columns = mapper::to_model(message.info().columns());

		const UUID data_frame_uuid = storage_service_.create_data_frame(session_uuid, name, type, columns);

		herd::proto::DataFrameAddResponse response;
		response.mutable_metadata()->set_uuid(data_frame_uuid.as_string());
		response.mutable_metadata()->set_name(name);
		response.mutable_metadata()->set_schema_type(message.info().type());
		response.mutable_metadata()->mutable_columns()->CopyFrom(message.info().columns());
		response.mutable_metadata()->set_rows_count(rows);
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

			if(received_rows > rows)
			{
				spdlog::info("Data frame bigger than specified in metadata. Deleting stored chunks...");
				storage_service_.remove_data_frame(session_uuid, data_frame_uuid);

				return {StatusCode::ABORTED, "Data frame bigger than specified"};
			}
		}

		if(received_rows != rows)
		{
			spdlog::info("Data frame size differ from metadata info. Deleting stored chunks...");
			storage_service_.remove_data_frame(session_uuid, data_frame_uuid);

			return {StatusCode::ABORTED, "Data frame size differ from metadata info"};
		}

		storage_service_.mark_data_frame_as_uploaded(session_uuid, data_frame_uuid);
	}
	catch(const mapper::MappingError&)
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
	return Service::remove_data_frame(context, request, response);
}

grpc::Status StorageController::list_data_frames(::grpc::ServerContext* context, const ::herd::proto::DataFrameListRequest* request, ::herd::proto::DataFrameMetadataList* response)
{
	return Service::list_data_frames(context, request, response);
}

grpc::Status StorageController::download_data_frame(::grpc::ServerContext* context, const ::herd::proto::DataFrameDownloadDataRequest* request, ::herd::proto::DataFrameDownloadDataResponse* response)
{
	return Service::download_data_frame(context, request, response);
}
