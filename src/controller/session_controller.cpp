#include "controller/session_controller.hpp"

#include "spdlog/spdlog.h"

#include "mapper/schema_type_mapper.hpp"
#include "utils/controller_utils.hpp"
#include "service/common_exceptions.hpp"


SessionController::SessionController(SessionService& session_service, KeyService& key_service) noexcept
	:
	session_service_(session_service),
	key_service_(key_service)
{
}

grpc::Status SessionController::create_session(
		grpc::ServerContext* context,
		const herd::proto::SessionCreateRequest* request,
		herd::proto::SessionInfo* response)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	if(request->name().empty())
	{
		spdlog::info("Failed to create session for user {}. Empty identifier", user_id);
		return {StatusCode::INVALID_ARGUMENT, "Empty session identifier"};
	}

	try
	{
		const auto uuid = session_service_.create_session(user_id, request->name());
		*response->mutable_name() = request->name();
		*response->mutable_uuid() = uuid.as_string();
	}
	catch(const ObjectAlreadyExistsException&)
	{
		spdlog::info("Failed to create session for user {}. Session {} already exists", user_id, request->name());
		return {StatusCode::ALREADY_EXISTS, "Session already exists"};
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}

grpc::Status SessionController::destroy_session(
		grpc::ServerContext* context,
		const herd::proto::SessionDestroyRequest* request,
		herd::proto::Empty* response)
{
	static_cast<void>(response);

	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const UUID session_uuid(request->uuid());
		session_service_.destroy_session_by_uuid(user_id, session_uuid);
	}
	catch(const ObjectNotFoundException&)
	{
		spdlog::info("Failed to delete session {} for user {}. Session does not exist", request->uuid(), user_id);
		return {StatusCode::NOT_FOUND, "Session already exists"};
	}
	catch(const std::invalid_argument&)
	{
		spdlog::info("Failed to delete session for user {}. Invalid identifier", user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}

grpc::Status SessionController::list_sessions(
		grpc::ServerContext* context,
		const herd::proto::Empty* request,
		herd::proto::SessionInfoList* response)
{
	static_cast<void>(request);

	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const auto sessions = session_service_.sessions_by_user(user_id);
		for(const auto& session: sessions)
		{
			auto info = response->mutable_sessions()->Add();

			*info->mutable_uuid() = session.uuid.as_string();
			*info->mutable_name() = session.name;
		}
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}

grpc::Status SessionController::add_key(
		grpc::ServerContext* context,
		grpc::ServerReader<herd::proto::SessionAddKeyRequest>* reader,
		herd::proto::Empty* response)
{
	static_cast<void>(response);

	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	herd::proto::SessionAddKeyRequest message;
	if(!reader->Read(&message))
	{
		spdlog::info("Failed to receive key metadata");
		return {StatusCode::INVALID_ARGUMENT, "Failed to receive key metadata"};
	}

	if(!message.has_options())
	{
		spdlog::info("Failed to receive key metadata");
		return {StatusCode::INVALID_ARGUMENT, "Failed to receive key metadata"};
	}

	try
	{
		const auto type = mapper::to_model(message.options().type());
		const auto session_uuid = UUID(message.options().session_uuid());
		const auto size = message.options().size();

		std::vector<std::byte> key_data;
		key_data.resize(size);

		uint32_t received = 0;

		while(reader->Read(&message))
		{
			if(!message.has_data())
			{
				spdlog::info("Encountered second metadata packet. Aborting upload...");
				return {StatusCode::ABORTED, "Second metadata packet detected"};
			}

			const auto& blob = message.data().blob();

			if(received + static_cast<uint32_t>(blob.size()) > size)
			{
				spdlog::info("File bigger than specified in metadata. Aborting upload...");
				return {StatusCode::ABORTED, "File bigger than specified in metadata"};
			}

			std::memcpy(key_data.data() + received, blob.data(), blob.size());
			received += static_cast<uint32_t>(blob.size());
		}

		if(received != size)
		{
			spdlog::info("Missing file part. Aborting upload...");
			return {StatusCode::ABORTED, "Missing file part"};
		}

		key_service_.add_key(session_uuid, type, key_data);
	}
	catch(const mapper::MappingError&)
	{
		spdlog::info("Failed to upload key for session {} associated to user {}. Invalid key type", message.options().session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid type identifier"};
	}
	catch(const std::invalid_argument&)
	{
		spdlog::info("Failed to upload key for session {} associated to user {}. Invalid identifier", message.options().session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}

	return Status::OK;
}

grpc::Status SessionController::remove_key(
		grpc::ServerContext* context,
		const herd::proto::SessionRemoveKeyRequest* request,
		herd::proto::Empty* response)
{
	static_cast<void>(response);

	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const UUID session_uuid(request->session_uuid());
		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", request->session_uuid(), user_id);
			return {StatusCode::NOT_FOUND, "Session not found"};
		}

		key_service_.remove_key(session_uuid, mapper::to_model(request->type()));
	}
	catch(const mapper::MappingError&)
	{
		spdlog::info("Failed to delete key for session {} associated to user {}. Invalid key type", request->session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}
	catch(const std::invalid_argument&)
	{
		spdlog::info("Failed to delete key for session {} associated to user {}. Invalid identifier", request->session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}

grpc::Status SessionController::list_keys(
		grpc::ServerContext* context,
		const herd::proto::SessionKeyListRequest* request,
		herd::proto::SessionKeyList* response)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const UUID session_uuid(request->session_uuid());
		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", request->session_uuid(), user_id);
			return {StatusCode::NOT_FOUND, "Session not found"};
		}
		const auto key_types = key_service_.list_available_keys(session_uuid);

		for(const auto type: key_types)
		{
			response->add_type(mapper::to_proto(type));
		}
	}
	catch(const std::invalid_argument&)
	{
		spdlog::info("Failed to list keys for session {} associated to user {}. Invalid identifier", request->session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}
