#include "controller/session_controller.hpp"

#include "spdlog/spdlog.h"

#include "utils/controller_utils.hpp"
#include "service/common_exceptions.hpp"


SessionController::SessionController(SessionService& session_service) noexcept
	:
	session_service_(session_service)
{
}

grpc::Status SessionController::create_session(
		grpc::ServerContext* context,
		const herd::SessionCreateRequest* request,
		herd::SessionInfo* response)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	if(request->name().empty())
	{
		spdlog::info("Failed to create session for user {}. Empty identifier", user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}

	try
	{
		const auto uuid = session_service_.create_session(user_id, request->name());
		*response->mutable_name() = request->name();
		*response->mutable_uuid() = uuid.as_string();
	}
	catch(const ObjectAlreadyExistsException& error)
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
		const herd::SessionDestroyRequest* request,
		herd::Empty* response)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	if(request->uuid().empty())
	{
		spdlog::info("Failed to delete session for user {}. Empty identifier", user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}

	try
	{
		const UUID session_uuid(request->uuid());
		session_service_.destroy_session_by_uuid(user_id, session_uuid);
	}
	catch(const ObjectNotFoundException& error)
	{
		spdlog::info("Failed to delete session {} for user {}. Session does not exist", request->uuid(), user_id);
		return {StatusCode::NOT_FOUND, "Session already exists"};
	}
	catch(const std::invalid_argument& error)
	{
		spdlog::info("Failed to delete session for user {}. Invalid identifier", user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid session identifier"};
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Service::destroy_session(context, request, response);
}

grpc::Status SessionController::list_sessions(
		grpc::ServerContext* context,
		const herd::Empty* request,
		herd::SessionInfoList* response)
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
