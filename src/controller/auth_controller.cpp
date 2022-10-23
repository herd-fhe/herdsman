#include "controller/auth_controller.hpp"

#include <spdlog/spdlog.h>

#include "utils/controller_utils.hpp"


AuthController::AuthController(AuthService& auth_service) noexcept
: auth_service_(auth_service)
{}

grpc::Status AuthController::authorize_connection(
		grpc::ServerContext *context,
		const herd::AuthenticationToken *request,
		herd::ConnectionToken *response
)
{
	using namespace grpc;
	static_cast<void>(context);

	try
	{
		const auto token = auth_service_.authenticate(request->authentication_token());
		if(!token)
		{
			return {StatusCode::UNAUTHENTICATED, "Invalid authentication token"};
		}

		response->set_token(token.value());
	}
	catch(const AuthenticationError& error)
	{
		spdlog::info(error.what());
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}
