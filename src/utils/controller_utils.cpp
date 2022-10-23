#include "utils/controller_utils.hpp"

#include "service/auth_service.hpp"


uint64_t extract_user_id_from_context(const grpc::ServerContext* context) noexcept
{
	assert(context);
	assert(context->auth_context());
	assert(context->auth_context()->GetPeerIdentity().size() == 1);

	const auto user_id_property = context->auth_context()->GetPeerIdentity().front();
	const auto user_id_string = std::string(user_id_property.data(), user_id_property.length());

	return AuthService::user_id_from_string(user_id_string);
}
