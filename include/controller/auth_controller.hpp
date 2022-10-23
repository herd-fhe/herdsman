#ifndef HERDSMAN_AUTH_CONTROLLER_HPP
#define HERDSMAN_AUTH_CONTROLLER_HPP

#include <auth.grpc.pb.h>

#include "service/auth_service.hpp"


class AuthController: public herd::Auth::Service
{
public:
	explicit AuthController(AuthService& auth_service) noexcept;

	grpc::Status authorize_connection(
			grpc::ServerContext *context,
			const herd::AuthenticationToken *request,
			herd::ConnectionToken *response
	) override;

private:
	AuthService& auth_service_;
};




#endif //HERDSMAN_AUTH_CONTROLLER_HPP
