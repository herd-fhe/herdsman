#ifndef HERDSMAN_SESSION_CONTROLLER_HPP
#define HERDSMAN_SESSION_CONTROLLER_HPP

#include <session.grpc.pb.h>

#include "service/session_service.hpp"


class SessionController: public herd::proto::Session::Service
{
public:
	explicit SessionController(SessionService& session_service) noexcept;

	grpc::Status create_session(
			grpc::ServerContext *context,
			const herd::proto::SessionCreateRequest *request,
            herd::proto::SessionInfo *response
	) override;

	grpc::Status destroy_session(
			grpc::ServerContext *context,
			const herd::proto::SessionDestroyRequest *request,
			herd::Empty *response
	) override;

	grpc::Status list_sessions(
			grpc::ServerContext *context,
			const herd::Empty *request,
	        herd::proto::SessionInfoList *response
	) override;

private:
	SessionService& session_service_;
};

#endif //HERDSMAN_SESSION_CONTROLLER_HPP
