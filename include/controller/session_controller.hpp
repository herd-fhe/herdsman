#ifndef HERDSMAN_SESSION_CONTROLLER_HPP
#define HERDSMAN_SESSION_CONTROLLER_HPP

#include <session.grpc.pb.h>

#include "service/session_service.hpp"
#include "service/key_service.hpp"


class SessionController: public herd::proto::Session::Service
{
public:
	SessionController(SessionService& session_service, KeyService& key_service) noexcept;

	//session manipulation
	grpc::Status create_session(
			grpc::ServerContext *context,
			const herd::proto::SessionCreateRequest *request,
            herd::proto::SessionInfo *response
	) override;

	grpc::Status destroy_session(
			grpc::ServerContext *context,
			const herd::proto::SessionDestroyRequest *request,
			herd::proto::Empty *response
	) override;

	grpc::Status list_sessions(
			grpc::ServerContext *context,
			const herd::proto::Empty *request,
	        herd::proto::SessionInfoList *response
	) override;

	//session encryption key management
	grpc::Status add_key(
			grpc::ServerContext* context,
			grpc::ServerReader<herd::proto::SessionAddKeyRequest>* reader,
			herd::proto::Empty* response
	) override;

	grpc::Status remove_key(
			grpc::ServerContext* context,
			const herd::proto::SessionRemoveKeyRequest* request,
			herd::proto::Empty* response
	) override;

	grpc::Status list_keys(
			grpc::ServerContext* context,
		    const herd::proto::SessionKeyListRequest* request,
			herd::proto::SessionKeyList* response
	) override;

private:
	SessionService& session_service_;
	KeyService& key_service_;
};

#endif //HERDSMAN_SESSION_CONTROLLER_HPP
