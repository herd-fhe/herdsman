#ifndef HERDSMAN_TOKEN_AUTH_METADATA_PROCESSOR_HPP
#define HERDSMAN_TOKEN_AUTH_METADATA_PROCESSOR_HPP

#include <grpcpp/grpcpp.h>

#include "service/auth_service.hpp"


class TokenAuthMetadataProcessor: public grpc::AuthMetadataProcessor
{
public:
	constexpr static const char* const AUTH_TOKEN_KEY = "authorization";
	constexpr static const char* const PEER_IDENTITY_PROPERTY_NAME = "user_id";

	TokenAuthMetadataProcessor(AuthService& auth_service, std::vector<std::string>  paths_not_secured);

	grpc::Status Process(
			const InputMetadata& auth_metadata, grpc::AuthContext* context,
			OutputMetadata* consumed_auth_metadata,
	        OutputMetadata* response_metadata
	) override;

	[[nodiscard]] bool IsBlocking() const override
	{
		return false;
	}

private:
	AuthService& auth_service_;
	std::vector<std::string> paths_not_secured_;
};

#endif //HERDSMAN_TOKEN_AUTH_METADATA_PROCESSOR_HPP
