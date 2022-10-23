#include <utility>

#include "plugins/token_auth_metadata_processor.hpp"

#include "spdlog/spdlog.h"

#include "utils/string_utils.hpp"


namespace
{
	constexpr const char* const BEARER_PREFIX = "Bearer";
}

TokenAuthMetadataProcessor::TokenAuthMetadataProcessor(
		AuthService& auth_service,
		std::vector<std::string> paths_not_secured
)
: auth_service_(auth_service), paths_not_secured_(std::move(paths_not_secured))
{
}

grpc::Status TokenAuthMetadataProcessor::Process(
		const grpc_impl::AuthMetadataProcessor::InputMetadata &auth_metadata,
		grpc::AuthContext *context,
		grpc_impl::AuthMetadataProcessor::OutputMetadata *consumed_auth_metadata,
		grpc_impl::AuthMetadataProcessor::OutputMetadata *response_metadata)
{
	static_cast<void>(response_metadata);

	using namespace grpc;

	const auto token_metadata = auth_metadata.find(AUTH_TOKEN_KEY);
	if(token_metadata != std::end(auth_metadata))
	{
		const auto token_metadata_value = std::string(token_metadata->second.data(), token_metadata->second.length());
		if(token_metadata_value.starts_with(BEARER_PREFIX))
		{
			auto token_metadata_value_begin = std::begin(token_metadata_value);
			std::advance(token_metadata_value_begin, strlen(BEARER_PREFIX));

			const auto token_value = trim(token_metadata_value_begin, std::end(token_metadata_value));

			try
			{
				const auto token = auth_service_.load_token(token_value);
				if(auth_service_.is_auth_token_valid(token))
				{
					context->AddProperty(PEER_IDENTITY_PROPERTY_NAME, std::to_string(token.user_id));
					context->SetPeerIdentityPropertyName(PEER_IDENTITY_PROPERTY_NAME);

					consumed_auth_metadata->insert(std::make_pair(
						string(token_metadata->first.data(), token_metadata->first.length()),
						string(token_metadata->second.data(), token_metadata->second.length())
					));

					return Status::OK;
				}

				spdlog::info("Auth token expired");
				return {StatusCode::UNAUTHENTICATED, "Auth token expired"};
			}
			catch(const InvalidTokenError& error)
			{
				spdlog::info("Failed to decode connection token");
				return {StatusCode::UNAUTHENTICATED, error.what()};
			}
		}
		spdlog::info("Invalid auth token format");
		return {StatusCode::UNAUTHENTICATED, "Invalid auth token format"};
	}
	return Status::OK;
}
