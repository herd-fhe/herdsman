#include <utility>

#include "plugins/token_auth_metadata_processor.hpp"

#include "spdlog/spdlog.h"

#include "utils/string_utils.hpp"


namespace
{
	constexpr const char* const BEARER_PREFIX = "Bearer";
	constexpr const char* const PATH_KEY = ":path";
}

TokenAuthMetadataProcessor::TokenAuthMetadataProcessor(
		AuthService& auth_service,
		std::vector<std::string> paths_not_secured):
	auth_service_(auth_service),
	paths_not_secured_(std::move(paths_not_secured))
{
}

grpc::Status TokenAuthMetadataProcessor::Process(
		const grpc_impl::AuthMetadataProcessor::InputMetadata& auth_metadata,
		grpc::AuthContext* context,
		grpc_impl::AuthMetadataProcessor::OutputMetadata* consumed_auth_metadata,
		grpc_impl::AuthMetadataProcessor::OutputMetadata* response_metadata)
{
	static_cast<void>(response_metadata);

	using namespace grpc;

	if(const auto path = auth_metadata.find(PATH_KEY); path != std::end(auth_metadata))
	{
		const bool matched = std::ranges::any_of(
				paths_not_secured_,
				[&path](const auto& open_path)
				{
					return open_path == path->second;
				}
        );

		if(matched)
		{
			return Status::OK;
		}
	}

	if(const auto token_metadata = auth_metadata.find(AUTH_TOKEN_KEY); token_metadata != std::end(auth_metadata))
	{
		if(const auto token_metadata_value = std::string(token_metadata->second.data(), token_metadata->second.length()); token_metadata_value.starts_with(BEARER_PREFIX))
		{
			auto token_metadata_value_begin = std::begin(token_metadata_value);
			std::advance(token_metadata_value_begin, strlen(BEARER_PREFIX));

			const auto token_value = trim(token_metadata_value_begin, std::end(token_metadata_value));

			try
			{
				if(const auto token = auth_service_.load_token(token_value); auth_service_.is_auth_token_valid(token))
				{
					const auto user_id_str = std::to_string(token.user_id);

					if(!context->FindPropertyValues(PEER_IDENTITY_PROPERTY_NAME).empty())
					{
						assert(context->GetPeerIdentity().size() == 1);
						const auto connection_user_id = context->GetPeerIdentity();

						if(user_id_str != connection_user_id.front())
						{
							return {StatusCode::UNAUTHENTICATED, "Connection already used by other user. Please open new connection to server"};
						}
					}
					else
					{
						context->AddProperty(PEER_IDENTITY_PROPERTY_NAME, user_id_str);
						context->SetPeerIdentityPropertyName(PEER_IDENTITY_PROPERTY_NAME);
					}

					consumed_auth_metadata->insert(std::make_pair(
							string(token_metadata->first.data(), token_metadata->first.length()),
							string(token_metadata->second.data(), token_metadata->second.length())));

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
	return {StatusCode::UNAUTHENTICATED, "No token provided"};
}
