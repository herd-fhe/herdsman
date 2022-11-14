#include "service/auth_service.hpp"

#include <paseto.h>
#include <spdlog/spdlog.h>

#include <cstring>


const char* const AuthService::footer = "herdsman";

std::array<uint8_t, 16> AuthService::AuthToken::to_binarray() const
{
	std::array<uint8_t, 16> binarray{};

	memcpy(binarray.data(), reinterpret_cast<const std::byte*>(&user_id), sizeof(user_id));
	const auto session_start_from_epoch = session_start_time.time_since_epoch();
	const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(session_start_from_epoch).count();

	memcpy(binarray.data() + sizeof(user_id), reinterpret_cast<const std::byte*>(&timestamp), sizeof(timestamp));

	return binarray;
}

AuthService::AuthToken AuthService::AuthToken::from_binarray(const std::array<uint8_t, 16>& bin_token)
{
	AuthToken token{};

	memcpy(reinterpret_cast<std::byte*>(&token.user_id), bin_token.data(), sizeof(user_id));

	int64_t timestamp;
	memcpy(reinterpret_cast<std::byte*>(&timestamp), bin_token.data() + sizeof(user_id), sizeof(timestamp));

	std::chrono::seconds seconds_from_epoch(timestamp);
	std::chrono::time_point<std::chrono::system_clock> session_start_time{seconds_from_epoch};
	token.session_start_time = session_start_time;

	return token;
}

AuthService::AuthService(const paseto_key_type& key, std::chrono::seconds token_lifetime)
	:key_(key), token_lifetime_(token_lifetime)
{
}

std::optional<std::string> AuthService::authenticate(std::string_view authentication_token) const
{
	const auto session_start = std::chrono::system_clock::now();

	AuthToken token {.user_id = 0, .session_start_time = session_start};
	if(authentication_token == "admin==true")
	{
		const auto token_binarray = token.to_binarray();
		const auto encrypted_token = paseto_v2_local_encrypt(
				token_binarray.data(),
				token_binarray.size(),
				key_.data(),
				reinterpret_cast<const uint8_t*>(AuthService::footer),
				std::strlen(AuthService::footer)
		);

		if(!encrypted_token)
		{
			throw AuthenticationError("Invalid token");
		}

		std::string token_str(encrypted_token);
		paseto_free(encrypted_token);

		return token_str;
	}
	else
	{
		return std::nullopt;
	}
}

AuthService::AuthToken AuthService::load_token(const std::string& connection_token) const
{
	const auto token_size = std::tuple_size_v<AuthToken::token_bin_type>;
	size_t decrypted_size = 0;

	const auto token_data = paseto_v2_local_decrypt(connection_token.c_str(), &decrypted_size, key_.data(), nullptr, nullptr);
	if(!token_data || decrypted_size != token_size)
	{
		throw InvalidTokenError("Invalid token format");
	}

	AuthToken::token_bin_type token_bin{};
	std::copy(token_data, token_data + token_size, std::begin(token_bin));

	return AuthToken::from_binarray(token_bin);
}

bool AuthService::is_auth_token_valid(
		const AuthToken& auth_token,
		std::chrono::time_point<std::chrono::system_clock> point
) const noexcept
{
	return auth_token.session_start_time + token_lifetime_ > point;
}

uint64_t AuthService::user_id_from_string(const std::string& user_id_string) noexcept
{
	std::size_t processed_size;
	auto user_id = std::stoull(user_id_string, &processed_size);

	assert(processed_size == user_id_string.size());

	return user_id;
}

