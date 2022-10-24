#ifndef HERDSMAN_AUTH_SERVICE_HPP
#define HERDSMAN_AUTH_SERVICE_HPP

#include <string>
#include <optional>
#include <chrono>
#include <stdexcept>

#include "utils/paseto_utils.hpp"


struct AuthenticationError: public std::runtime_error
{
	explicit AuthenticationError(const std::string& arg): std::runtime_error(arg) {};
};

struct InvalidTokenError: public std::runtime_error
{
	explicit InvalidTokenError(const std::string& arg): std::runtime_error(arg) {};
};

class AuthService
{
public:
	struct AuthToken
	{
		using token_bin_type = std::array<uint8_t, 16>;

		uint64_t user_id{};
		std::chrono::time_point<std::chrono::system_clock> session_start_time{};

		[[nodiscard]] token_bin_type to_binarray() const;
		static AuthToken from_binarray(const token_bin_type& bin_token);
	};

	static const char* const footer;

	explicit AuthService(const paseto_key_type& key, std::chrono::seconds token_lifetime);

	[[nodiscard]] std::optional<std::string> authenticate(std::string_view authentication_token) const;
	[[nodiscard]] AuthToken load_token(const std::string& connection_token) const;
	[[nodiscard]] bool is_auth_token_valid(
			const AuthToken& auth_token,
			std::chrono::time_point<std::chrono::system_clock> point = std::chrono::system_clock::now()
	) const noexcept;

	static uint64_t user_id_from_string(const std::string& user_id_string) noexcept;

private:
	paseto_key_type key_;
	std::chrono::seconds token_lifetime_;
};

#endif //HERDSMAN_AUTH_SERVICE_HPP
