#ifndef HERDSMAN_CONFIG_HPP
#define HERDSMAN_CONFIG_HPP

#include <cstdint>
#include <filesystem>
#include <optional>


struct Config
{
	struct ServerConfig
	{
		std::string address;
		uint16_t port{};
	};

	struct SecurityConfig
	{
		struct SSLConfig
		{
			std::string ca_certificate_path;
			std::string certificate_path;
			std::string certificate_key_path;
		};

		std::string secret_key;
		uint64_t token_lifetime;

		std::optional<SSLConfig> ssl_config;
	};

	struct LoggingConfig
	{
		enum class LogLevel
		{
			INFO,
			WARNING,
			ERROR,
			DEBUG
		};

		LogLevel level;
	};

	ServerConfig server;
	SecurityConfig security;
	LoggingConfig logging;
};


Config load_config(const std::filesystem::path& path);

#endif //HERDSMAN_CONFIG_HPP
