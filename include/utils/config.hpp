#ifndef HERDSMAN_CONFIG_HPP
#define HERDSMAN_CONFIG_HPP

#include <cstdint>
#include <filesystem>
#include <optional>
#include <vector>
#include <variant>

#include "address.hpp"


struct Config
{
	struct ServerConfig
	{
		Address listen_address;
		std::string key_directory;
		std::string storage_directory;
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

	struct GrpcWorkersConfig
	{
		std::vector<Address> addresses;
	};

	using workers_config_t = std::variant<GrpcWorkersConfig>;

	ServerConfig server;
	SecurityConfig security;
	LoggingConfig logging;
	workers_config_t workers;
};


Config load_config(const std::filesystem::path& path);

#endif //HERDSMAN_CONFIG_HPP
