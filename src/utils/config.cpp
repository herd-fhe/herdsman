#include "utils/config.hpp"

#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>


namespace
{
	template<typename T>
	T get_value(const YAML::Node& root_node, const std::string& name)
	{
		if(const auto node = root_node[name]; node)
		{
			return node.as<T>();
		}
		spdlog::error("Failed to read node " +name);
		throw std::runtime_error("Failed to read node " +name);
	}

	template<typename T>
	T get_optional_value(const YAML::Node& root_node, const std::string& name, T default_value)
	{
		if(const auto node = root_node[name]; node)
		{
			return node.as<T>();
		}
		else
		{
			return default_value;
		}
	}

	Config::SecurityConfig::SSLConfig load_ssl_config(const YAML::Node& ssl_node)
	{
		Config::SecurityConfig::SSLConfig ssl_config;
		if(!ssl_node["ca_certificate"] || !ssl_node["certificate"] || !ssl_node["certificate_key"])
		{
			spdlog::error("Failed to read node ssl_config. Both certificate and certificate_key_path must be provided");
			throw std::runtime_error("Failed to read node server.  Both certificate and certificate_key_path must be provided");
		}
		ssl_config.ca_certificate_path = get_value<std::string>(ssl_node, "ca_certificate");
		ssl_config.certificate_path = get_value<std::string>(ssl_node, "certificate");
		ssl_config.certificate_key_path = get_value<std::string>(ssl_node, "certificate_key");

		return ssl_config;
	}

	Config::ServerConfig load_server_config(const YAML::Node& node)
	{
		Config::ServerConfig server_config{};

		server_config.address = get_optional_value<std::string>(node, "address", "0.0.0.0");
		server_config.port = get_optional_value<uint16_t>(node, "port", 5000);

		return server_config;
	}

	Config::SecurityConfig load_security_config(const YAML::Node& node)
	{
		Config::SecurityConfig security_config;

		security_config.secret_key = get_value<std::string>(node, "secret_key");
		security_config.token_lifetime = get_optional_value<uint64_t>(node, "token_lifetime", 3600);

		if(const auto ssl_node = node["ssl"]; ssl_node)
		{
			security_config.ssl_config = load_ssl_config(ssl_node);
		}
		else
		{
			security_config.ssl_config = std::nullopt;
		}
		return security_config;
	}
}

Config load_config(const std::filesystem::path &path)
{
	if(!std::filesystem::exists(path))
	{
		throw std::runtime_error("File " + path.string() + " not found");
	}

	if(!std::filesystem::is_regular_file(path))
	{
		throw std::runtime_error(path.string() + "is not a regular file");
	}

	const YAML::Node root_node = YAML::LoadFile(path.c_str());

	Config config;

	if(const auto node = root_node["server"]; node)
	{
		config.server = load_server_config(node);
	}
	else
	{
		spdlog::error("Failed to read node server");
		throw std::runtime_error("Failed to read node server");
	}

	if(const auto node = root_node["security"]; node)
	{
		config.security = load_security_config(node);
	}
	else
	{
		spdlog::error("Failed to read node security");
		throw std::runtime_error("Failed to read node security");
	}

	return config;
}
