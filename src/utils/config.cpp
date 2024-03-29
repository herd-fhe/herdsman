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
		Config::ServerConfig server_config;

		server_config.listen_address.hostname = get_optional_value<std::string>(node, "hostname", "0.0.0.0");
		server_config.listen_address.port = get_optional_value<uint16_t>(node, "port", 5000);

		server_config.key_directory = get_optional_value<std::string>(node, "key_directory", "./");
		server_config.storage_directory = get_optional_value<std::string>(node, "storage_directory", "./");

		return server_config;
	}

	Config::SecurityConfig load_security_config(const YAML::Node& node)
	{
		Config::SecurityConfig security_config;

		security_config.secret_key = get_value<std::string>(node, "secret_key");
		security_config.token_lifetime = get_optional_value<uint64_t>(node, "token_lifetime", 12 * 60 * 60);

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

	Config::LoggingConfig::LogLevel map_log_level(const std::string& log_level_name)
	{
		const std::unordered_map<std::string, Config::LoggingConfig::LogLevel> level_mapping{
				{"INFO", Config::LoggingConfig::LogLevel::INFO},
				{"WARNING", Config::LoggingConfig::LogLevel::WARNING},
				{"ERROR", Config::LoggingConfig::LogLevel::ERROR},
				{"DEBUG", Config::LoggingConfig::LogLevel::DEBUG}
		};

		for(const auto& [name, enum_value]: level_mapping)
		{
			if(log_level_name == name)
			{
				return enum_value;
			}
		}

		spdlog::error("Invalid logging level: {}", log_level_name);
		throw std::runtime_error("Invalid logging level");
	}

	Config::LoggingConfig load_logging_config(const YAML::Node& node)
	{
		Config::LoggingConfig logging_config = {};

		const auto level_string = get_value<std::string>(node, "level");
		logging_config.level = map_log_level(level_string);

		return logging_config;
	}

	Config::LambdaWorkersConfig load_lambda_workers_config(const YAML::Node& node)
	{
		Config::LambdaWorkersConfig config{};

		const auto& address_node = node["address"];
		config.address.hostname = get_value<std::string>(address_node, "hostname");
		config.address.port = get_value<uint16_t>(address_node, "port");

		config.concurrency_limit = get_optional_value<std::size_t>(node, "concurrency_limit", 1);

		return config;
	}

	Config::GrpcWorkersConfig load_grpc_workers_config(const YAML::Node& node)
	{
		Config::GrpcWorkersConfig config{};

		const auto addresses = node["addresses"];
		if(!addresses)
		{
			throw std::runtime_error("No worker address provided");
		}

		for(auto iter = std::cbegin(addresses); iter != std::cend(addresses); ++iter)
		{
			const auto address_node = iter->as<YAML::Node>();
			const auto hostname = get_value<std::string>(address_node, "hostname");
			const auto port = get_value<uint16_t>(address_node, "port");

			config.addresses.emplace_back(hostname, port);
		}

		return config;
	}

	Config::workers_config_t load_workers_config(const YAML::Node& node)
	{
		Config::workers_config_t workers_config;
		if(node.size() == 0)
		{
			throw std::runtime_error("No workers configuration");
		}

		if(node.size() > 1)
		{
			throw std::runtime_error("Multiple workers configuration not supported");
		}

		if(const auto grpc_node = node["grpc"]; grpc_node)
		{
			workers_config = load_grpc_workers_config(grpc_node);
		}
		else if(const auto lambda_node = node["lambda"]; lambda_node)
		{
			workers_config = load_lambda_workers_config(lambda_node);
		}
		else
		{
			throw std::runtime_error("Invalid worker type");
		}

		return workers_config;
	}

	Config::LambdaWorkersConfig load_lambda_workers_config_override()
	{
		Config::LambdaWorkersConfig config{};

		bool hostname_loaded = false;
		bool port_loaded = false;

		if(const auto hostname = std::getenv("LAMBDA_WORKER_HOSTNAME"))
		{
			config.address.hostname = hostname;
			hostname_loaded = true;
		}

		if(const auto port = std::getenv("LAMBDA_WORKER_PORT"))
		{
			config.address.port = static_cast<uint16_t>(std::atoi(port));
			port_loaded = true;
		}

		if(!hostname_loaded || !port_loaded)
		{
			throw std::runtime_error("Incomplete Lamda worker configuration");
		}

		if(const auto concurrency_limit = std::getenv("LAMBDA_CONCURRENCY_LIMIT"))
		{
			config.concurrency_limit = static_cast<size_t>(std::atoi(concurrency_limit));
		}

		return config;
	}

	std::optional<Config::workers_config_t> load_workers_config_override()
	{
		if(const auto worker_type = std::getenv("WORKER_TYPE"))
		{
			if(strcmp(worker_type, "LAMBDA") == 0)
			{
				return load_lambda_workers_config_override();
			}
		}
		return std::nullopt;
	}

	std::string log_level_to_str(Config::LoggingConfig::LogLevel level)
	{
		switch(level)
		{
			using enum Config::LoggingConfig::LogLevel;
			case INFO:
				return "INFO";
			case WARNING:
				return "WARNING";
			case ERROR:
				return "ERROR";
			case DEBUG:
				return "DEBUG";
		}

		return "UNKNOWN";
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

	if(const auto node = root_node["logging"]; node)
	{
		config.logging = load_logging_config(node);
	}
	else
	{
		config.logging = Config::LoggingConfig{
			Config::LoggingConfig::LogLevel::INFO
		};
	}

	if(const auto node = root_node["workers"]; node)
	{
		config.workers = load_workers_config(node);
		const auto override = load_workers_config_override();
		if(override.has_value())
		{
			config.workers = override.value();
		}
	}
	else
	{
		spdlog::error("Failed to read workers config");
		throw std::runtime_error("Failed to read workers config");
	}

	return config;
}

void log_config(const Config& config)
{
	const auto listen_address = config.server.listen_address.hostname + ":" + std::to_string(config.server.listen_address.port);

	std::stringstream config_description;
	config_description << "===== CONFIG =====\n";
	config_description << "\n";
	config_description << "[server_config]\n";

	config_description << "listen_address = \"" << listen_address << "\"\n";
	config_description << "key_directory = \"" << config.server.key_directory << "\"\n";
	config_description << "storage_directory = \"" << config.server.storage_directory << "\"\n";
	config_description << "\n";
	config_description << "[security_config]\n";
	config_description << "token_lifetime = " << config.security.token_lifetime << "\n";

	if(config.security.ssl_config.has_value())
	{
		const auto& ssl_config = config.security.ssl_config.value();
		config_description << "ca_certificate_path = \"" << ssl_config.ca_certificate_path << "\"\n";
		config_description << "certificate_path = \"" << ssl_config.certificate_path << "\"\n";
		config_description << "certificate_key_path = \"" << ssl_config.certificate_key_path << "\"\n";
	}

	config_description << "[logging]\n";
	config_description << "level = " << log_level_to_str(config.logging.level) << "\n";

	if(std::holds_alternative<Config::LambdaWorkersConfig>(config.workers))
	{
		const auto& lambda_config = std::get<Config::LambdaWorkersConfig>(config.workers);
		const auto lambda_address = lambda_config.address.hostname + ":" + std::to_string(lambda_config.address.port);

		config_description << "[workers - lambda]\n";
		config_description << "address = \"" << lambda_address << "\"\n";
		config_description << "concurrency_limit = " << lambda_config.concurrency_limit << "\n";

	}
	else if(std::holds_alternative<Config::GrpcWorkersConfig>(config.workers))
	{
		const auto& grpc_config = std::get<Config::GrpcWorkersConfig>(config.workers);

		config_description << "[workers - grpc]\n";
		config_description << "address = [";
		for(std::size_t i = 0; i < grpc_config.addresses.size(); ++i)
		{
			config_description << "\t\"" <<grpc_config.addresses[i].hostname << ":" << grpc_config.addresses[i].port << "\"";
			if(i != grpc_config.addresses.size() - 1)
			{
				config_description << ",";
			}
			config_description << "\n";
		}
		config_description << "]";
	}

	spdlog::debug(config_description.str());
}
