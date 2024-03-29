#include <string>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

#include "utils/config.hpp"
#include "utils/paseto_utils.hpp"
#include "utils/file_utils.hpp"

#include "plugins/token_auth_metadata_processor.hpp"

#include "execution/executor/executor.hpp"

#include "service/auth_service.hpp"
#include "service/session_service.hpp"
#include "service/storage_service.hpp"
#include "service/execution_service.hpp"

#include "controller/auth_controller.hpp"
#include "controller/execution_controller.hpp"
#include "controller/session_controller.hpp"
#include "controller/storage_controller.hpp"

#include "execution/worker/grpc/grpc_worker_group.hpp"
#include "execution/worker/lambda/lambda_http_worker_group.hpp"


std::shared_ptr<grpc::ServerCredentials> build_server_credentials(
		const Config::SecurityConfig& config,
		AuthService& auth_service
)
{
	std::vector<std::string> path_not_secured = {std::string("/") + herd::proto::Auth::service_full_name() + "/authorize_connection"};
	const auto auth_metadata_processor = std::make_shared<TokenAuthMetadataProcessor>(auth_service, path_not_secured);

	if(config.ssl_config)
	{
		spdlog::info("Running in SSL mode");
		grpc::SslServerCredentialsOptions options(
				GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE
		);

		auto ca_cert = read_file(config.ssl_config->ca_certificate_path);
		auto cert = read_file(config.ssl_config->certificate_path);
		auto key = read_file(config.ssl_config->certificate_key_path);

		grpc::SslServerCredentialsOptions::PemKeyCertPair key_cert = {key, cert};
		options.pem_root_certs = ca_cert;
		options.pem_key_cert_pairs.emplace_back(std::move(key_cert));

		auto credentials = grpc::SslServerCredentials(options);
		credentials->SetAuthMetadataProcessor(auth_metadata_processor);

		return credentials;
	}
	else
	{
		spdlog::info("Running in LOCAL mode");
		auto credentials = grpc::experimental::LocalServerCredentials(grpc_local_connect_type::LOCAL_TCP);
		credentials->SetAuthMetadataProcessor(auth_metadata_processor);

		return credentials;
	}
}

std::unique_ptr<IWorkerGroup> build_worker_group(const Config& config)
{
	const auto& workers_config = config.workers;
	if(std::holds_alternative<Config::GrpcWorkersConfig>(workers_config))
	{
		const auto& grpc_workers_config = std::get<Config::GrpcWorkersConfig>(workers_config);
		return std::make_unique<GrpcWorkerGroup>(grpc_workers_config.addresses);
	}
	else if(std::holds_alternative<Config::LambdaWorkersConfig>(workers_config))
	{
		const auto& lambda_worker_config = std::get<Config::LambdaWorkersConfig>(workers_config);
		return std::make_unique<LambdaWorkerGroup>(lambda_worker_config.address , lambda_worker_config.concurrency_limit, config.server.storage_directory);
	}

	assert(false && "Invalid configuration");

	return nullptr;
}

void init_global_logger(const Config::LoggingConfig& config)
{
	using enum Config::LoggingConfig::LogLevel;

	const std::unordered_map<Config::LoggingConfig::LogLevel, spdlog::level::level_enum> spdlog_log_level_map{
		{INFO, spdlog::level::level_enum::info},
		{WARNING, spdlog::level::level_enum::warn},
		{ERROR, spdlog::level::level_enum::err},
		{DEBUG, spdlog::level::level_enum::debug}
	};

	const auto spdlog_level = spdlog_log_level_map.at(config.level);
	spdlog::set_level(spdlog_level);
	spdlog::info("Logger set up to: {} level", spdlog::level::to_short_c_str(spdlog_level));
}

int main()
{
	const auto config = load_config("./herdsman.yaml");
	init_global_logger(config.logging);

	if(spdlog::should_log(spdlog::level::debug))
	{
		log_config(config);
	}

	const paseto_key_type paseto_key = init_paseto(config.security.secret_key);
	const std::string address = config.server.listen_address.hostname + ":" + std::to_string(config.server.listen_address.port);

	AuthService auth_service(paseto_key, std::chrono::seconds(config.security.token_lifetime));
	SessionService session_service;
	KeyService key_service(config.server.key_directory);
	StorageService storage_service(config.server.storage_directory);
	ExecutionService execution_service(key_service, storage_service);


	const auto executor = std::make_shared<executor::Executor>(execution_service);
	{
			auto worker_group = build_worker_group(config);
			executor->set_worker_group(std::move(worker_group));
	}

	execution_service.set_executor(executor);

	const auto credentials = build_server_credentials(config.security, auth_service);

	grpc::ServerBuilder builder;
	builder.AddListeningPort(address, credentials);

	builder.SetMaxSendMessageSize(32 * 1024 * 1024);
	builder.SetMaxReceiveMessageSize(32 * 1024 * 1024);

	AuthController auth_controller(auth_service);
	builder.RegisterService(&auth_controller);
	spdlog::debug("Auth controller created");

	SessionController session_controller(session_service, key_service);
	builder.RegisterService(&session_controller);
	spdlog::debug("Session controller created");

	StorageController storage_controller(storage_service, session_service, key_service);
	builder.RegisterService(&storage_controller);
	spdlog::debug("Storage controller created");

	ExecutionController execution_controller(execution_service, session_service);
	builder.RegisterService(&execution_controller);
	spdlog::debug("Execution controller created");

	auto server = builder.BuildAndStart();
	spdlog::info("Server listening on hostname: {}", address);
	server->Wait();

	return 0;
}