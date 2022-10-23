#include "utils/paseto_utils.hpp"

#include <spdlog/spdlog.h>


paseto_key_type init_paseto(const std::string &secret_key)
{
	if(!paseto_init())
	{
		spdlog::error("Failed to initialize libpaseto");
		throw std::runtime_error("Failed to initialize libpaseto");
	}

	paseto_key_type key;
	if(!paseto_v2_local_load_key_base64(key.data(), secret_key.c_str()))
	{
		spdlog::error("Failed to load paseto key");
		throw std::runtime_error("Failed to initialize paseto key");
	}

	return key;
}
