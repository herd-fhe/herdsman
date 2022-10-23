#ifndef HERDSMAN_CONTROLLER_UTILS_HPP
#define HERDSMAN_CONTROLLER_UTILS_HPP

#include <cstdint>

#include <grpcpp/grpcpp.h>


constexpr const char* const INTERNAL_ERROR_MSG = "Internal server error";

[[nodiscard]] uint64_t extract_user_id_from_context(const grpc::ServerContext* context) noexcept;

#endif //HERDSMAN_CONTROLLER_UTILS_HPP
