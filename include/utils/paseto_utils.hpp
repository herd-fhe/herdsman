#ifndef HERDSMAN_PASETO_UTILS_HPP
#define HERDSMAN_PASETO_UTILS_HPP

#include <array>
#include <string>
#include <cstdint>

#include <paseto.h>


using paseto_key_type = std::array<uint8_t, paseto_v2_LOCAL_KEYBYTES>;

[[nodiscard]] paseto_key_type init_paseto(const std::string& secret_key);


#endif //HERDSMAN_PASETO_UTILS_HPP
