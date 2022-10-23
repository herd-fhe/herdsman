#ifndef HERDSMAN_FILE_UTILS_HPP
#define HERDSMAN_FILE_UTILS_HPP

#include <filesystem>


[[nodiscard]] std::string read_file(const std::filesystem::path& filepath);

#endif //HERDSMAN_FILE_UTILS_HPP
