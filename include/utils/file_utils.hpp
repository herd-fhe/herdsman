#ifndef HERDSMAN_FILE_UTILS_HPP
#define HERDSMAN_FILE_UTILS_HPP

#include <filesystem>
#include <vector>


struct FileReadError: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};

struct FileWriteError: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};


[[nodiscard]] std::string read_file(const std::filesystem::path& filepath);
void write_file(const std::filesystem::path& filepath, const std::vector<std::byte>& val);

#endif //HERDSMAN_FILE_UTILS_HPP
