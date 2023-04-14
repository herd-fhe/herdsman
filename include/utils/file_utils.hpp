#ifndef HERDSMAN_FILE_UTILS_HPP
#define HERDSMAN_FILE_UTILS_HPP

#include <filesystem>
#include <vector>

struct DirectoryNotExistError: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};

struct FileNotExistError: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};

struct FileReadError: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};

struct FileWriteError: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};


[[nodiscard]] std::string read_file(const std::filesystem::path& filepath);
void write_file(const std::filesystem::path& filepath, const std::vector<std::byte>& data);
void append_to_file(const std::filesystem::path& filepath, const std::vector<std::byte>& data);
void append_to_file(const std::filesystem::path& filepath, const uint8_t* data, std::size_t size);

void append_to_file(std::ofstream& out_file, const std::vector<std::byte>& val);
void append_to_file(std::ofstream& out_file, const uint8_t* data, std::size_t size);

bool file_exist(const std::filesystem::path& file_path);
bool directory_exist(const std::filesystem::path& dir_path);
std::size_t get_file_size(const std::filesystem::path& file_path);

#endif //HERDSMAN_FILE_UTILS_HPP
