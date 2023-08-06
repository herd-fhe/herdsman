#include "utils/file_utils.hpp"

#include <fstream>


namespace fs = std::filesystem;

std::string read_file(const std::filesystem::path &filepath)
{
	if(!std::filesystem::exists(filepath))
	{
		throw FileReadError("File " + filepath.string() + " not found");
	}

	if(!std::filesystem::is_regular_file(filepath))
	{
		throw FileReadError(filepath.string() + "is not a regular file");
	}

	const auto file_size = std::filesystem::file_size(filepath);
	std::ifstream file(filepath, std::ios::binary);
	file.exceptions(std::ios::badbit | std::ios::failbit);

	std::string string_data;
	string_data.reserve(file_size);

	try
	{
		const auto start_iter = std::istreambuf_iterator<char>(file);
		const auto end_iter = std::istreambuf_iterator<char>();
		string_data.assign(start_iter, end_iter);
	}
	catch(const std::ios_base::failure&)
	{
		throw FileReadError("Failed to read data from file");
	}

	return string_data;
}

void write_file(const std::filesystem::path& filepath, const std::vector<std::byte>& data)
{
	std::ofstream file(filepath, std::ios_base::trunc | std::ios_base::binary);

	append_to_file(file, data);

	file.close();
}

void append_to_file(const std::filesystem::path& filepath, const std::vector<std::byte>& data)
{
	append_to_file(filepath, reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

void append_to_file(const std::filesystem::path& filepath, const uint8_t* data, std::size_t size)
{
	std::ofstream file(filepath, std::ios_base::app | std::ios_base::binary);

	append_to_file(file, data, size);
}


void append_to_file(std::ofstream& out_file, const std::vector<std::byte>& data)
{
	append_to_file(out_file, reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

void append_to_file(std::ofstream& out_file, const uint8_t* data, std::size_t size)
{
	out_file.write(reinterpret_cast<const char*>(data), static_cast<long>(size));
	if(!out_file)
	{
		out_file.close();
		throw FileWriteError("Failed to append to file");
	}
}

bool file_exist(const std::filesystem::path& file_path)
{
	const auto file_status = fs::status(file_path);
	return fs::is_regular_file(file_status);
}

bool directory_exist(const std::filesystem::path& dir_path)
{
	const auto directory_status = fs::status(dir_path);
	return fs::is_directory(directory_status);
}

std::size_t get_file_size(const std::filesystem::path& file_path)
{
	try
	{
		return fs::file_size(file_path);
	}
	catch(const fs::filesystem_error& error)
	{
		std::throw_with_nested(FileNotExistError("Failed to get size of file"));
	}
}
