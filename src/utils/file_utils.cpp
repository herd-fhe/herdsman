#include "utils/file_utils.hpp"

#include <fstream>


std::string read_file(const std::filesystem::path &filepath)
{
	if(!std::filesystem::exists(filepath))
	{
		throw std::runtime_error("File " + filepath.string() + " not found");
	}

	if(!std::filesystem::is_regular_file(filepath))
	{
		throw std::runtime_error(filepath.string() + "is not a regular file");
	}

	const auto file_size = std::filesystem::file_size(filepath);
	std::ifstream file(filepath, std::ios::binary);
	file.exceptions(std::ios::badbit | std::ios::failbit);

	std::string string_data;
	string_data.reserve(file_size);

	try
	{
		string_data.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
	}
	catch(const std::ios_base::failure& fail)
	{
		throw std::runtime_error("Failed to read data from file");
	}

	return string_data;
}
