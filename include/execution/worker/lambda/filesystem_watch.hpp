#ifndef HERDSMAN_FILESYSTEM_WATCH_HPP
#define HERDSMAN_FILESYSTEM_WATCH_HPP

#include <unordered_map>
#include <filesystem>
#include <vector>


class FilesystemWatch
{
public:

	void watch_for(const std::filesystem::path& file_to_watch);
	void unwatch(const std::filesystem::path& file_to_unwatch);

	std::vector<std::filesystem::path> detect_changes();

private:
	std::unordered_multimap<std::filesystem::path, std::filesystem::path> watched_files_by_directories_;
	std::unordered_map<std::filesystem::path, std::filesystem::file_time_type> directory_modify_time_;
};

#endif //HERDSMAN_FILESYSTEM_WATCH_HPP
