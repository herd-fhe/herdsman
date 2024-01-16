#include "execution/worker/lambda/filesystem_watch.hpp"


void FilesystemWatch::watch_for(const std::filesystem::path& file_to_watch)
{
	const auto base_directory = file_to_watch.parent_path();

	watched_files_by_directories_.emplace(base_directory, file_to_watch);

	directory_modify_time_.try_emplace(file_to_watch, std::filesystem::file_time_type());
}

void FilesystemWatch::unwatch(const std::filesystem::path& file_to_unwatch)
{
	const auto base_directory = file_to_unwatch.parent_path();

	auto [iter, end] = watched_files_by_directories_.equal_range(base_directory);
	for(;iter != end; ++iter)
	{
		if(iter->second == file_to_unwatch)
		{
			watched_files_by_directories_.erase(iter);
			break;
		}
	}

	if(!watched_files_by_directories_.contains(base_directory))
	{
		directory_modify_time_.erase(base_directory);
	}
}

std::vector<std::filesystem::path> FilesystemWatch::detect_changes()
{
	std::vector<std::filesystem::path> new_files;

	for(auto dir_iter = std::begin(directory_modify_time_); dir_iter != std::end(directory_modify_time_); ++dir_iter)
	{
		const auto& dir = dir_iter->first;

		const auto new_modify_time = std::filesystem::last_write_time(dir);
		if(new_modify_time != dir_iter->second)
		{
			dir_iter->second = new_modify_time;
			auto [iter, end] = watched_files_by_directories_.equal_range(dir);
			for(;iter != end; ++iter)
			{
				if(std::filesystem::exists(iter->second))
				{
					new_files.emplace_back(iter->second);

					iter = watched_files_by_directories_.erase(iter);
				}
			}

			if (!watched_files_by_directories_.contains(dir))
			{
				dir_iter = directory_modify_time_.erase(dir_iter);
			}
		}
	}

	return new_files;
}