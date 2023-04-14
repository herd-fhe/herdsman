#include "service/storage_service.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <utility>
#include <span>
#include <memory>

#include "herd_common/schema_type.hpp"

#include "service/common_exceptions.hpp"
#include "utils/file_utils.hpp"


namespace fs = std::filesystem;

namespace
{
	uint32_t next_row_size(std::span<const std::byte> bytes)
	{
		uint32_t size = 0;
		std::copy_n(std::begin(bytes), sizeof(size), reinterpret_cast<std::byte*>(&size));

		//space for size
		size += sizeof(size);

		return size;
	}
}

StorageService::StorageService(std::filesystem::path storage_dir, std::size_t max_chunk_size)
	: data_frame_storage_dir_(std::move(storage_dir)), max_chunk_size_(max_chunk_size)
{
}

std::vector<StorageService::DataFrameEntry> StorageService::list_session_data_frames(const UUID& session_uuid)
{
	const auto [frames_begin, frames_end] = data_frames_.equal_range(session_uuid);

	std::vector<DataFrameEntry> result;
	result.reserve(static_cast<unsigned long>(std::distance(frames_begin, frames_end)));

	std::transform(
		frames_begin, frames_end,
		std::back_inserter(result),
		[](const auto& entry)
		{
			return entry.second;
		}
	);

	return result;
}

std::vector<StorageService::DataFrameEntry> StorageService::list_session_data_frames(const UUID& session_uuid, herd::common::SchemaType type)
{
	std::vector<DataFrameEntry> result = list_session_data_frames(session_uuid);

	std::erase_if(
		result,
        [type](const auto& entry)
		{
			return entry.schema_type != type;
		}
	);

	return result;
}

UUID StorageService::create_data_frame(const UUID& session_uuid, std::string frame_name, herd::common::SchemaType type, herd::common::column_map_type column_map)
{
	std::unique_lock lock(descriptors_mutex_);

	DataFrameEntry entry = {};
	entry.name = std::move(frame_name);
	entry.columns = std::move(column_map);
	entry.schema_type = type;
	entry.uuid = UUID();
	entry.uploaded = false;
	entry.busy = true;

	create_directory_for_data_frame(session_uuid, entry.uuid);

	data_frames_.insert(std::make_pair(session_uuid, entry));

	return entry.uuid;
}

uint32_t StorageService::append_to_data_frame(const UUID& session_uuid, const UUID& uuid, const uint8_t* data, std::size_t size)
{
	static_cast<void>(session_uuid);
	static_cast<void>(uuid);
	static_cast<void>(data);

	auto [iter_begin, iter_end] = data_frames_.equal_range(session_uuid);
	const auto data_frame_iter = std::find_if(
			iter_begin, iter_end,
			[&uuid](const auto& entry)
			{
				return entry.second.uuid == uuid;
			}
    );

	assert(data_frame_iter != iter_end);

	auto& data_frame_entry = data_frame_iter->second;

	auto byte_iter = reinterpret_cast<const std::byte*>(data);
	const auto byte_end_iter = std::next(byte_iter, static_cast<std::iter_difference_t<const uint8_t*>>(size));

	uint32_t stored_rows = 0;
	bool last_chunk_full = false;

	while(byte_iter != byte_end_iter)
	{
		std::basic_ofstream<char> output;
		std::size_t available_chunk_space = 0;

		if(data_frame_entry.allocated_chunks == 0 || last_chunk_full)
		{
			output = create_chunk_file(session_uuid, uuid, std::to_string(data_frame_entry.allocated_chunks));
			available_chunk_space = max_chunk_size_;
			data_frame_entry.allocated_chunks += 1;
		}
		else
		{
			const auto current_chunk_name = std::to_string(data_frame_entry.allocated_chunks - 1);
			const auto current_chunk_path = data_frame_storage_dir_ / session_uuid.as_string() / uuid.as_string() / current_chunk_name;

			if(!file_exist(current_chunk_path))
			{
				spdlog::error("Chunk part not found for session: {}, data_frame: {}, chunk: {}", session_uuid.as_string(), uuid.as_string(), current_chunk_name);
				throw ObjectNotFoundException("Chunk file not found");
			}

			output = std::ofstream(current_chunk_path, std::ios_base::app | std::ios_base::binary);
			const auto current_chunk_size = get_file_size(current_chunk_path);
			available_chunk_space = max_chunk_size_ > current_chunk_size ? max_chunk_size_ - current_chunk_size : 0;
		}

		std::vector<std::byte> upload_buffer;

		while(byte_iter != byte_end_iter)
		{
			const auto row_size = next_row_size(std::span(byte_iter, byte_end_iter));
			if (row_size > available_chunk_space)
			{
				last_chunk_full = true;
				break;
			}
			std::copy_n(byte_iter, row_size, std::back_inserter(upload_buffer));

			std::advance(byte_iter, row_size);

			++stored_rows;
		}

		if (!upload_buffer.empty())
		{
			append_to_file(output, upload_buffer);
		}
	}

	return stored_rows;
}

bool StorageService::data_frame_exists(const UUID& session_uuid, const UUID& uuid) const
{
	std::unique_lock lock(descriptors_mutex_);

	auto [iter_begin, iter_end] = data_frames_.equal_range(session_uuid);
	return std::any_of(
			iter_begin, iter_end,
			[&uuid](const auto& entry)
			{
				return entry.second.uuid == uuid;
			}
	);
}

void StorageService::create_directory_for_session(const UUID& session_uuid)
{
	const auto session_dir_path = data_frame_storage_dir_ / session_uuid.as_string();

	if(directory_exist(session_dir_path))
	{
		spdlog::info("Clearing old data frame directory for session {}", session_uuid.as_string());
		std::filesystem::remove_all(session_dir_path);
	}
	else
	{
		fs::create_directory(session_dir_path);
		fs::permissions(session_dir_path, fs::perms::owner_all, fs::perm_options::replace);

		spdlog::info("Created new data frame directory for session {}", session_uuid.as_string());
	}
}

void StorageService::create_directory_for_data_frame(const UUID& session_uuid, const UUID& uuid)
{
	const auto session_dir_path = data_frame_storage_dir_ / session_uuid.as_string();

	if(!directory_exist(session_dir_path))
	{
		spdlog::info("Data frame session directory does not exist for session {}.", session_uuid.as_string());
		create_directory_for_session(session_uuid);
	}

	const auto data_frame_directory_path = session_dir_path / uuid.as_string();

	if(directory_exist(data_frame_directory_path))
	{
		spdlog::info("Data frame session({}) directory exists", session_uuid.as_string());
		std::filesystem::remove_all(data_frame_directory_path);
	}
	else
	{
		spdlog::info("Creating Data frame directory for session({}), data frame({})", session_uuid.as_string(), uuid.as_string());
		fs::create_directory(data_frame_directory_path);
	}
}

std::ofstream StorageService::create_chunk_file(const UUID& session_uuid, const UUID& uuid, const std::string& chunk_name)
{
	assert(!chunk_name.empty());

	const auto data_frame_dir_path = data_frame_storage_dir_ / session_uuid.as_string() / uuid.as_string();
	if(!directory_exist(data_frame_dir_path))
	{
		spdlog::error("Data frame directory does not exist for session {}, data frame {}.", session_uuid.as_string(), uuid.as_string());
		throw ObjectNotFoundException("Data frame directory not found");
	}

	const auto data_frame_chunk_path = data_frame_dir_path / chunk_name;
	if(file_exist(data_frame_chunk_path))
	{
		spdlog::error("Chunk already exist. Session: {} data_frame: {} chunk_name: {}", session_uuid.as_string(), uuid.as_string(), chunk_name);
		throw ObjectAlreadyExistsException("Chunk already exist");
	}

	spdlog::info("Created new chunk: {} for session: {}, data frame: {}", chunk_name, session_uuid.as_string(), uuid.as_string());
	return {data_frame_chunk_path, std::ios_base::binary};
}

void StorageService::mark_data_frame_as_uploaded(const UUID& session_uuid, const UUID& uuid)
{
	auto [iter_begin, iter_end] = data_frames_.equal_range(session_uuid);
	const auto data_frame_iter = std::find_if(
			iter_begin, iter_end,
			[&uuid](const auto& entry)
			{
				return entry.second.uuid == uuid;
			}
	);

	assert(data_frame_iter != iter_end);

	auto& data_frame_entry = data_frame_iter->second;

	data_frame_entry.uploaded = true;
}

void StorageService::remove_data_frame(const UUID& session_uuid, const UUID& uuid)
{
	std::unique_lock lock(descriptors_mutex_);

	auto [iter_begin, iter_end] = data_frames_.equal_range(session_uuid);
	const auto data_frame_iter = std::find_if(
			iter_begin, iter_end,
			[&uuid](const auto& entry)
			{
				return entry.second.uuid == uuid;
			}
	);

	assert(data_frame_iter != iter_end);

	const auto chunks_path = data_frame_storage_dir_ / session_uuid.as_string();

	spdlog::info("Deleting data frame {} session({}) directory exists", data_frame_iter->first.as_string(), session_uuid.as_string());
	std::filesystem::remove_all(chunks_path);

	data_frames_.erase(data_frame_iter);
}
