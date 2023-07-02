#include "service/storage_service.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <utility>
#include <span>

#include "herd/common/model/schema_type.hpp"

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

StorageService::StorageService(std::filesystem::path storage_dir)
	: data_frame_storage_dir_(std::move(storage_dir))
{
}

std::vector<StorageService::DataFrameEntry> StorageService::list_session_data_frames(const herd::common::UUID& session_uuid)
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

std::vector<StorageService::DataFrameEntry> StorageService::list_session_data_frames(const herd::common::UUID& session_uuid, herd::common::SchemaType type)
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

herd::common::UUID StorageService::create_data_frame(
		const herd::common::UUID& session_uuid, std::string frame_name,
		herd::common::SchemaType type, const std::vector<herd::common::ColumnMeta>& columns,
		uint64_t row_count, uint32_t partitions)
{
	std::unique_lock lock(descriptors_mutex_);

	DataFrameEntry entry = {};
	entry.name = std::move(frame_name);
	entry.schema_type = type;
	entry.uuid = herd::common::UUID();
	entry.row_count = row_count;
	entry.partitions = partitions;
	entry.uploaded = false;
	entry.busy = true;

	for(uint8_t index = 0; const auto& column: columns)
	{
		entry.columns.try_emplace(column.name, herd::common::ColumnDescriptor{index, column.type});
	}

	create_directory_for_data_frame(session_uuid, entry.uuid);

	data_frames_.insert(std::make_pair(session_uuid, entry));
	upload_state_.insert(std::make_pair(entry.uuid, UploadState{0, 0}));

	return entry.uuid;
}

uint64_t StorageService::append_to_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, const uint8_t* data, std::size_t size)
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

	if(data_frame_iter == iter_end)
	{
		throw ObjectNotFoundException("No data frame found");
	}

	const auto& data_frame_entry = data_frame_iter->second;
	auto& upload_state = upload_state_[data_frame_entry.uuid];
	uint64_t rows_read = 0;

	const uint64_t chunk_row_count = data_frame_entry.row_count / data_frame_entry.partitions;
	const uint64_t remainder_count = data_frame_entry.row_count % data_frame_entry.partitions;


	auto byte_iter = reinterpret_cast<const std::byte*>(data);
	const auto byte_end_iter = std::next(byte_iter, static_cast<std::iter_difference_t<const uint8_t*>>(size));
	while(byte_iter != byte_end_iter)
	{
		auto output = open_chunk_file(session_uuid, uuid, std::to_string(upload_state.current_partition));
		std::size_t max_rows_in_partition = chunk_row_count + (upload_state.current_partition < remainder_count ? 1 : 0);

		while(byte_iter != byte_end_iter)
		{
			if(upload_state.rows_stored_in_partition == max_rows_in_partition)
			{
				upload_state.current_partition += 1;
				upload_state.rows_stored_in_partition = 0;
				break;
			}

			const auto row_size = next_row_size(std::span(byte_iter, byte_end_iter));
			append_to_file(output, reinterpret_cast<const uint8_t*>(byte_iter), row_size);
			std::advance(byte_iter, row_size);

			++upload_state.rows_stored_in_partition;
			++rows_read;
		}
	}

	return rows_read;
}

bool StorageService::data_frame_exists(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const
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

bool StorageService::data_frame_busy(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const
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

	if(data_frame_iter == iter_end)
	{
		throw ObjectNotFoundException("No data frame found");
	}

	return data_frame_iter->second.busy;
}

void StorageService::create_directory_for_session(const herd::common::UUID& session_uuid)
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

void StorageService::create_directory_for_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid)
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

std::ofstream StorageService::open_chunk_file(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, const std::string& chunk_name)
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
		spdlog::info("Chunk already exist. Session: {} data_frame: {} chunk_name: {}", session_uuid.as_string(), uuid.as_string(), chunk_name);
	}
	else
	{
		spdlog::info("Created new chunk: {} for session: {}, data frame: {}", chunk_name, session_uuid.as_string(), uuid.as_string());
	}

	return {data_frame_chunk_path, std::ios_base::binary | std::ios_base::ate};
}

void StorageService::mark_data_frame_as_uploaded(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid)
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

	if(data_frame_iter == iter_end)
	{
		throw ObjectNotFoundException("No data frame found");
	}

	auto& data_frame_entry = data_frame_iter->second;

	data_frame_entry.uploaded = true;
}

void StorageService::lock_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid)
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

	if(data_frame_iter == iter_end)
	{
		throw ObjectNotFoundException("No data frame found");
	}

	data_frame_iter->second.busy = true;
}


void StorageService::remove_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid)
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

uint64_t StorageService::get_partition_size(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, uint32_t partition)
{
	const auto data_frame_opt = get_data_frame(session_uuid, uuid);
	assert(data_frame_opt.has_value());

	const auto& data_frame = data_frame_opt.value();

	const uint64_t chunk_row_count = data_frame.row_count / data_frame.partitions;
	const uint64_t remainder_count = data_frame.row_count % data_frame.partitions;

	return chunk_row_count + (partition < remainder_count ? 1 : 0);
}

std::optional<StorageService::DataFrameEntry> StorageService::get_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const
{
	std::shared_lock lock(descriptors_mutex_);

	auto [iter_begin, iter_end] = data_frames_.equal_range(session_uuid);
	auto data_frame_iter = std::find_if(
			iter_begin, iter_end,
			[&uuid](const auto& entry)
			{
				return entry.second.uuid == uuid;
			}
	);

	if(data_frame_iter != iter_end)
	{
		return data_frame_iter->second;
	}
	else
	{
		return std::nullopt;
	}
}
