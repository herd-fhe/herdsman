#ifndef HERDSMAN_STORAGE_SERVICE_HPP
#define HERDSMAN_STORAGE_SERVICE_HPP

#include <vector>
#include <map>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <condition_variable>

#include "herd_common/column_descriptor.hpp"
#include "herd_common/data_type.hpp"
#include "herd_common/schema_type.hpp"
#include "utils/uuid.hpp"


class StorageService
{
public:
	static constexpr std::size_t CHUNK_SIZE = 1024*1024*512; // 0.5 GiB

	struct DataFrameEntry
	{
		UUID uuid;
		std::string name;
		herd::common::SchemaType schema_type;
		herd::common::column_map_type columns;

		std::size_t allocated_chunks;
		uint32_t row_count;
		bool uploaded;
		bool busy;
	};

	StorageService(std::filesystem::path storage_dir, std::size_t max_chunk_size);

	UUID create_data_frame(const UUID& session_uuid, std::string frame_name, herd::common::SchemaType type, herd::common::column_map_type column_map,  uint32_t row_count);
	uint32_t append_to_data_frame(const UUID& session_uuid, const UUID& uuid, const uint8_t* data, std::size_t size);
	void mark_data_frame_as_uploaded(const UUID& session_uuid, const UUID& uuid);

	[[nodiscard]] std::vector<std::byte> get_data_from_data_frame(const UUID& session_uuid, const UUID& uuid, std::size_t offset, std::size_t len);

	[[nodiscard]] bool data_frame_exists(const UUID& session_uuid, const UUID& uuid) const;
	[[nodiscard]] bool data_frame_busy(const UUID& session_uuid, const UUID& uuid) const;

	void remove_data_frame(const UUID& session_uuid, const UUID& uuid);

	[[nodiscard]] std::vector<StorageService::DataFrameEntry> list_session_data_frames(const UUID& session_uuid);
	[[nodiscard]] std::vector<StorageService::DataFrameEntry> list_session_data_frames(const UUID& session_uuid, herd::common::SchemaType type);

private:

	mutable std::mutex descriptors_mutex_;

	std::filesystem::path data_frame_storage_dir_;
	std::multimap<UUID, DataFrameEntry> data_frames_;

	std::size_t max_chunk_size_;

	void create_directory_for_session(const UUID& session_uuid);
	void create_directory_for_data_frame(const UUID& session_uuid, const UUID& uuid);

	std::ofstream create_chunk_file(const UUID& session_uuid, const UUID& uuid, const std::string& chunk_name);
};

#endif //HERDSMAN_STORAGE_SERVICE_HPP
