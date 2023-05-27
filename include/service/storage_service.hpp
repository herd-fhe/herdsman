#ifndef HERDSMAN_STORAGE_SERVICE_HPP
#define HERDSMAN_STORAGE_SERVICE_HPP

#include <vector>
#include <map>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <shared_mutex>
#include <condition_variable>

#include "herd/common/model/column_descriptor.hpp"
#include "herd/common/model/data_type.hpp"
#include "herd/common/model/schema_type.hpp"
#include "herd/common/uuid.hpp"


class StorageService
{
public:
	static constexpr std::size_t CHUNK_SIZE = 1024*1024*512; // 0.5 GiB

	struct DataFrameEntry
	{
		herd::common::UUID uuid;
		std::string name;
		herd::common::SchemaType schema_type;
		herd::common::column_map_type columns;

		std::size_t allocated_chunks;
		uint32_t row_count;
		bool uploaded;
		bool busy;
	};

	StorageService(std::filesystem::path storage_dir, std::size_t max_chunk_size);

	herd::common::UUID create_data_frame(const herd::common::UUID& session_uuid, std::string frame_name, herd::common::SchemaType type, herd::common::column_map_type column_map,  uint32_t row_count);
	uint32_t append_to_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, const uint8_t* data, std::size_t size);

	void mark_data_frame_as_uploaded(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);
	void lock_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);

	[[nodiscard]] bool data_frame_exists(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const;
	[[nodiscard]] bool data_frame_busy(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const;

	void remove_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);

	[[nodiscard]] std::vector<StorageService::DataFrameEntry> list_session_data_frames(const herd::common::UUID& session_uuid);
	[[nodiscard]] std::vector<StorageService::DataFrameEntry> list_session_data_frames(const herd::common::UUID& session_uuid, herd::common::SchemaType type);

private:

	mutable std::shared_mutex descriptors_mutex_;

	std::filesystem::path data_frame_storage_dir_;
	std::multimap<herd::common::UUID, DataFrameEntry> data_frames_;

	std::size_t max_chunk_size_;

	void create_directory_for_session(const herd::common::UUID& session_uuid);
	void create_directory_for_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);

	std::ofstream create_chunk_file(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, const std::string& chunk_name);
};

#endif //HERDSMAN_STORAGE_SERVICE_HPP
