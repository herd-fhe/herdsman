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
#include "herd/common/model/column_meta.hpp"
#include "herd/common/model/data_type.hpp"
#include "herd/common/model/schema_type.hpp"
#include "herd/common/uuid.hpp"


class StorageService
{
public:
	struct DataFrameEntry
	{
		herd::common::UUID uuid;
		std::string name;
		herd::common::SchemaType schema_type;
		herd::common::column_map_type columns;

		uint64_t row_count;
		uint32_t partitions;
		bool uploaded;
		bool busy;
	};

	StorageService(std::filesystem::path storage_dir);

	herd::common::UUID create_data_frame(
			const herd::common::UUID& session_uuid, std::string frame_name,
			herd::common::SchemaType type, const std::vector<herd::common::ColumnMeta>& columns,
			uint64_t row_count, uint32_t partitions);
	uint64_t append_to_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, const uint8_t* data, std::size_t size);

	void mark_data_frame_as_uploaded(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);
	void lock_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);

	[[nodiscard]] bool data_frame_exists(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const;
	[[nodiscard]] bool data_frame_busy(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const;

	void remove_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);

	[[nodiscard]] std::optional<DataFrameEntry> get_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid) const;

	[[nodiscard]] std::vector<DataFrameEntry> list_session_data_frames(const herd::common::UUID& session_uuid);
	[[nodiscard]] std::vector<DataFrameEntry> list_session_data_frames(const herd::common::UUID& session_uuid, herd::common::SchemaType type);

	[[nodiscard]] uint64_t get_partition_size(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, uint32_t partition);

private:
	struct UploadState
	{
			std::size_t current_partition;
			std::size_t rows_stored_in_partition;
	};

	mutable std::shared_mutex descriptors_mutex_;

	std::filesystem::path data_frame_storage_dir_;
	std::multimap<herd::common::UUID, DataFrameEntry> data_frames_;

	std::map<herd::common::UUID, UploadState> upload_state_;

	void create_directory_for_session(const herd::common::UUID& session_uuid);
	void create_directory_for_data_frame(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid);

	std::ofstream open_chunk_file(const herd::common::UUID& session_uuid, const herd::common::UUID& uuid, const std::string& chunk_name);
};

#endif //HERDSMAN_STORAGE_SERVICE_HPP
