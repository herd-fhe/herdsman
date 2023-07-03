#ifndef HERDSMAN_KEY_SERVICE_HPP
#define HERDSMAN_KEY_SERVICE_HPP

#include <filesystem>
#include <vector>
#include <map>
#include <shared_mutex>

#include "herd/common/uuid.hpp"
#include "herd/common/model/schema_type.hpp"


class KeyService
{
public:
	struct KeyEntry
	{
		herd::common::SchemaType type;
		std::filesystem::path key_path;
		uint32_t locks;
	};

	explicit KeyService(std::filesystem::path  key_dir);

	void add_key(const herd::common::UUID& session_uuid, herd::common::SchemaType type, const std::vector<std::byte>& key_data);
	void remove_key(const herd::common::UUID& session_uuid, herd::common::SchemaType type);

	void lock_key(const herd::common::UUID& session_uuid, herd::common::SchemaType type);
	void unlock_key(const herd::common::UUID& session_uuid, herd::common::SchemaType type);

	[[nodiscard]] std::vector<herd::common::SchemaType> list_available_keys(const herd::common::UUID& session_uuid) const;
	[[nodiscard]] bool schema_key_exists_for_session(const herd::common::UUID& session_uuid, herd::common::SchemaType type) const noexcept;

private:
	mutable std::shared_mutex key_mutex_;

	std::filesystem::path key_storage_dir_;
	std::multimap<herd::common::UUID, KeyEntry> keys_;

	void create_directory_for_session(const herd::common::UUID& uuid);
};

#endif //HERDSMAN_KEY_SERVICE_HPP
