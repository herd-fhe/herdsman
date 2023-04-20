#ifndef HERDSMAN_KEY_SERVICE_HPP
#define HERDSMAN_KEY_SERVICE_HPP

#include <filesystem>
#include <vector>
#include <map>

#include "herd_common/schema_type.hpp"
#include "utils/uuid.hpp"


class KeyService
{
public:
	struct KeyEntry {
		herd::common::SchemaType type;
		std::filesystem::path key_path;
	};

	void add_key(const UUID& session_uuid, herd::common::SchemaType type, const std::vector<std::byte>& key_data);
	void remove_key(const UUID& session_uuid, herd::common::SchemaType type);

	[[nodiscard]] std::vector<herd::common::SchemaType> list_available_keys(const UUID& session_uuid) const;
	[[nodiscard]] bool schema_key_exists_for_session(const UUID& session_uuid, herd::common::SchemaType type) const noexcept;

private:
	std::filesystem::path key_storage_dir_;
	std::multimap<UUID, KeyEntry> keys_;

	void create_directory_for_session(const UUID& uuid);
};

#endif //HERDSMAN_KEY_SERVICE_HPP
