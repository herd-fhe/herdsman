#ifndef HERDSMAN_KEY_SERVICE_HPP
#define HERDSMAN_KEY_SERVICE_HPP

#include <filesystem>
#include <vector>
#include <map>

#include "schema_type.hpp"
#include "utils/uuid.hpp"


class KeyService
{
public:
	struct KeyEntry {
		SchemaType type;
		std::filesystem::path key_path;
	};

	void add_key(const UUID& session_uuid, SchemaType type, const std::vector<std::byte>& key_data);
	void remove_key(const UUID& session_uuid, SchemaType type);

	[[nodiscard]] std::vector<SchemaType> list_available_keys(const UUID& session_uuid) const;

private:
	std::filesystem::path key_storage_dir_;
	std::multimap<UUID, KeyEntry> keys_;

	void create_directory_for_session(const UUID& uuid) const;
};

#endif //HERDSMAN_KEY_SERVICE_HPP
