#include "service/key_service.hpp"

#include "spdlog/spdlog.h"

#include "herd_common/schema_type.hpp"

#include "service/common_exceptions.hpp"
#include "utils/file_utils.hpp"


namespace fs = std::filesystem;


void KeyService::add_key(const UUID& session_uuid, herd::common::SchemaType type, const std::vector<std::byte>& key_data)
{
	if(!keys_.contains(session_uuid))
	{
		create_directory_for_session(session_uuid);
	}

	const auto session_dir_path = key_storage_dir_ / session_uuid.as_string();
	const auto type_name = std::to_string(static_cast<std::underlying_type_t<herd::common::SchemaType>>(type)) + ".key";

	const auto key_path = session_dir_path / type_name;

	if(const auto key_file_status = fs::status(key_path); fs::is_regular_file(key_file_status))
	{
		spdlog::info("Found old schema {} key file for schema {}. Old file will be overwritten.", type_name, session_uuid.as_string());
	}

	write_file(key_path, key_data);

	keys_.emplace(
			session_uuid,
			KeyEntry{type, key_path}
    );
}

void KeyService::create_directory_for_session(const UUID& uuid)
{
	const auto session_dir_path = key_storage_dir_ / uuid.as_string();
	const auto session_dir_status = fs::status(session_dir_path);

	if(fs::is_directory(session_dir_status))
	{
		spdlog::info("Clearing old directory for session {}", uuid.as_string());
		std::filesystem::remove_all(session_dir_path);
	}
	else
	{
		fs::create_directory(session_dir_path);
		fs::permissions(session_dir_path, fs::perms::owner_all, fs::perm_options::replace);

		spdlog::info("Created new directory for session {}", uuid.as_string());
	}
}

void KeyService::remove_key(const UUID& session_uuid, herd::common::SchemaType type)
{
	const auto [keys_begin, keys_end] = keys_.equal_range(session_uuid);
	const auto key_with_type_predicate = [type](const auto& key_entry)
	{
		return key_entry.second.type == type;
	};

	if(const auto key_iter = std::find_if(keys_begin, keys_end,  key_with_type_predicate); key_iter != keys_end)
	{
		fs::remove(key_iter->second.key_path);
	}
	else
	{
		throw ObjectNotFoundException("No key of selected type assigned to session");
	}
}

std::vector<herd::common::SchemaType> KeyService::list_available_keys(const UUID& session_uuid) const
{
	std::vector<herd::common::SchemaType> types;
	const auto [keys_begin, keys_end] = keys_.equal_range(session_uuid);

	std::transform(keys_begin, keys_end, std::back_inserter(types),
				   [](const auto& entry)
				   {
					   return entry.second.type;
				   }
   );

	return types;
}

bool KeyService::schema_key_exists_for_session(const UUID& session_uuid, herd::common::SchemaType type) const noexcept
{
	const auto [keys_begin, keys_end] = keys_.equal_range(session_uuid);
	return std::any_of(keys_begin, keys_end,
		[type](const auto& entry)
		{
			return entry.second.type == type;
		}
	);
}
