#include "service/session_service.hpp"

#include <algorithm>
#include <cassert>

#include "service/common_exceptions.hpp"


bool SessionService::session_exists_by_name(uint64_t user_id, const std::string &session_name) const noexcept
{
	const auto [user_sessions_begin, user_sessions_end] = sessions_.equal_range(user_id);

	auto session_name_equals = [&session_name](const auto& key_value) {
		return key_value.second.name == session_name;
	};

	auto session_iter = std::find_if(user_sessions_begin, user_sessions_end, session_name_equals);

	return session_iter != user_sessions_end;
}

bool SessionService::session_exists_by_uuid(uint64_t user_id, const UUID& uuid) const noexcept
{
	const auto [user_sessions_begin, user_sessions_end] = sessions_.equal_range(user_id);

	auto session_name_equals = [&uuid](const auto& key_value) {
		return key_value.second.uuid == uuid;
	};

	auto session_iter = std::find_if(user_sessions_begin, user_sessions_end, session_name_equals);

	return session_iter != user_sessions_end;
}

UUID SessionService::create_session(uint64_t user_id, const std::string& session_name)
{
	assert(!session_name.empty());

	if(session_exists_by_name(user_id, session_name))
	{
		throw ObjectAlreadyExistsException("Session with provided name already exists");
	}

	Session new_session = {
			{},
			session_name
	};

	sessions_.insert(std::make_pair(user_id, new_session));

	return new_session.uuid;
}

void SessionService::destroy_session_by_name(uint64_t user_id, const std::string& session_name)
{
	if(!session_exists_by_name(user_id, session_name))
	{
		throw ObjectNotFoundException("Session does not exists");
	}

	const auto [user_sessions_begin, user_sessions_end] = sessions_.equal_range(user_id);
	const auto to_remove_iter = std::find_if(
			user_sessions_begin, user_sessions_end,
			[&session_name](const auto& key_value)
			{
				return key_value.second.name == session_name;
			}
	);

	sessions_.erase(to_remove_iter);
}

void SessionService::destroy_session_by_uuid(uint64_t user_id, const UUID& uuid)
{
	if(!session_exists_by_uuid(user_id, uuid))
	{
		throw ObjectNotFoundException("Session does not exists");
	}

	const auto [user_sessions_begin, user_sessions_end] = sessions_.equal_range(user_id);
	const auto to_remove_iter = std::find_if(
			user_sessions_begin, user_sessions_end,
			[&uuid](const auto& key_value)
			{
				return key_value.second.uuid == uuid;
			}
	);

	sessions_.erase(to_remove_iter);
}

void SessionService::destroy_user_sessions(uint64_t user_id)
{
	const auto [user_sessions_begin, user_sessions_end] = sessions_.equal_range(user_id);
	sessions_.erase(user_sessions_begin, user_sessions_end);
}

std::vector<SessionService::Session> SessionService::sessions_by_user(uint64_t user_id) const
{
	const auto [user_sessions_begin, user_sessions_end] = sessions_.equal_range(user_id);

	std::vector<Session> sessions;
	sessions.reserve(static_cast<std::size_t>(std::distance(user_sessions_begin, user_sessions_end)));

	std::transform(user_sessions_begin, user_sessions_end, std::back_inserter(sessions), [](const auto& key_value) {return key_value.second;});

	return sessions;
}

