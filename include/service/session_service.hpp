#ifndef HERDSMAN_SESSION_SERVICE_HPP
#define HERDSMAN_SESSION_SERVICE_HPP

#include <string>
#include <map>
#include <vector>

#include "herd/common/uuid.hpp"


class SessionService
{
public:
	struct Session
	{
		herd::common::UUID uuid;
		std::string name;
	};

	[[nodiscard]] herd::common::UUID create_session(uint64_t user_id, const std::string& session_name);

	void destroy_session_by_name(uint64_t user_id, const std::string& session_name);
	void destroy_session_by_uuid(uint64_t user_id, const herd::common::UUID& uuid);
	void destroy_user_sessions(uint64_t user_id);

	[[nodiscard]] std::vector<Session> sessions_by_user(uint64_t user_id) const;
	[[nodiscard]] bool session_exists_by_name(uint64_t user_id, const std::string& session_name) const noexcept;
	[[nodiscard]] bool session_exists_by_uuid(uint64_t user_id, const herd::common::UUID& uuid) const noexcept;
private:
	std::multimap<uint64_t, Session> sessions_;

};


#endif //HERDSMAN_SESSION_SERVICE_HPP
