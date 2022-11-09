#include "utils/uuid.hpp"

#include <stdexcept>


UUID::UUID()
{
	uuid_generate(uuid_);
}

UUID::UUID(const std::string& uuid_string)
{
	if(uuid_string.empty())
	{
		throw std::invalid_argument("Invalid uuid");
	}

	const auto result = uuid_parse(uuid_string.c_str(), uuid_);
	if(result != 0)
	{
		throw std::invalid_argument("Invalid uuid");
	}
}

std::strong_ordering operator<=>(const UUID &lhs, const UUID &rhs)
{
	return uuid_compare(lhs.uuid_, rhs.uuid_) <=> 0;
}

bool operator==(const UUID &lhs, const UUID &rhs)
{
	return uuid_compare(lhs.uuid_, rhs.uuid_) == 0;
}

std::string UUID::as_string() const
{
	std::string out;
	out.resize(UUID_STR_LEN-1);

	uuid_unparse(uuid_, out.data());

	return out;
}
