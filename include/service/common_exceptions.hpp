#ifndef HERDSMAN_COMMON_EXCEPTIONS_HPP
#define HERDSMAN_COMMON_EXCEPTIONS_HPP

#include <stdexcept>

struct InvalidIdentifierException: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};

struct ObjectAlreadyExistsException: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};

struct ObjectNotFoundException: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};

struct ResourceLockedException: public std::runtime_error
{
	using std::runtime_error::runtime_error;
};


#endif //HERDSMAN_COMMON_EXCEPTIONS_HPP
