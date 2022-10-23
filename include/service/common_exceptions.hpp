#ifndef HERDSMAN_COMMON_EXCEPTIONS_HPP
#define HERDSMAN_COMMON_EXCEPTIONS_HPP

#include <stdexcept>


struct InvalidIdentifierException: public std::runtime_error
{
	explicit InvalidIdentifierException(const std::string& arg): std::runtime_error(arg) {};
};

struct ObjectAlreadyExistsException: public std::runtime_error
{
	explicit ObjectAlreadyExistsException(const std::string& arg): std::runtime_error(arg) {};
};

struct ObjectNotFoundException: public std::runtime_error
{
	explicit ObjectNotFoundException(const std::string& arg): std::runtime_error(arg) {};
};


#endif //HERDSMAN_COMMON_EXCEPTIONS_HPP
