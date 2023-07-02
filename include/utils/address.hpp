#ifndef HERDSMAN_ADDRESS_HPP
#define HERDSMAN_ADDRESS_HPP

#include <string>


struct Address
{
	std::string hostname;
	uint16_t port;

	Address() = default;

	Address(std::string hostname, uint16_t port)
	: hostname(std::move(hostname)), port(port)
	{};
};

#endif //HERDSMAN_ADDRESS_HPP
