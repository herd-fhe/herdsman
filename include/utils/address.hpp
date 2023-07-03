#ifndef HERDSMAN_ADDRESS_HPP
#define HERDSMAN_ADDRESS_HPP

#include <string>


struct Address
{
	std::string hostname;
	uint16_t port;

	Address() = default;

	Address(std::string address_hostname, uint16_t address_port)
	: hostname(std::move(address_hostname)), port(address_port)
	{};
};

#endif //HERDSMAN_ADDRESS_HPP
