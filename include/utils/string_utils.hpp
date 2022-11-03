#ifndef HERDSMAN_STRING_UTILS_HPP
#define HERDSMAN_STRING_UTILS_HPP

#include <string>
#include <algorithm>
#include <locale>
#include <concepts>


template<typename InputIterator>
std::string trim(InputIterator begin, InputIterator end, const std::locale& current_locale = {})
requires std::input_iterator<InputIterator> && std::is_same_v<typename InputIterator::value_type, std::string::value_type>
{
	assert(begin <= end);

	std::string res;
	res.resize(static_cast<size_t>(std::distance(begin, end)));
	std::remove_copy_if(
			begin, end, std::begin(res),
			[&current_locale](auto character)
			{
				return std::isspace(character, current_locale);
			}
	);

	return res;
}

std::string trim(const std::string& val, const std::locale& current_locale = {})
{
	return trim(std::begin(val), std::end(val), current_locale);
}

#endif //HERDSMAN_STRING_UTILS_HPP
