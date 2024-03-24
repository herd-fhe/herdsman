find_package(GTest QUIET)

if (NOT GTest_FOUND)
	include(FetchContent)
	FetchContent_Declare(
		googletest
		GIT_REPOSITORY https://github.com/google/googletest.git
		GIT_TAG        release-1.12.1
	)

	FetchContent_GetProperties(googletest)
	if(NOT googletest_POPULATED)
		FetchContent_Populate(googletest)
		set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1 CACHE BOOL "")
		add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR} EXCLUDE_FROM_ALL)
		unset(CMAKE_SUPPRESS_DEVELOPER_WARNINGS)
	endif()
endif()

add_executable(herdsman_test "")
target_link_libraries(
	herdsman_test
	GTest::gtest GTest::gtest_main
	options_target
	warnings_target
)

include(GoogleTest)
gtest_discover_tests(herdsman_test)