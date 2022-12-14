cmake_minimum_required(VERSION 3.21)
project(
        herdsman
        VERSION 0.1
        DESCRIPTION "herd manager"
        LANGUAGES CXX
)

# Warnings config
add_library(warnings_target INTERFACE)
if(MSVC)
    target_compile_options(
            warnings_target
            INTERFACE
            /W4
            /WX "/permissive-"
    )
else()
    target_compile_options(
            warnings_target
            INTERFACE
            -Wall
            -Wextra
            -Wshadow
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Wcast-align
            -Wdouble-promotion
            -Wunused
            -Woverloaded-virtual
            -Wpedantic
            -Wconversion
            -Wsign-conversion
            -Wnull-dereference
            -Werror
    )
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        target_compile_options(
                warnings_target
                INTERFACE
                -Waddress-of-packed-member
                -Wmisleading-indentation
                -Wduplicated-cond
                -Wduplicated-branches
                -Wlogical-op
                -Wuseless-cast
        )
    endif()
endif()


# Project options config

add_library(options_target INTERFACE)
target_compile_features(options_target INTERFACE cxx_std_20)

if(!MSVC)
    option(ENABLE_COVERAGE "Enable coverage reporting (gss/clang)" FALSE)

    if(ENABLE_COVERAGE)
        target_compile_options(options_target INTERFACE --coverage -O0 -g)
        target_link_libraries(options_targer INTERFACE --coverage)
    endif()
endif()

# Dependencies
find_package(PkgConfig REQUIRED)

## OpenFHE
find_package(OpenFHE REQUIRED)

## SQLite
find_package(SQLite3)

## Sodium
pkg_check_modules(SODIUM REQUIRED libsodium)

## UUID
pkg_search_module(UUID REQUIRED uuid)

## Paseto
add_subdirectory(third-party/libpaseto EXCLUDE_FROM_ALL)

## YAML
add_subdirectory(third-party/yaml-cpp EXCLUDE_FROM_ALL)

## Spdlog
add_subdirectory(third-party/spdlog EXCLUDE_FROM_ALL)

find_package(Threads REQUIRED)
find_package(OpenMP REQUIRED)

# Targets
## Herd - proto
add_subdirectory(proto)

## Herdsman - config
add_custom_target(herdsman-config ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/herdsman.yaml)
add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/herdsman.yaml
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/template.yaml
                                         ${CMAKE_CURRENT_BINARY_DIR}/herdsman.yaml
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/template.yaml
)

## Herdsman
add_executable(herdsman)

target_include_directories(herdsman PRIVATE include)

target_sources(
        herdsman
        PRIVATE

        src/main.cpp
        src/utils/config.cpp
        src/utils/paseto_utils.cpp
        src/utils/file_utils.cpp
        src/utils/uuid.cpp
        src/utils/controller_utils.cpp

        src/controller/auth_controller.cpp
        src/controller/session_controller.cpp

        src/service/auth_service.cpp
        src/service/session_service.cpp
        src/service/key_service.cpp

        src/mapper/schema_type_mapper.cpp

        src/plugins/token_auth_metadata_processor.cpp

        include/utils/config.hpp
        include/utils/paseto_utils.hpp
        include/utils/string_utils.hpp
        include/utils/file_utils.hpp
        include/utils/uuid.hpp
        include/utils/controller_utils.hpp

        include/controller/auth_controller.hpp
        include/controller/session_controller.hpp

        include/service/auth_service.hpp
        include/service/session_service.hpp
        include/service/key_service.hpp

        include/mapper/schema_type_mapper.hpp

        include/schema_type.hpp

        include/service/common_exceptions.hpp

        include/plugins/token_auth_metadata_processor.hpp
)

target_compile_definitions(
        herdsman
        PUBLIC
        MATHBACKEND=2
)

target_link_libraries(
        herdsman
        PRIVATE
        options_target
        warnings_target
)

target_link_libraries(
        herdsman
        PRIVATE
        pthread

        OPENFHEcore
        OPENFHEbinfhe
        OpenMP::OpenMP_CXX

        ${SODIUM_LIBRARIES}
        ${UUID_LIBRARIES}
        paseto
        SQLite::SQLite3

        yaml-cpp::yaml-cpp

        spdlog::spdlog_header_only

        herd_proto
)

target_include_directories(
        herdsman
        SYSTEM PRIVATE
        ${OpenFHE_INCLUDE}
        ${OpenFHE_INCLUDE}/third-party/include
        ${OpenFHE_INCLUDE}/core
        ${OpenFHE_INCLUDE}/pke
        ${OpenFHE_INCLUDE}/binfhe
        ${SODIUM_INCLUDE_DIRS}
        ${UUID_INCLUDE_DIRS}
        third-party/libpaseto/include
)

# Hack for yaml-cpp
get_target_property(YAML_INCLUDE yaml-cpp::yaml-cpp INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories(herdsman SYSTEM PRIVATE ${YAML_INCLUDE})

add_dependencies(herdsman herdsman-config)

# Test

option(ENABLE_TESTS "Enable tests" FALSE)

if(ENABLE_TESTS)
    enable_testing()
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

    add_executable(libherd_test "")
    target_link_libraries(
            herdsman_test
            herdsman
            GTest::gtest GTest::gtest_main
            options_target
            warnings_target
    )

#    target_sources(
#            herdsman_test
#            PRIVATE
#    )
endif()