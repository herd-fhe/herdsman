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
        target_link_libraries(options_target INTERFACE --coverage)
    endif()
endif()

# Dependencies
find_package(PkgConfig REQUIRED)

find_package(CURL REQUIRED)
add_subdirectory(third-party/json)

## OpenFHE
find_package(OpenFHE REQUIRED)

## SQLite
find_package(SQLite3)

## Sodium
pkg_check_modules(SODIUM REQUIRED libsodium)

## Paseto
pkg_check_modules(PASETO QUIET paseto)
if(NOT PASETO_MODULE_NAME)
    message("Paseto not found. Using included")
    add_subdirectory(third-party/libpaseto EXCLUDE_FROM_ALL)
endif ()

## YAML
add_subdirectory(third-party/yaml-cpp EXCLUDE_FROM_ALL)

## Spdlog
add_subdirectory(third-party/spdlog EXCLUDE_FROM_ALL)

find_package(Threads REQUIRED)
find_package(OpenMP REQUIRED)

## Herd - libraries
add_subdirectory(proto)
add_subdirectory(common)
add_subdirectory(mappers)

# Targets

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
        src/utils/controller_utils.cpp

        src/execution/executor/executor.cpp
        src/execution/execution_plan/execution_plan_analyzer.cpp

        src/controller/auth_controller.cpp
        src/controller/session_controller.cpp
        src/controller/storage_controller.cpp
        src/controller/execution_controller.cpp

        src/service/auth_service.cpp
        src/service/session_service.cpp
        src/service/key_service.cpp
        src/service/storage_service.cpp
        src/service/execution_service.cpp

        src/execution/worker/i_worker_group.cpp

        src/execution/worker/grpc/grpc_worker_group.cpp

        src/execution/worker/lambda/lambda_http_worker_group.cpp
        src/execution/worker/lambda/filesystem_watch.cpp

        src/plugins/token_auth_metadata_processor.cpp

        include/model/task.hpp

        include/utils/config.hpp
        include/utils/address.hpp
        include/utils/paseto_utils.hpp
        include/utils/string_utils.hpp
        include/utils/file_utils.hpp
        include/utils/controller_utils.hpp

        include/execution/executor/i_executor.hpp
        include/execution/executor/executor.hpp
        include/execution/executor/event/job_scheduled.hpp
        include/execution/executor/event/event.hpp
        include/execution/execution_plan/execution_plan_analyzer.hpp

        include/controller/auth_controller.hpp
        include/controller/session_controller.hpp
        include/controller/storage_controller.hpp
        include/controller/execution_controller.hpp

        include/service/auth_service.hpp
        include/service/session_service.hpp
        include/service/key_service.hpp
        include/service/storage_service.hpp
        include/service/execution_service.hpp

        include/execution/worker/i_worker_group.hpp

        include/execution/worker/grpc/grpc_worker_group.hpp

        include/execution/worker/lambda/lambda_http_worker_group.hpp
        include/execution/worker/lambda/filesystem_watch.hpp

        include/service/common_exceptions.hpp

        include/plugins/token_auth_metadata_processor.hpp
)

#  Fix for gcc bug https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105580
set_source_files_properties(src/utils/file_utils.cpp PROPERTIES COMPILE_FLAGS -Wno-null-dereference)

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
        SQLite::SQLite3
        CURL::libcurl

        yaml-cpp::yaml-cpp
        nlohmann_json::nlohmann_json

        spdlog::spdlog_header_only

        herd_proto
        herd_common
        herd_dto_mappers
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
)

if(PASETO_MODULE_NAME)
    target_include_directories(herdsman SYSTEM PRIVATE ${PASETO_INCLUDE_DIRS})
    target_link_directories(herdsman PRIVATE ${PASETO_LIBRARY_DIRS})
    target_link_libraries(herdsman PRIVATE ${PASETO_LIBRARIES})
else()
    target_include_directories(herdsman SYSTEM PRIVATE third-party/libpaseto/include)
    target_link_libraries(herdsman PRIVATE paseto)
endif()

# Hack for yaml-cpp
get_target_property(YAML_INCLUDE yaml-cpp::yaml-cpp INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories(herdsman SYSTEM PRIVATE ${YAML_INCLUDE})

add_dependencies(herdsman herdsman-config)

# Install
install(TARGETS herdsman DESTINATION bin)

# Test

option(ENABLE_TESTS "Enable tests" FALSE)
if(ENABLE_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()