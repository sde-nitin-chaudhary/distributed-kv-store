cmake_minimum_required(VERSION 3.14)
project(DistributedKVStore VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

# Server executable sources
set(SERVER_SOURCES
    src/resp_parser.cpp
    src/lru_cache.cpp
    src/consistent_hash.cpp
    src/wal.cpp
    src/kv_server.cpp
    src/main.cpp
)

add_executable(kv_server ${SERVER_SOURCES})

if (WIN32)
    target_link_libraries(kv_server ws2_32)
else()
    target_link_libraries(kv_server pthread)
endif()

# Test executables
add_executable(test_cache
    src/lru_cache.cpp
    tests/test_cache.cpp
)

add_executable(test_hash
    src/consistent_hash.cpp
    tests/test_hash.cpp
)

add_executable(test_resp
    src/resp_parser.cpp
    tests/test_resp.cpp
)

add_executable(test_wal
    src/wal.cpp
    src/resp_parser.cpp
    src/lru_cache.cpp
    tests/test_wal.cpp
)

if (WIN32)
    target_link_libraries(test_wal ws2_32)
else()
    target_link_libraries(test_cache pthread)
    target_link_libraries(test_hash pthread)
    target_link_libraries(test_resp pthread)
    target_link_libraries(test_wal pthread)
endif()

enable_testing()
add_test(NAME test_cache COMMAND test_cache)
add_test(NAME test_hash COMMAND test_hash)
add_test(NAME test_resp COMMAND test_resp)
add_test(NAME test_wal COMMAND test_wal)
