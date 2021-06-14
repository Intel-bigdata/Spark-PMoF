cmake_minimum_required(VERSION 3.11)
project(rpmof)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-std=c++14 -g -pthread -fPIC -ldl -lz")

# Generate compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# place binaries and libraries according to GNU standards
include(GNUInstallDirs)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR})

if(CMAKE_CXX_COMPILER_ID MATCHES GNU)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

include(cmake/googletest.cmake)

fetch_googletest(
  ${PROJECT_SOURCE_DIR}/cmake
  ${PROJECT_BINARY_DIR}/googletest
)

find_package(Boost REQUIRED COMPONENTS program_options serialization)
if(Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
endif()

include_directories(${PROJECT_SOURCE_DIR}/)
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/include/spdlog/include)
include_directories(${PROJECT_BINARY_DIR}/googletest/googletest-src/googletest/include)
include_directories(${PROJECT_SOURCE_DIR}/include/hiredis)
include_directories(${PROJECT_SOURCE_DIR}/include/jsoncpp/include)

enable_testing()

add_subdirectory(include/spdlog)
add_subdirectory(pmpool)
add_subdirectory(test)
add_subdirectory(benchmark)

file(COPY bin/start-rpmp.sh DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
file(COPY bin/stop-rpmp.sh DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

add_executable(data-server main.cc)
add_executable(chashtest test/chash_test.cc)
add_executable(rocksdbtest test/rocksdb_test.cc)
add_executable(proxy-server ProxyMain.cc)
add_executable(client Client.cc)
target_link_libraries(chashtest pmpool spdlog hiredis jsoncpp rocksdb redis++)
target_link_libraries(data-server pmpool spdlog hiredis jsoncpp rocksdb redis++)
target_link_libraries(proxy-server pmpool spdlog hiredis jsoncpp rocksdb redis++)
target_link_libraries(client pmpool spdlog hiredis jsoncpp rocksdb redis++)
target_link_libraries(rocksdbtest spdlog hiredis jsoncpp rocksdb redis++)

file(COPY ${PROJECT_SOURCE_DIR}/config DESTINATION .)
