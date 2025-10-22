cmake_minimum_required(VERSION 3.12)

project(decoder)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

set(build_tools OFF CACHE BOOL "" FORCE)  # prevents executable target
set(build_tests OFF CACHE BOOL "" FORCE)
set(build_examples OFF CACHE BOOL "" FORCE)
set(build_kcd OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    dbcppp
    GIT_REPOSITORY https://github.com/xR3b0rn/dbcppp
    CMAKE_ARGS -Dbuild_tools=OFF -Dbuild_tests=OFF -Dbuild_examples=OFF -Dbuild_kcd=OFF
)

FetchContent_MakeAvailable(dbcppp)

if (TARGET dbcppp)
    set(build_tools OFF)
endif()

FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib
)

FetchContent_MakeAvailable(httplib)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(decoder main.cpp)

target_link_libraries(decoder PRIVATE dbcppp arrow_shared parquet_shared httplib::httplib)


