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
    # The 'dbcppp' library is already created. We check if the executable 
    # was also created and give it a unique name (e.g., dbcppp_cli)
    # Note: If the tool/executable is the last target created, this might rename the library. 
    # Use this with caution.
    # The safest bet is to check the target type, but CMake doesn't allow easy checking.
    
    # A safer approach is Option 1, but if you must, you can try:
    set(build_tools OFF)
    # set_target_properties(dbcppp PROPERTIES OUTPUT_NAME dbcppp_cli) 
endif()

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(decoder main.cpp)

target_link_libraries(decoder PRIVATE dbcppp arrow_shared parquet_shared)


