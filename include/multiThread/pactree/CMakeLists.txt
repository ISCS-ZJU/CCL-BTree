cmake_minimum_required(VERSION 3.11)
include(FetchContent)

set(CMAKE_BUILD_TYPE Release)
#set(CMAKE_BUILD_TYPE Debug)

#add_definitions(-DVAR_KEY)
#add_definitions(-DMEMORY_FOOTPRINT)

project(pactree)

project(pactree)

include_directories(./include)
include_directories(./src)

execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clwb OUTPUT_VARIABLE CLWB)
if(CLWB)
    message(STATUS "Use clwb")
else()
        add_definitions(-DNO_CLWB)
    message(STATUS "Use clflush instaed of clwb")
endif()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-fPIC -pthread -Wall -Wextra -march=native -mavx512bw")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

#include_directories(${CMAKE_SOURCE_DIR}/include)
enable_testing()


add_subdirectory(lib/PDL-ART)
add_subdirectory(src)
add_subdirectory(example)
#add_subdirectory(tests)
#add_subdirectory(benchmarks)
