file(GLOB SRCS *.cpp)
MESSAGE($SRCS)
ADD_EXECUTABLE(pactree-example example.cpp)

target_include_directories(pactree PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
TARGET_LINK_LIBRARIES(
	pactree-example
        pactree
        pdlart
	tbb
        numa
	pmem
	pmemobj
)


