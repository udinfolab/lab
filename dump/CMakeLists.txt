cmake_minimum_required(VERSION 2.6.4)

add_definitions(-DHAVE_NETINET_IN_H)
add_definitions(-DHAVE_INTTYPES_H)

# find path to thrift
find_path(THRIFT_INCLUDE_DIR protocol/TBinaryProtocol.h
    /usr/local/include/thrift
    )
message(STATUS "Include file detected: [${THRIFT_INCLUDE_DIR}].")
include_directories(${THRIFT_INCLUDE_DIR})

# path to streamcorpus
include_directories(${FILTER_SOURCE_DIR}/streamcorpus)
# make sure linker can find the streamcorpus library
link_directories(${FILTER_BINARY_DIR}/streamcorpus)

add_executable(dump dump.cpp)

# link the executable to the streamcorpus library
target_link_libraries(dump streamcorpus thrift boost_filesystem
  boost_program_options boost_system)
