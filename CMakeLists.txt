cmake_minimum_required(VERSION 2.6.4)
project (FILTER)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
add_subdirectory(streamcorpus)
add_subdirectory(filter)
add_subdirectory(dump)
add_subdirectory(select)
