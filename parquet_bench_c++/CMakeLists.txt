cmake_minimum_required(VERSION 3.19)
project(parquet_bench)

set(CMAKE_CXX_STANDARD 20)
LINK_DIRECTORIES(/usr/include)

add_executable(parquet_bench main.cpp)
TARGET_LINK_LIBRARIES(parquet_bench arrow parquet)