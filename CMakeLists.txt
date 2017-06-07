cmake_minimum_required(VERSION 2.8)
project(cmongolib)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/libs/mongo-c-driver/build/cmake ${PROJECT_SOURCE_DIR}/libs/libbson/build/cmake)

set(BSON_ROOT_DIR ${PROJECT_SOURCE_DIR}/libs/libbson)

add_subdirectory(libs/libbson)
add_subdirectory(libs/mongo-c-driver)

set(SOURCE_FILES
        mongo.c
        mongo.h)

add_executable(cmongolib ${SOURCE_FILES})