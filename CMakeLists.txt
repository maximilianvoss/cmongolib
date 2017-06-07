cmake_minimum_required(VERSION 2.8)
project(cmongolib)

set(SOURCE_FILES
        mongo.c
        mongo.h)

add_executable(cmongolib ${SOURCE_FILES})