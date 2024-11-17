# CMakeLists.txt
cmake_minimum_required(VERSION 3.10)

project(project4)
set (CMAKE_CXX_STANDARD 17)

include(GenerateProtos.cmake)

add_library(
  mapreducelib #library name
  mapreduce.cc mapreduce_impl.cc #sources
  master.h  mapreduce_spec.h file_shard.h ) #headers
if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 7)
    target_link_libraries(mapreducelib p4protolib stdc++fs)
    else()
    target_link_libraries(mapreducelib p4protolib )
endif ()

target_include_directories(mapreducelib PUBLIC ${MAPREDUCE_INCLUDE_DIR})
add_dependencies(mapreducelib p4protolib)

add_library(
  mr_workerlib #library name
  mr_task_factory.cc run_worker.cc #sources
  mr_tasks.h worker.h ) #headers

if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 7)
    target_link_libraries(mr_workerlib p4protolib stdc++fs)
else()
    target_link_libraries(mr_workerlib p4protolib)
endif ()
#target_link_libraries(mr_workerlib p4protolib stdc++fs)
target_include_directories(mr_workerlib PUBLIC ${MAPREDUCE_INCLUDE_DIR})
add_dependencies(mr_workerlib p4protolib)