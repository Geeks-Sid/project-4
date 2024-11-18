cmake_minimum_required(VERSION 3.10)

project(project4)

find_program(PROTOC NAMES protoc)
if (NOT EXISTS ${PROTOC})
  message(FATAL_ERROR "protoc not found")
endif()
message(STATUS "protoc found: ${PROTOC}")

find_program(GRPC_PLUGIN NAMES "grpc_cpp_plugin")
if (NOT EXISTS ${GRPC_PLUGIN})
  message(WARNING "grpc_cpp_plugin not found, gRPC classes not generated")
else()
  message(STATUS "grpc_cpp_plugin found: ${GRPC_PLUGIN}")
endif()

if (NOT DEFINED PROTO_SEARCH_PATH)
  set(PROTO_SEARCH_PATH ${CMAKE_CURRENT_SOURCE_DIR})
endif()

set(PROTO_CFLAGS)
list(APPEND PROTOC_FLAGS "--proto_path=${PROTO_SEARCH_PATH}")
list(APPEND PROTOC_FLAGS "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}") 

if (EXISTS ${GRPC_PLUGIN})
  list(APPEND PROTOC_FLAGS "--plugin=protoc-gen-grpc=${GRPC_PLUGIN}")
  list(APPEND PROTOC_FLAGS "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}") 
endif()

file(GLOB_RECURSE proto_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.proto)

set(ABSProtoFiles)
set(ProtoHeaders)
set(ProtoSources)

include_directories(${ProtoHeaders})

foreach(FNAME ${proto_files})
  get_filename_component(PROTO_PATH ${FNAME} DIRECTORY)
  get_filename_component(PROTO_NAME ${FNAME} NAME_WE)
  set(GENERATED_PROTO_PATH "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_PATH}/${PROTO_NAME}")

  list(APPEND ABSProtoFiles "${PROTO_SEARCH_PATH}/${FNAME}")
  list(APPEND ProtoHeaders "${GENERATED_PROTO_PATH}.pb.h")
  list(APPEND ProtoSources "${GENERATED_PROTO_PATH}.pb.cc")
  install (FILES ${GENERATED_PROTO_PATH}.pb.h DESTINATION include/${PROTO_PATH})

  if (EXISTS ${GRPC_PLUGIN})
    list(APPEND ProtoHeaders "${GENERATED_PROTO_PATH}.grpc.pb.h")
    list(APPEND ProtoSources "${GENERATED_PROTO_PATH}.grpc.pb.cc")
    install (FILES "${GENERATED_PROTO_PATH}.grpc.pb.h" DESTINATION "include/${PROTO_PATH}")
  endif()
endforeach()

add_custom_command(
  COMMAND ${PROTOC} ${PROTOC_FLAGS} ${ABSProtoFiles}
  OUTPUT ${ProtoSources} ${ProtoHeaders}
  COMMENT "Generating proto messages..."
  DEPENDS ${ABSProtoFiles}
)

add_library(p4protolib ${ProtoHeaders} ${ProtoSources})
target_link_libraries(p4protolib PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_include_directories(p4protolib PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_BINARY_DIR})
