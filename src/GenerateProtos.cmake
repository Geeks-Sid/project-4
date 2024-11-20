# CMake configuration for generating Protocol Buffers and gRPC files

# Specify the minimum version of CMake required
cmake_minimum_required(VERSION 3.10)

# Define the project name
project(project4)

# Locate the Protocol Buffers compiler (protoc)
find_program(PROTOC NAMES protoc)
if (NOT EXISTS ${PROTOC})
  message(FATAL_ERROR "Error: Protocol Buffers compiler 'protoc' not found. Please ensure it is installed and available in your PATH.")
else()
  message(STATUS "Protocol Buffers compiler 'protoc' found: ${PROTOC}")
endif()

# Locate the gRPC plugin for protoc
find_program(GRPC_PLUGIN NAMES "grpc_cpp_plugin")
if (NOT EXISTS ${GRPC_PLUGIN})
  message(WARNING "Warning: gRPC plugin 'grpc_cpp_plugin' not found. gRPC classes will not be generated.")
else()
  message(STATUS "gRPC plugin 'grpc_cpp_plugin' found: ${GRPC_PLUGIN}")
endif()

# Set the default search path for .proto files if not already defined
if (NOT DEFINED PROTO_SEARCH_PATH)
  set(PROTO_SEARCH_PATH ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# Initialize flags for protoc
set(PROTOC_FLAGS)
list(APPEND PROTOC_FLAGS "--proto_path=${PROTO_SEARCH_PATH}")
list(APPEND PROTOC_FLAGS "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}")

# Add gRPC plugin flags if the plugin exists
if (EXISTS ${GRPC_PLUGIN})
  list(APPEND PROTOC_FLAGS "--plugin=protoc-gen-grpc=${GRPC_PLUGIN}")
  list(APPEND PROTOC_FLAGS "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}")
endif()

# Recursively find all .proto files in the source directory
file(GLOB_RECURSE proto_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.proto)

# Initialize lists to store absolute proto file paths, headers, and source files
set(ABSProtoFiles)
set(ProtoHeaders)
set(ProtoSources)

# Include directories for generated headers
include_directories(${ProtoHeaders})

# Process each .proto file
foreach(FNAME ${proto_files})
  # Extract the directory and name without extension of the proto file
  get_filename_component(PROTO_PATH ${FNAME} DIRECTORY)
  get_filename_component(PROTO_NAME ${FNAME} NAME_WE)
  set(GENERATED_PROTO_PATH "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_PATH}/${PROTO_NAME}")

  # Append the absolute path of the proto file
  list(APPEND ABSProtoFiles "${PROTO_SEARCH_PATH}/${FNAME}")
  # Append generated header and source files for protobuf
  list(APPEND ProtoHeaders "${GENERATED_PROTO_PATH}.pb.h")
  list(APPEND ProtoSources "${GENERATED_PROTO_PATH}.pb.cc")
  # Install the generated protobuf header
  install(FILES ${GENERATED_PROTO_PATH}.pb.h DESTINATION include/${PROTO_PATH})

  # Append generated header and source files for gRPC if the plugin exists
  if (EXISTS ${GRPC_PLUGIN})
    list(APPEND ProtoHeaders "${GENERATED_PROTO_PATH}.grpc.pb.h")
    list(APPEND ProtoSources "${GENERATED_PROTO_PATH}.grpc.pb.cc")
    # Install the generated gRPC header
    install(FILES "${GENERATED_PROTO_PATH}.grpc.pb.h" DESTINATION "include/${PROTO_PATH}")
  endif()
endforeach()

# Add a custom command to generate protobuf and gRPC files
add_custom_command(
  COMMAND ${PROTOC} ${PROTOC_FLAGS} ${ABSProtoFiles}
  OUTPUT ${ProtoSources} ${ProtoHeaders}
  COMMENT "Generating Protocol Buffers and gRPC source files..."
  DEPENDS ${ABSProtoFiles}
)

# Create a library target for the generated protobuf and gRPC files
add_library(p4protolib ${ProtoHeaders} ${ProtoSources})
# Link the library with protobuf and gRPC libraries
target_link_libraries(p4protolib PUBLIC protobuf::libprotobuf gRPC::grpc++)
# Include directories for the library
target_include_directories(p4protolib PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_BINARY_DIR})
