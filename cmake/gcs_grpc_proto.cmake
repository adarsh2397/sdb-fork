# =============================================================================
# Copyright 2026, Sirius Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
# =============================================================================
#
# Generates C++ + gRPC stubs for the Cloud Storage gRPC API
# (google.storage.v2) used by the native GCS gRPC backend
# (src/io/gcs/gcs_grpc_*). Produces a static library target
# `gcs_storage_protos` carrying the generated sources, linked against the gRPC
# and protobuf runtimes.
#
# storage.proto has a large transitive import graph (google/api/*,
# google/rpc/*, google/type/*, google/iam/v1/*, plus protobuf well-known
# types). We fetch the googleapis repo at a pinned commit and run protoc over
# the curated subset of protos that storage.proto pulls in. NOTE: this proto
# list is the most likely thing to need adjustment when first building — if
# protoc reports an unresolved import, add the named .proto to
# GCS_GRPC_PROTO_FILES below.

include(FetchContent)

find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# protoc + grpc_cpp_plugin come from the conda-forge grpc-cpp / vcpkg grpc
# toolchain. Prefer the imported targets when present, fall back to the binaries.
if(TARGET protobuf::protoc)
  set(_GCS_PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
  find_program(_GCS_PROTOC protoc REQUIRED)
endif()
if(TARGET gRPC::grpc_cpp_plugin)
  set(_GCS_GRPC_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  find_program(_GCS_GRPC_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

# Pinned googleapis snapshot. Bump deliberately; the storage.proto surface
# (ReadObject / BidiReadObject) is stable but field numbers must match runtime.
FetchContent_Declare(
  googleapis
  GIT_REPOSITORY https://github.com/googleapis/googleapis.git
  GIT_TAG        master # TODO(grpc-backend): pin to a specific commit before merge
  GIT_SHALLOW    TRUE)
FetchContent_MakeAvailable(googleapis)

set(_GCS_PROTO_ROOT ${googleapis_SOURCE_DIR})
set(_GCS_PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/gcs_grpc_gen)
file(MAKE_DIRECTORY ${_GCS_PROTO_OUT})

# storage.proto annotates several bytes fields (notably ChecksummedData.content)
# with `[ctype = CORD]`. Protobuf builds without Cord support (the common
# conda-forge / vcpkg case) generate the std::string content() accessor as
# PRIVATE and emit no usable public Cord accessor — making the field unreadable
# ("content() is private"). Strip just the `ctype = CORD` directive in-place
# before protoc runs so the generated accessor is a plain public
# `const std::string& content()`. The directive appears in any list position and
# is usually combined with other field options, e.g.
#   bytes content = 1 [ctype = CORD, (google.api.field_behavior) = OPTIONAL];
# so we remove only the CORD option (and its adjoining comma), preserving the
# rest. The four passes cover sole / first / middle / last positions, for every
# CORD field in the file.
file(READ ${_GCS_PROTO_ROOT}/google/storage/v2/storage.proto _gcs_storage_proto_src)
string(REGEX REPLACE "\\[ *ctype *= *CORD *\\]" ""  _gcs_storage_proto_src "${_gcs_storage_proto_src}")  # sole option
string(REGEX REPLACE ", *ctype *= *CORD *,"      "," _gcs_storage_proto_src "${_gcs_storage_proto_src}")  # middle of list
string(REGEX REPLACE "\\[ *ctype *= *CORD *, *"  "[" _gcs_storage_proto_src "${_gcs_storage_proto_src}")  # first in list
string(REGEX REPLACE ", *ctype *= *CORD *\\]"    "]" _gcs_storage_proto_src "${_gcs_storage_proto_src}")  # last in list
file(WRITE ${_GCS_PROTO_ROOT}/google/storage/v2/storage.proto "${_gcs_storage_proto_src}")

# Curated subset of protos to generate. storage.proto is the only one needing
# the grpc plugin (it defines the Storage service); the rest are message-only
# dependencies. Add entries here if protoc reports an unresolved import.
set(GCS_GRPC_PROTO_FILES
    google/storage/v2/storage.proto
    google/api/annotations.proto
    google/api/client.proto
    google/api/field_behavior.proto
    google/api/launch_stage.proto
    google/api/http.proto
    google/api/resource.proto
    google/api/routing.proto
    google/rpc/status.proto
    google/type/date.proto
    google/type/expr.proto
    google/iam/v1/iam_policy.proto
    google/iam/v1/policy.proto
    google/iam/v1/options.proto)

set(_GCS_GEN_SRCS "")
foreach(_proto ${GCS_GRPC_PROTO_FILES})
  get_filename_component(_dir ${_proto} DIRECTORY)
  get_filename_component(_name ${_proto} NAME_WE)
  set(_pb_cc ${_GCS_PROTO_OUT}/${_dir}/${_name}.pb.cc)
  set(_pb_h ${_GCS_PROTO_OUT}/${_dir}/${_name}.pb.h)
  list(APPEND _GCS_GEN_SRCS ${_pb_cc})

  # Only storage.proto needs the gRPC service stub.
  set(_grpc_args "")
  set(_grpc_out "")
  if(_proto STREQUAL "google/storage/v2/storage.proto")
    set(_grpc_cc ${_GCS_PROTO_OUT}/${_dir}/${_name}.grpc.pb.cc)
    list(APPEND _GCS_GEN_SRCS ${_grpc_cc})
    set(_grpc_args --grpc_out=${_GCS_PROTO_OUT}
                   --plugin=protoc-gen-grpc=${_GCS_GRPC_PLUGIN})
    set(_grpc_out ${_grpc_cc})
  endif()

  add_custom_command(
    OUTPUT ${_pb_cc} ${_pb_h} ${_grpc_out}
    COMMAND ${_GCS_PROTOC} --proto_path=${_GCS_PROTO_ROOT}
            --cpp_out=${_GCS_PROTO_OUT} ${_grpc_args} ${_proto}
    DEPENDS ${_GCS_PROTO_ROOT}/${_proto}
    COMMENT "protoc (gcs grpc): ${_proto}"
    VERBATIM)
endforeach()

add_library(gcs_storage_protos STATIC ${_GCS_GEN_SRCS})
# Wrap the generated-headers dir in $<BUILD_INTERFACE:> so the path applies only
# in the build tree. Required because gcs_storage_protos is in DuckDB's export
# set (see CMakeLists.txt install(TARGETS ... gcs_storage_protos EXPORT ...)):
# CMake forbids exporting a raw build-directory path on a target's include
# interface. These protos are internal (only sirius_extension consumes the
# headers, at build time), so there is no $<INSTALL_INTERFACE:> counterpart.
target_include_directories(gcs_storage_protos PUBLIC $<BUILD_INTERFACE:${_GCS_PROTO_OUT}>)
target_link_libraries(gcs_storage_protos PUBLIC gRPC::grpc++ protobuf::libprotobuf)
# Generated protobuf code triggers these under -Werror; quiet them locally.
target_compile_options(gcs_storage_protos PRIVATE -Wno-unused-parameter)
