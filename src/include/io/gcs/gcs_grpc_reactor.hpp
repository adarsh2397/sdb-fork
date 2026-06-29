/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "io/io_context.hpp"
#include "io/s3/s3_request_authorizer.hpp"
#include "io/types.hpp"

#include <cudf/io/text/byte_range_info.hpp>

#include <cuda_runtime.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace cucascade::memory {
class fixed_size_host_memory_resource;
}  // namespace cucascade::memory

namespace sirius::io::gcs {

/// Immutable per-object descriptor carried (by shared_ptr) in every read
/// request so the object outlives the async transfer. Mirrors
/// s3_reactor's s3_object_state; for gRPC the "key" is the object name and the
/// "bucket" is the gRPC resource path "projects/_/buckets/<bucket>".
struct gcs_grpc_object_state {
  std::string bucket;  ///< bare bucket name (resource path built at call time)
  std::string key;     ///< object name
  std::size_t object_size{0};
  std::int64_t generation{0};  ///< pinned read generation (0 = latest)
};

using gcs_grpc_native_handle = std::shared_ptr<const gcs_grpc_object_state>;

/// io_object for the gRPC backend. Satisfies templated_ioctx's io_object_c
/// (host_handle/device_handle returning the native handle). Distinct from the
/// XML-API gcs_io_object / s3_async_io_object.
class gcs_grpc_io_object : public sirius_io_object {
 public:
  gcs_grpc_io_object(std::string uri, std::shared_ptr<const gcs_grpc_object_state> state)
    : _uri(std::move(uri)), _state(std::move(state))
  {
  }

  [[nodiscard]] const std::string& raw_file_cache_id() const noexcept override { return _uri; }
  [[nodiscard]] const std::string& object_path() const noexcept override { return _uri; }
  [[nodiscard]] std::size_t size() const noexcept override { return _state->object_size; }

  [[nodiscard]] gcs_grpc_native_handle host_handle() const noexcept { return _state; }
  [[nodiscard]] gcs_grpc_native_handle device_handle() const noexcept { return _state; }

 private:
  std::string _uri;
  std::shared_ptr<const gcs_grpc_object_state> _state;
};

/**
 * @brief Native GCS gRPC reactor (google.storage.v2 ReadObject).
 *
 * Models the @c io_reactor_c concept consumed by @c templated_ioctx, exactly
 * like @c s3_reactor — so the scan path, prefetch cache, and pipeline are
 * unchanged; only the transport differs. Reads issue server-streaming
 * @c ReadObject RPCs over a gRPC channel (DirectPath when the VM + bucket are
 * co-located), draining @c ReadObjectResponse chunks into a caller buffer
 * (host path) or a borrowed pinned staging block followed by an async H2D copy
 * (device path), mirroring s3_reactor's pinned-staging + stream-callback model.
 *
 * Auth reuses the existing @c s3_request_authorizer chain (e.g.
 * @c gcs_metadata_server_authorizer): the bearer token it produces is attached
 * to each RPC as gRPC call credentials rather than an HTTP header.
 *
 * @note BidiReadObject (multi-range, Rapid Storage fast path) is private
 *       preview at time of writing; this reactor uses the GA single-range
 *       @c ReadObject and is structured so the per-range submit can later fan
 *       into one bidi stream.
 */
class gcs_grpc_reactor {
 public:
  struct config {
    std::shared_ptr<sirius::io::s3::s3_request_authorizer> creds;
    std::string endpoint{"storage.googleapis.com"};  ///< gRPC target (DirectPath: google-c2p:///)
    /// When true, build the channel for DirectPath: target the c2p resolver
    /// ("google-c2p:///<host>") with GoogleDefaultCredentials (ALTS handshake +
    /// compute-SA auth), bypassing the GFE on a co-located GCE VM. Auth in this
    /// mode is handled by GoogleDefaultCredentials, so @c creds is not used for
    /// the bearer header (it is still required for the non-DirectPath path and
    /// for stat/HEAD construction). Off-GCE this must be false. gRPC auto-falls
    /// back to CFE/TLS if DirectPath cannot be negotiated.
    bool directpath{false};
    long request_timeout_s{60};
    std::size_t max_streams{16};  ///< concurrent ReadObject streams in flight
    cucascade::memory::fixed_size_host_memory_resource* host_memory_resource{nullptr};
    std::size_t max_retry_attempts{4};
    std::chrono::milliseconds retry_backoff_base{50};
    std::chrono::milliseconds retry_jitter{20};
  };

  using native_handle_type   = gcs_grpc_native_handle;
  using io_object_type       = gcs_grpc_io_object;
  using device_read_req_type = device_read_req<native_handle_type>;
  using host_read_req_type   = host_read_req<native_handle_type>;

  explicit gcs_grpc_reactor(config cfg);
  ~gcs_grpc_reactor();

  gcs_grpc_reactor(gcs_grpc_reactor const&)            = delete;
  gcs_grpc_reactor& operator=(gcs_grpc_reactor const&) = delete;

  // -- io_reactor_c surface ---------------------------------------------------
  std::size_t host_read(native_handle_type handle,
                        std::size_t offset,
                        std::size_t size,
                        std::uint8_t* dst);
  void host_read_async(host_read_req_type req);
  void host_enqueue_bulk(std::span<host_read_req_type> batch);
  void enqueue_bulk(std::span<device_read_req_type> batch);  // device reads

  void interrupt();
  void shutdown();

  /// gRPC reads have no physical alignment requirement; clip to file size.
  static cudf::io::text::byte_range_info align_to_physical(cudf::io::text::byte_range_info logical,
                                                           std::size_t file_size);

  [[nodiscard]] static bool supports(std::string_view path);

  /// Always throws — io_objects are built via gcs_grpc_ioctx::create_io_object
  /// (needs an instance metadata lookup via the authorizer + channel).
  static std::unique_ptr<gcs_grpc_io_object> create_io_object(std::string path);

  [[nodiscard]] static std::size_t size(native_handle_type handle) noexcept
  {
    return handle->object_size;
  }

  /// GetObject metadata (size + generation) — the HEAD equivalent.
  gcs_grpc_object_state stat_object(std::string_view bucket, std::string_view key);

  [[nodiscard]] std::uint64_t bytes_read_total() const noexcept
  {
    return _bytes_read_total.load(std::memory_order_relaxed);
  }

 private:
  struct impl;                  // hides grpc/proto headers from this TU
  std::unique_ptr<impl> _impl;  // channel, stub, completion-queue worker
  config _cfg;
  std::atomic<std::uint64_t> _bytes_read_total{0};
};

}  // namespace sirius::io::gcs
