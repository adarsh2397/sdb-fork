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

/// BidiReadObject usage policy (Rapid Storage multi-range fast path).
enum class gcs_bidi_mode {
  off,       ///< never use BidiReadObject
  on,        ///< always use it; errors surface to the caller
  automatic  ///< probe per bucket; fall back to unary ReadObject when unsupported
};

/**
 * @brief Native GCS gRPC reactor (google.storage.v2 ReadObject/BidiReadObject).
 *
 * Models the @c io_reactor_c concept consumed by @c templated_ioctx, exactly
 * like @c s3_reactor — so the scan path, prefetch cache, and pipeline are
 * unchanged; only the transport differs.
 *
 * Transport architecture (per instance):
 *   - A pool of @c num_channels gRPC channels ("lanes"), each with its own
 *     CompletionQueue + dedicated worker thread. Distinct channel args force
 *     separate TCP connections, removing both the per-channel HTTP/2
 *     concurrent-stream cap and the single-worker deserialize/memcpy ceiling.
 *   - Reads are submitted to lanes round-robin; each lane runs a bounded
 *     in-flight window of @c max_streams / @c num_channels concurrent streams.
 *   - When @c bidi_reads permits, ranges for the same object multiplex over a
 *     persistent @c BidiReadObject stream per (lane, object) — the Rapid
 *     Storage (zonal bucket) fast path. Otherwise each range is a
 *     server-streaming @c ReadObject.
 *   - Transient failures (UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED,
 *     RESOURCE_EXHAUSTED, INTERNAL) retry with exponential backoff + jitter,
 *     resuming at the last delivered byte.
 *
 * Auth reuses the existing @c s3_request_authorizer chain (e.g.
 * @c gcs_metadata_server_authorizer): the bearer token it produces is attached
 * to each RPC as gRPC call credentials rather than an HTTP header.
 */
class gcs_grpc_reactor {
 public:
  struct config {
    std::shared_ptr<sirius::io::s3::s3_request_authorizer> creds;
    std::string endpoint{"storage.googleapis.com"};  ///< gRPC target (DirectPath: google-c2p:///)
    /// When true, build channels for DirectPath: target the c2p resolver
    /// ("google-c2p:///<host>") with GoogleDefaultCredentials (ALTS handshake +
    /// compute-SA auth), bypassing the GFE on a co-located GCE VM. Auth in this
    /// mode is handled by GoogleDefaultCredentials, so @c creds is not used for
    /// the bearer header (it is still required for the non-DirectPath path and
    /// for stat/HEAD construction). Off-GCE this must be false. gRPC auto-falls
    /// back to CFE/TLS if DirectPath cannot be negotiated.
    bool directpath{false};
    long request_timeout_s{60};
    std::size_t max_streams{16};   ///< TOTAL concurrent streams in flight (split across lanes)
    std::size_t num_channels{4};   ///< gRPC channels, each with its own CQ + worker thread
    gcs_bidi_mode bidi_reads{gcs_bidi_mode::automatic};
    /// Large scatter-gather reads are split into sub-reads of this size so
    /// they parallelize across streams/lanes (bidi ranges also obey it).
    std::size_t target_read_bytes{16UL << 20};
    cucascade::memory::fixed_size_host_memory_resource* host_memory_resource{nullptr};
    std::size_t max_retry_attempts{4};
    std::chrono::milliseconds retry_backoff_base{50};
    std::chrono::milliseconds retry_jitter{20};
  };

  /// Cumulative activity counters — one snapshot per reactor. Exposed for the
  /// periodic stats log and tests.
  struct stats_snapshot {
    std::uint64_t bytes_read{0};
    std::uint64_t ranges_completed{0};   ///< logical ranges delivered (unary + bidi)
    std::uint64_t sg_reads{0};           ///< scatter-gather requests accepted
    std::uint64_t unary_streams{0};      ///< ReadObject streams started
    std::uint64_t bidi_sessions{0};      ///< BidiReadObject streams opened
    std::uint64_t bidi_ranges{0};        ///< ranges submitted over bidi sessions
    std::uint64_t bidi_fallbacks{0};     ///< ranges rerouted bidi -> unary
    std::uint64_t retries{0};            ///< retry attempts scheduled
    std::uint64_t retry_exhausted{0};    ///< ranges failed after max retries
    std::uint64_t device_chunks{0};      ///< device-path chunks (staged H2D)
  };

  using native_handle_type    = gcs_grpc_native_handle;
  using io_object_type        = gcs_grpc_io_object;
  using device_read_req_type  = device_read_req<native_handle_type>;
  using host_read_req_type    = host_read_req<native_handle_type>;
  using host_read_sg_req_type = host_read_sg_req<native_handle_type>;

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

  /// Scatter-gather read: ONE contiguous file range delivered into multiple
  /// destination segments (native SG hook consumed by templated_ioctx).
  void host_read_sg_async(host_read_sg_req_type req);

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

  [[nodiscard]] stats_snapshot stats() const noexcept;

 private:
  struct impl;                  // hides grpc/proto headers from this TU
  std::unique_ptr<impl> _impl;  // channel lanes, CQ workers, bidi sessions
  config _cfg;
  std::atomic<std::uint64_t> _bytes_read_total{0};
};

}  // namespace sirius::io::gcs
