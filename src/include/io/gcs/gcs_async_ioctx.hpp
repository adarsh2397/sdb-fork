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

#include "io/s3/s3_ioctx.hpp"
#include "io/s3/s3_request_authorizer.hpp"

#include <cucascade/memory/fixed_size_host_memory_resource.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace sirius::io::gcs {

/**
 * @brief Async GCS io context backed by the S3-compatible XML API (libcurl-multi reactor).
 *
 * GCS exposes an S3-compatible HTTP endpoint at @c storage.googleapis.com.
 * This class reuses @c s3_ioctx (and its @c s3_reactor) verbatim — the
 * libcurl-multi event loop, pipelined concurrent range GETs, pinned staging,
 * async H2D copies, retry/backoff — and overrides only the two URI-facing
 * methods:
 *
 *   - @c supports()          — returns @c true for @c "gs://" URIs.
 *   - @c create_io_object()  — parses @c "gs://bucket/key" and constructs an
 *                              @c s3_async_io_object (identical internal
 *                              representation; the @c gs:// URI is preserved as
 *                              the cache key to avoid cross-backend collisions).
 *
 * The @c s3_reactor is auth-agnostic: it calls @c creds->authorize(bucket, key)
 * to obtain a fully-qualified HTTPS URL and @c Authorization header, then issues
 * the curl request. Any @c s3_request_authorizer implementation — HMAC/SigV4
 * (@c sirius_sigv4_presigned_authorizer), OAuth2 bearer token
 * (@c gcs_metadata_server_authorizer), or a static token — works without
 * change.
 *
 * @par Prefer this over @c gcs_ioctx
 *
 * Unlike the blocking @c gcs_ioctx which fans GETs over a caller-supplied
 * @c static_thread_pool, @c gcs_async_ioctx owns a single reactor thread that
 * drives all concurrent GETs via curl_multi. Range GETs for all column chunks
 * in a row group are pipelined without per-chunk synchronisation: each chunk
 * is GET'd into a borrowed pinned staging block, then H2D-copied by a CUDA
 * stream callback, all overlapped. This eliminates the thread-pool overhead and
 * significantly reduces I/O latency for multi-column reads.
 *
 * @par Construction
 *
 * @code
 *   // OAuth2 (GCE metadata server — recommended on GCE VMs):
 *   auto auth = std::make_shared<sirius::io::gcs::gcs_metadata_server_authorizer>();
 *   auto gcs  = std::make_shared<sirius::io::gcs::gcs_async_ioctx>(
 *       std::move(auth),
 *       60,    // request_timeout_s
 *       "",    // ca_bundle_path (use system bundle)
 *       true,  // tls_verify
 *       16,    // max_connections
 *       host_mr);
 * @endcode
 */
class gcs_async_ioctx final : public sirius::io::s3::s3_ioctx {
 public:
  /// Construct an async GCS backend. Parameters mirror @c s3_ioctx exactly;
  /// see that class for documentation. @p creds must produce @c gs:// /
  /// @c storage.googleapis.com URLs and @c Authorization headers suitable for
  /// GCS.
  gcs_async_ioctx(std::shared_ptr<sirius::io::s3::s3_request_authorizer> creds,
                  long request_timeout_s,
                  std::string ca_bundle_path,
                  bool tls_verify,
                  std::size_t max_connections,
                  cucascade::memory::fixed_size_host_memory_resource* host_mr,
                  std::optional<std::size_t> max_retry_attempts               = std::nullopt,
                  std::optional<std::chrono::milliseconds> retry_backoff_base = std::nullopt,
                  std::optional<std::chrono::milliseconds> retry_jitter       = std::nullopt,
                  std::optional<bool> honor_retry_after                       = std::nullopt);

  /// @c true iff @p path has a case-insensitive @c "gs://" prefix and parses
  /// to a non-empty bucket and key. No network call; never throws.
  [[nodiscard]] bool supports(std::string_view path) const override;

  /// Parse @p path as @c "gs://bucket/key", issue a HEAD to obtain the object
  /// size, and return an @c s3_async_io_object with the @c gs:// URI preserved
  /// as the cache key. Throws @c std::invalid_argument on a non-GCS scheme or
  /// malformed URI; throws on HEAD failure (404, 403, network error).
  std::shared_ptr<sirius::io::sirius_io_object> create_io_object(std::string path) override;
};

}  // namespace sirius::io::gcs
