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

#include "io/s3/s3_blocking_ioctx.hpp"

#include <memory>
#include <string>
#include <string_view>

namespace sirius::io::gcs {

/**
 * @brief GCS @c sirius_ioctx backed by the S3-compatible XML API (HMAC auth).
 *
 * GCS exposes an S3-compatible HTTP endpoint at @c storage.googleapis.com.
 * Requests are authenticated with the same SigV4 algorithm used for S3, so
 * this class inherits the full @c s3_blocking_ioctx implementation — libcurl
 * handle pool, range GETs, retry/backoff, device-read bounce, async paths,
 * and prefetch-cache hookup — and overrides only the two URI-facing methods:
 *
 *   - @c supports()          — returns @c true for @c "gs://" URIs.
 *   - @c create_io_object()  — parses @c "gs://bucket/key" and constructs a
 *                              @c gcs_io_object (same data as
 *                              @c s3_blocking_io_object but with a
 *                              @c "gs://" cache-id prefix to prevent
 *                              cross-backend key collisions).
 *
 * @par Construction
 *
 * Wire an @c s3_request_authorizer configured for GCS credentials
 * (typically @c sirius_sigv4_presigned_authorizer with endpoint
 * @c "https://storage.googleapis.com" and region @c "auto") into an
 * @c s3_ioctx_config and pass it to this constructor:
 *
 * @code
 *   sirius::io::s3::static_credentials creds;
 *   creds.access_key_id     = "GOOG1E...";
 *   creds.secret_access_key = "...";
 *   auto auth = std::make_shared<
 *     sirius::io::s3::sirius_sigv4_presigned_authorizer>(
 *       std::move(creds), "auto", "https://storage.googleapis.com",
 *       std::chrono::minutes{5});
 *
 *   sirius::io::s3::s3_ioctx_config cfg{};
 *   cfg.creds = std::move(auth);
 *   auto gcs = std::make_shared<sirius::io::gcs::gcs_ioctx>(std::move(cfg));
 *
 *   // Use directly:
 *   auto ds = gcs->open_datasource("gs://my-bucket/data.parquet");
 * @endcode
 *
 * @par SiriusContext wiring
 *
 * @c SiriusContext::initialize() constructs this backend when @c gcs_config
 * has non-empty @c hmac_access_key and @c hmac_secret_key, then pushes it
 * onto the @c borrowed_io_ctxs list so @c sirius_scan_manager dispatches
 * @c "gs://" paths here.
 */
class gcs_ioctx final : public sirius::io::s3::s3_blocking_ioctx {
 public:
  explicit gcs_ioctx(sirius::io::s3::s3_ioctx_config config);

  /// @c true iff @p path has a case-insensitive @c "gs://" prefix and parses
  /// to a non-empty bucket and key. No network call; never throws.
  [[nodiscard]] bool supports(std::string_view path) const override;

  /// Parse @p path as @c "gs://bucket/key", issue a HEAD to obtain the object
  /// size, and return a @c gcs_io_object. Throws @c std::invalid_argument on
  /// non-GCS scheme or malformed URI; throws @c std::runtime_error on HEAD
  /// failure (404, 403, network error).
  std::shared_ptr<sirius::io::sirius_io_object> create_io_object(std::string path) override;
};

}  // namespace sirius::io::gcs
