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

#include "io/s3/s3_request_authorizer.hpp"

#include <chrono>
#include <mutex>
#include <string>

namespace sirius::io::gcs {

/**
 * @brief GCS authorizer that obtains OAuth2 bearer tokens from the GCE
 *        instance metadata server (no HMAC keys required).
 *
 * On a GCE VM with an attached service account, the metadata server at
 * @c http://metadata.google.internal exposes a token endpoint that returns a
 * short-lived OAuth2 bearer token for the default service account. This
 * authorizer fetches the token on first use and caches it until
 * @c expires_in - @c kRefreshLeadSeconds seconds have elapsed, then
 * re-fetches transparently.
 *
 * @c authorize() returns a plain @c "https://storage.googleapis.com/..."
 * URL with an @c "Authorization: Bearer <token>" header. The
 * @c s3_blocking_ioctx / @c gcs_ioctx HTTP layer attaches the header
 * verbatim to the libcurl request — no other changes required.
 *
 * @par Thread safety
 *   All public methods are safe to call concurrently. The token cache is
 *   protected by a mutex; concurrent callers racing on an expired token
 *   will serialize through a single refresh.
 *
 * @par Construction
 * @code
 *   // Default: service-account=default, GCS global endpoint
 *   auto auth = std::make_shared<sirius::io::gcs::gcs_metadata_server_authorizer>();
 *
 *   // Custom service account or emulator endpoint:
 *   auto auth = std::make_shared<sirius::io::gcs::gcs_metadata_server_authorizer>(
 *       "my-sa@project.iam.gserviceaccount.com",
 *       "https://storage.googleapis.com");
 * @endcode
 */
class gcs_metadata_server_authorizer final : public sirius::io::s3::s3_request_authorizer {
 public:
  /// @param service_account  Service account identifier used in the metadata
  ///        server token URL. Defaults to @c "default" which resolves to the
  ///        VM's primary service account.
  /// @param gcs_endpoint     GCS XML API base URL. Defaults to the global GCS
  ///        endpoint; override for local emulators.
  explicit gcs_metadata_server_authorizer(
    std::string service_account = "default",
    std::string gcs_endpoint    = "https://storage.googleapis.com");

  /// Returns @c "https://storage.googleapis.com/{bucket}/{key}" (or the
  /// configured endpoint) plus @c "Authorization: Bearer <token>". The token
  /// is fetched from the GCE metadata server on first call and refreshed
  /// automatically before expiry. @p timeout is unused (bearer tokens carry
  /// their own server-side expiry).
  ///
  /// @throw sirius::io::credential_error if the metadata server is
  ///        unreachable or returns a non-200 response, or if the token JSON
  ///        cannot be parsed.
  sirius::io::s3::s3_authorized_request authorize(sirius::io::s3::s3_object_ref const& obj,
                                  sirius::io::s3::s3_request_method method,
                                  std::chrono::seconds timeout) override;

 private:
  /// Seconds before token expiry at which we proactively refresh.
  static constexpr long kRefreshLeadSeconds = 60;

  /// Fetch a fresh token from the GCE metadata server and update the cache.
  /// Must be called with @c _mtx held.
  void refresh_token_locked();

  std::string _service_account;
  std::string _gcs_endpoint;  // scheme://host — no trailing slash
  std::string _scheme;        // "https" or "http"
  std::string _host;          // host[:port]

  mutable std::mutex _mtx;
  std::string _token;                                    // cached bearer token
  std::chrono::steady_clock::time_point _token_expiry;  // when the token expires
};

}  // namespace sirius::io::gcs
