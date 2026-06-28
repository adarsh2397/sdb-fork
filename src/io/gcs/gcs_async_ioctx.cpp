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

#include "io/gcs/gcs_async_ioctx.hpp"

#include "io/s3/s3_ioctx.hpp"
#include "io/s3/s3_reactor.hpp"
#include "io/uri_parser.hpp"

#include <cctype>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

namespace sirius::io::gcs {

gcs_async_ioctx::gcs_async_ioctx(
  std::shared_ptr<sirius::io::s3::s3_request_authorizer> creds,
  long request_timeout_s,
  std::string ca_bundle_path,
  bool tls_verify,
  std::size_t max_connections,
  cucascade::memory::fixed_size_host_memory_resource* host_mr,
  std::optional<std::size_t> max_retry_attempts,
  std::optional<std::chrono::milliseconds> retry_backoff_base,
  std::optional<std::chrono::milliseconds> retry_jitter,
  std::optional<bool> honor_retry_after)
  : s3_ioctx(std::move(creds),
             request_timeout_s,
             std::move(ca_bundle_path),
             tls_verify,
             max_connections,
             host_mr,
             max_retry_attempts,
             retry_backoff_base,
             retry_jitter,
             honor_retry_after)
{
}

bool gcs_async_ioctx::supports(std::string_view path) const
{
  constexpr std::string_view kPrefix = "gs://";
  if (path.size() < kPrefix.size()) return false;
  for (std::size_t i = 0; i < kPrefix.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(path[i])) !=
        static_cast<unsigned char>(kPrefix[i])) {
      return false;
    }
  }
  // supports() must not throw — multi-ioctx dispatch iterates supports() via
  // find_if; a throw would prevent fallback to other backends.
  try {
    auto parsed = sirius::io::parse(path);
    return parsed.scheme == "gs" && !parsed.host.empty() && !parsed.path.empty();
  } catch (std::invalid_argument const&) {
    return false;
  }
}

std::shared_ptr<sirius::io::sirius_io_object> gcs_async_ioctx::create_io_object(std::string path)
{
  auto parsed = sirius::io::parse(path);
  if (parsed.scheme != "gs") {
    throw std::invalid_argument(
      "gcs_async_ioctx::create_io_object: unsupported scheme '" + parsed.scheme +
      "' (expected 'gs')");
  }
  // parsed.host == bucket, parsed.path == object key.
  // Reuse s3_ioctx::head_object_size (calls the reactor's blocking HEAD path
  // which goes through the GCS authorizer to produce the correct HTTPS URL).
  auto size  = head_object_size(parsed.host, parsed.path);
  auto state = std::make_shared<sirius::io::s3::s3_object_state>(
    sirius::io::s3::s3_object_state{std::move(parsed.host), std::move(parsed.path), size});
  // Preserve the gs:// URI as the cache key to avoid cross-backend collisions
  // with any s3:// objects that might share the same bucket+key string.
  return std::make_shared<sirius::io::s3::s3_async_io_object>(std::move(path), std::move(state));
}

}  // namespace sirius::io::gcs
