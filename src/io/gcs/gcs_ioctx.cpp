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

#include "io/gcs/gcs_ioctx.hpp"

#include "io/gcs/gcs_io_object.hpp"
#include "io/uri_parser.hpp"

#include <cctype>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

namespace sirius::io::gcs {

gcs_ioctx::gcs_ioctx(sirius::io::s3::s3_ioctx_config config)
  : s3_blocking_ioctx(std::move(config))
{
}

bool gcs_ioctx::supports(std::string_view path) const
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

std::shared_ptr<sirius::io::sirius_io_object> gcs_ioctx::create_io_object(std::string path)
{
  auto parsed = sirius::io::parse(path);
  if (parsed.scheme != "gs") {
    throw std::invalid_argument("gcs_ioctx::create_io_object: unsupported scheme '" +
                                parsed.scheme + "' (expected 'gs')");
  }
  // parsed.host == bucket, parsed.path == object key (one separator slash consumed
  // by the parser; any further leading slashes survive per RFC 3986 semantics).
  auto size = head_object_size(parsed.host, parsed.path);
  return std::make_shared<gcs_io_object>(
    std::move(parsed.host), std::move(parsed.path), size, std::move(path));
}

}  // namespace sirius::io::gcs
