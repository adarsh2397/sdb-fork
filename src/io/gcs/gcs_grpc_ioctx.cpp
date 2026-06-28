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

#include "io/gcs/gcs_grpc_ioctx.hpp"

#include "io/uri_parser.hpp"

#include <cctype>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace sirius::io::gcs {

namespace {
std::vector<std::unique_ptr<gcs_grpc_reactor>> make_reactors(gcs_grpc_reactor::config cfg)
{
  std::vector<std::unique_ptr<gcs_grpc_reactor>> v;
  v.reserve(1);
  v.emplace_back(std::make_unique<gcs_grpc_reactor>(std::move(cfg)));
  return v;
}
}  // namespace

gcs_grpc_ioctx::gcs_grpc_ioctx(gcs_grpc_reactor::config cfg)
  : templated_ioctx<gcs_grpc_reactor>(make_reactors(std::move(cfg)))
{
}

bool gcs_grpc_ioctx::supports(std::string_view path) const
{
  constexpr std::string_view kPrefix = "gs://";
  if (path.size() < kPrefix.size()) return false;
  for (std::size_t i = 0; i < kPrefix.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(path[i])) !=
        static_cast<unsigned char>(kPrefix[i])) {
      return false;
    }
  }
  try {
    auto parsed = sirius::io::parse(path);
    return parsed.scheme == "gs" && !parsed.host.empty() && !parsed.path.empty();
  } catch (std::invalid_argument const&) {
    return false;
  }
}

gcs_grpc_object_state gcs_grpc_ioctx::stat_object(std::string_view bucket, std::string_view key)
{
  return reactor().stat_object(bucket, key);
}

std::shared_ptr<sirius_io_object> gcs_grpc_ioctx::create_io_object(std::string path)
{
  auto parsed = sirius::io::parse(path);
  if (parsed.scheme != "gs") {
    throw std::invalid_argument("gcs_grpc_ioctx::create_io_object: unsupported scheme '" +
                                parsed.scheme + "' (expected 'gs')");
  }
  // parsed.host == bucket, parsed.path == object key.
  auto state = reactor().stat_object(parsed.host, parsed.path);
  auto sp    = std::make_shared<gcs_grpc_object_state>(std::move(state));
  return std::make_shared<gcs_grpc_io_object>(std::move(path), std::move(sp));
}

}  // namespace sirius::io::gcs
