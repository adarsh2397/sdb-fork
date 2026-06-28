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

#include "io/gcs/gcs_grpc_reactor.hpp"
#include "io/s3/s3_request_authorizer.hpp"
#include "io/templated_ioctx.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sirius::io::gcs {

/**
 * @brief Native GCS gRPC io context (google.storage.v2 ReadObject).
 *
 * A thin @c templated_ioctx<gcs_grpc_reactor> — the generic plumbing (cache
 * hookup, datasource factory, device chunking) is inherited; only the two
 * URI-facing overrides are GCS-specific:
 *
 *   - @c supports()         — true for @c "gs://" URIs.
 *   - @c create_io_object() — parses @c "gs://bucket/key", does a GetObject
 *                             metadata lookup (size + generation), returns a
 *                             @c gcs_grpc_io_object.
 *
 * Selected over @c gcs_async_ioctx (XML/HTTP) when @c gcs.transport == grpc.
 * On a co-located GCE VM this enables DirectPath (bypassing the GFE) and is the
 * intended path for Rapid (zonal) buckets.
 */
class gcs_grpc_ioctx final : public templated_ioctx<gcs_grpc_reactor> {
 public:
  explicit gcs_grpc_ioctx(gcs_grpc_reactor::config cfg);

  [[nodiscard]] bool supports(std::string_view path) const override;

  std::shared_ptr<sirius_io_object> create_io_object(std::string path) override;

  /// GetObject metadata (size + generation) for @p bucket / @p key.
  gcs_grpc_object_state stat_object(std::string_view bucket, std::string_view key);

 private:
  [[nodiscard]] gcs_grpc_reactor& reactor() noexcept { return *_reactors.front(); }
};

}  // namespace sirius::io::gcs
