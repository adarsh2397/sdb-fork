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

#include "io/s3/s3_blocking_io_object.hpp"

#include <cstddef>
#include <string>

namespace sirius::io::gcs {

/**
 * @brief Handle for an object in Google Cloud Storage.
 *
 * Extends @c s3_blocking_io_object with a @c "gs://" cache-id prefix so that
 * the prefetching cache distinguishes GCS objects from S3 objects that happen
 * to live in a bucket of the same name. All other accessors (@c bucket(),
 * @c key(), @c size(), @c object_path()) are inherited unchanged.
 *
 * The cache-id is @c "gs://<bucket>/<key>" — consistent with the URI scheme
 * registered by @c gcs_ioctx and the format that @c parquet_split_provider
 * emits as the datasource path.
 */
class gcs_io_object final : public sirius::io::s3::s3_blocking_io_object {
 public:
  gcs_io_object(std::string bucket, std::string key, std::size_t size, std::string path)
    : s3_blocking_io_object(bucket, key, size, path),
      _cache_id("gs://" + bucket + "/" + key)
  {
  }

  [[nodiscard]] std::string const& raw_file_cache_id() const noexcept override
  {
    return _cache_id;
  }

 private:
  std::string _cache_id;
};

}  // namespace sirius::io::gcs
