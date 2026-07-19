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

#include "io/io_context.hpp"

#include "driver_types.h"
#include "io/prefetching_cache.hpp"

#include <rmm/device_buffer.hpp>

#include <cuda_runtime.h>

#include <cucascade/cuda/event.hpp>

#include <algorithm>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace sirius::io {

sirius_ioctx::sirius_ioctx()  = default;
sirius_ioctx::~sirius_ioctx() = default;

void sirius_ioctx::initialize_cache(buffer_pool& pool,
                                    size_t inflight_budget_chunks,
                                    bool cache_data_reads)
{
  _cache            = std::make_unique<prefetching_cache>(pool, this, inflight_budget_chunks);
  _cache_data_reads = cache_data_reads;
}

namespace {

std::future<size_t> copy_pinned_slices_to_device(
  std::vector<cudf::io::datasource::non_owning_buffer> const& slices,
  uint8_t* dst,
  rmm::cuda_stream_view stream)
{
  // Skip empty slices without touching CUDA.
  size_t n_nonempty =
    std::count_if(slices.begin(), slices.end(), [](auto const& s) { return s.size() > 0; });

  if (n_nonempty == 0) return std::async(std::launch::deferred, []() { return size_t{0}; });

  cucascade::cuda::cuda_event copy_done_event;

  // Fast path: one slice (common after pinned_view::slice coalescing when
  // chunks are contiguous in slab memory). Plain cudaMemcpyAsync avoids the
  // batch-API per-call overhead.
  if (n_nonempty == 1) {
    size_t copied = 0;
    for (auto const& s : slices) {
      if (s.size() == 0) continue;
      auto err =
        cudaMemcpyAsync(dst + copied, s.data(), s.size(), cudaMemcpyHostToDevice, stream.value());
      if (err != cudaSuccess) {
        throw std::runtime_error(std::string("sirius_ioctx: cudaMemcpyAsync failed: ") +
                                 cudaGetErrorString(err));
      }
      copied += s.size();
    }
    copy_done_event.record(stream);
    return std::async(std::launch::deferred, [e = std::move(copy_done_event), copied]() mutable {
      e.synchronize();
      return copied;
    });
  }

  // Build the batch descriptors. Copies within a batch are unordered with
  // respect to each other but the whole batch is stream-ordered; all copies
  // have disjoint destination ranges.
  std::vector<void*> dsts;
  std::vector<void const*> srcs;
  std::vector<size_t> sizes;
  dsts.reserve(n_nonempty);
  srcs.reserve(n_nonempty);
  sizes.reserve(n_nonempty);

  size_t copied = 0;
  for (auto const& s : slices) {
    auto n = s.size();
    if (n == 0) continue;
    dsts.push_back(dst + copied);
    srcs.push_back(s.data());
    sizes.push_back(n);
    copied += n;
  }

#if CUDART_VERSION < 12080
  // No batch API: fall back to sequential cudaMemcpyAsync on the caller's stream.
  size_t total_copied = 0;
  for (size_t i = 0; i < dsts.size(); ++i) {
    auto err = cudaMemcpyAsync(dsts[i], srcs[i], sizes[i], cudaMemcpyHostToDevice, stream.value());
    if (err != cudaSuccess) {
      throw std::runtime_error(std::string("sirius_ioctx: cudaMemcpyAsync failed at idx ") +
                               std::to_string(i) + ": " + cudaGetErrorString(err));
    }
    total_copied += sizes[i];
  }
  copy_done_event.record(stream);
  return std::async(std::launch::deferred,
                    [e = std::move(copy_done_event), total_copied]() mutable {
                      e.synchronize();
                      return total_copied;
                    });
#else
  // Batch API available. It does not support legacy/null streams, so in that
  // case redirect to cudaStreamPerThread. The caller's legacy stream is *not*
  // ordered against the work in that case — the function's contract is that
  // the copy is observed-complete only after future.get()/wait() syncs the
  // event recorded on stream_to_use.
  bool const stream_is_legacy = (stream.value() == nullptr) || (stream.value() == cudaStreamLegacy);
  cudaStream_t stream_to_use  = stream_is_legacy ? cudaStreamPerThread : stream.value();

  cudaMemcpyAttributes attrs{};
  attrs.srcAccessOrder  = cudaMemcpySrcAccessOrderStream;
  attrs.srcLocHint.type = cudaMemLocationTypeHost;
  attrs.dstLocHint.type = cudaMemLocationTypeDevice;
  attrs.flags           = 0;

  size_t attrs_idx = 0;  // attrs applies to all copies -> single entry at index 0
  size_t fail_idx  = 0;  // out-param: driver writes failing copy index on error

#if CUDART_VERSION < 13000
  auto err = cudaMemcpyBatchAsync(dsts.data(),
                                  srcs.data(),
                                  sizes.data(),
                                  n_nonempty,
                                  &attrs,
                                  &attrs_idx,
                                  1,
                                  &fail_idx,
                                  stream_to_use);
#else
  auto err = cudaMemcpyBatchAsync(
    dsts.data(), srcs.data(), sizes.data(), n_nonempty, &attrs, &attrs_idx, 1, stream_to_use);
#endif
  if (err != cudaSuccess) {
    throw std::runtime_error(std::string("sirius_ioctx: cudaMemcpyBatchAsync failed at idx ") +
                             std::to_string(fail_idx) + ": " + cudaGetErrorString(err));
  }
  copy_done_event.record(stream_to_use);
  return std::async(std::launch::deferred, [e = std::move(copy_done_event), copied]() mutable {
    e.synchronize();
    return copied;
  });
#endif
}
}  // namespace

size_t sirius_ioctx::host_read(sirius_io_object& obj, size_t offset, size_t size, uint8_t* dst)
{
  if (_cache) {
    if (auto view = _cache->read(obj, offset, size, nullptr); view) {
      auto slices   = view.slice(offset, size);
      size_t copied = 0;
      for (auto const& s : slices) {
        std::memcpy(dst + copied, s.data(), s.size());
        copied += s.size();
      }
      return copied;
    }
  }
  return host_read_io(obj, offset, size, dst);
}

std::future<size_t> sirius_ioctx::host_read_async(sirius_io_object& obj,
                                                  size_t offset,
                                                  size_t size,
                                                  uint8_t* dst)
{
  if (_cache) {
    if (auto view = _cache->read(obj, offset, size, nullptr); view) {
      auto slices = view.slice(offset, size);
      try {
        size_t copied = 0;
        for (auto const& s : slices) {
          std::memcpy(dst + copied, s.data(), s.size());
          copied += s.size();
        }
        return std::async(std::launch::deferred, [copied]() { return copied; });
      } catch (...) {
        return std::async(std::launch::deferred, [e = std::current_exception()]() -> size_t {
          std::rethrow_exception(e);
        });
      }
    }
  }
  auto promise = std::make_shared<std::promise<size_t>>();
  host_read_async_io(
    obj, offset, size, dst, [promise](size_t bytes_transferred, std::exception_ptr ep) {
      if (ep) {
        promise->set_exception(std::move(ep));
      } else {
        promise->set_value(bytes_transferred);
      }
    });
  return promise->get_future();
}

std::future<size_t> sirius_ioctx::host_read_segments_async(
  sirius_io_object& obj, size_t offset, std::vector<cudf::host_span<std::byte>> segments)
{
  size_t total = 0;
  for (auto const& s : segments)
    total += s.size();

  // Skip the lookup when the cache can never hold chunk data (prewarm off) —
  // this is the per-range hot path of the parquet scan, and the mutex+map
  // probe is pure overhead in that mode. Footer/metadata reads go through
  // host_read/host_read_async, which always consult.
  if (_cache && _cache_data_reads && total > 0) {
    if (auto view = _cache->read(obj, offset, total, nullptr); view) {
      auto slices = view.slice(offset, total);
      try {
        sg_write_cursor cur{segments};
        for (auto const& s : slices) {
          cur.append(s.data(), s.size());
        }
        auto copied = cur.written;
        return std::async(std::launch::deferred, [copied]() { return copied; });
      } catch (...) {
        return std::async(std::launch::deferred, [e = std::current_exception()]() -> size_t {
          std::rethrow_exception(e);
        });
      }
    }
  }

  auto promise = std::make_shared<std::promise<size_t>>();
  host_read_segments_async_io(
    obj, offset, std::move(segments), [promise](size_t bytes_transferred, std::exception_ptr ep) {
      if (ep) {
        promise->set_exception(std::move(ep));
      } else {
        promise->set_value(bytes_transferred);
      }
    });
  return promise->get_future();
}

void sirius_ioctx::host_read_segments_async_io(sirius_io_object& obj,
                                               size_t offset,
                                               std::vector<cudf::host_span<std::byte>> segments,
                                               io_completion_handler handler)
{
  // Generic fallback: one sub-read per destination segment. Backends with a
  // native scatter-gather path override this with a single transport request.
  size_t total = 0;
  for (auto const& s : segments)
    total += s.size();

  size_t n_segs =
    std::count_if(segments.begin(), segments.end(), [](auto const& s) { return s.size() > 0; });
  auto ctx = request_context::create(n_segs, total, std::move(handler));
  if (!ctx) return;

  size_t cur = offset;
  for (auto const& s : segments) {
    if (s.size() == 0) continue;
    host_read_async_io(obj,
                       cur,
                       s.size(),
                       reinterpret_cast<uint8_t*>(s.data()),
                       [ctx](size_t, std::exception_ptr ep) {
                         if (ep) {
                           ctx->chunk_failed(std::move(ep));
                         } else {
                           ctx->chunk_done();
                         }
                       });
    cur += s.size();
  }
}

size_t sirius_ioctx::device_read(
  sirius_io_object& obj, size_t offset, size_t size, uint8_t* dst, rmm::cuda_stream_view stream)
{
  if (_cache) {
    if (auto view = _cache->read(obj, offset, size, stream.value()); view) {
      auto slices = view.slice(offset, size);
      auto copied = copy_pinned_slices_to_device(slices, dst, stream);
      return copied.get();
    }
  }
  return device_read_io(obj, offset, size, dst, stream);
}

std::future<size_t> sirius_ioctx::device_read_async(
  sirius_io_object& obj, size_t offset, size_t size, uint8_t* dst, rmm::cuda_stream_view stream)
{
  if (_cache) {
    if (auto view = _cache->read(obj, offset, size, stream.value()); view) {
      auto slices = view.slice(offset, size);
      try {
        return copy_pinned_slices_to_device(slices, dst, stream);
      } catch (...) {
        return std::async(std::launch::deferred, [e = std::current_exception()]() -> size_t {
          std::rethrow_exception(e);
        });
      }
    }
  }
  auto promise = std::make_shared<std::promise<size_t>>();
  device_read_async_io(
    obj, offset, size, dst, stream, [promise](size_t bytes_transferred, std::exception_ptr ep) {
      if (ep) {
        promise->set_exception(std::move(ep));
      } else {
        promise->set_value(bytes_transferred);
      }
    });
  return promise->get_future();
}

}  // namespace sirius::io
