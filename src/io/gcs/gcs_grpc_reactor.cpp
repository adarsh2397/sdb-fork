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

#include "io/gcs/gcs_grpc_reactor.hpp"

#include "io/uri_parser.hpp"

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

#include "google/storage/v2/storage.grpc.pb.h"
#include "google/storage/v2/storage.pb.h"

#include <cucascade/memory/fixed_size_host_memory_resource.hpp>

#include <algorithm>
#include <cstring>
#include <future>
#include <stdexcept>
#include <string>
#include <utility>

namespace sirius::io::gcs {

namespace {

namespace v2 = google::storage::v2;

/// Build the gRPC resource path the v2 API expects for a bucket.
std::string bucket_resource(std::string_view bucket)
{
  return "projects/_/buckets/" + std::string(bucket);
}

/// gRPC call-credentials plugin that reuses the existing s3_request_authorizer
/// (e.g. gcs_metadata_server_authorizer) to obtain an OAuth2 bearer token and
/// inject it as the "authorization" metadata header on every RPC. The
/// authorizer already caches + refreshes the token, so this is cheap per call.
class authorizer_call_credentials final : public grpc::MetadataCredentialsPlugin {
 public:
  explicit authorizer_call_credentials(
    std::shared_ptr<sirius::io::s3::s3_request_authorizer> creds)
    : _creds(std::move(creds))
  {
  }

  grpc::Status GetMetadata(grpc::string_ref /*service_url*/,
                           grpc::string_ref /*method_name*/,
                           const grpc::AuthContext& /*channel_auth_context*/,
                           std::multimap<grpc::string, grpc::string>* metadata) override
  {
    // The authorizer is HTTP-shaped; ask it to authorize a GET and pull the
    // Authorization header it produces. obj/bucket are irrelevant for a pure
    // bearer-token authorizer (the metadata-server one ignores them).
    auto authd = _creds->authorize(
      sirius::io::s3::s3_object_ref{"", ""}, sirius::io::s3::s3_request_method::GET, std::chrono::seconds{60});
    for (auto const& [k, v] : authd.headers) {
      std::string lk = k;
      std::transform(lk.begin(), lk.end(), lk.begin(), [](unsigned char c) { return std::tolower(c); });
      if (lk == "authorization") {
        metadata->insert({"authorization", v});
        return grpc::Status::OK;
      }
    }
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                        "gcs_grpc: authorizer returned no Authorization header");
  }

 private:
  std::shared_ptr<sirius::io::s3::s3_request_authorizer> _creds;
};

std::exception_ptr to_exception(grpc::Status const& s, std::string_view what)
{
  return std::make_exception_ptr(std::runtime_error(
    "gcs_grpc: " + std::string(what) + " failed: " + std::to_string(static_cast<int>(s.error_code())) +
    " " + s.error_message()));
}

}  // namespace

// ---------------------------------------------------------------------------
// impl — hides grpc/proto types from the header
// ---------------------------------------------------------------------------
struct gcs_grpc_reactor::impl {
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<v2::Storage::Stub> stub;
  cucascade::memory::fixed_size_host_memory_resource* host_mr{nullptr};
  long timeout_s{60};

  // Minimal worker pool for async host reads. TODO(grpc-backend): replace with
  // a completion-queue-driven async reactor for true pipelining (this synchronous
  // pool mirrors s3_blocking_ioctx's fan-out, not s3_reactor's curl-multi loop).
  // For now std::async per request is sufficient to validate the path.
};

gcs_grpc_reactor::gcs_grpc_reactor(config cfg) : _cfg(std::move(cfg))
{
  _impl           = std::make_unique<impl>();
  _impl->host_mr  = _cfg.host_memory_resource;
  _impl->timeout_s = _cfg.request_timeout_s;

  if (!_cfg.creds) {
    throw std::invalid_argument("gcs_grpc_reactor: creds (authorizer) is required");
  }

  // Composite channel credentials: TLS transport + per-call bearer token.
  // TODO(grpc-backend): for DirectPath (co-located VM + Rapid bucket), target
  // "google-c2p:///storage.googleapis.com" and use GoogleDefaultCredentials so
  // gRPC negotiates ALTS/DirectPath; the SSL path below is the portable
  // fallback that works off-GCE.
  auto call_creds = grpc::MetadataCredentialsFromPlugin(
    std::make_unique<authorizer_call_credentials>(_cfg.creds));
  auto channel_creds =
    grpc::CompositeChannelCredentials(grpc::SslCredentials(grpc::SslCredentialsOptions{}), call_creds);

  std::string target = _cfg.endpoint;
  if (target.find("://") == std::string::npos && target.find(':') == std::string::npos) {
    target += ":443";  // default gRPC TLS port
  }
  _impl->channel = grpc::CreateChannel(target, channel_creds);
  _impl->stub    = v2::Storage::NewStub(_impl->channel);
}

gcs_grpc_reactor::~gcs_grpc_reactor() { shutdown(); }

void gcs_grpc_reactor::interrupt() {}
void gcs_grpc_reactor::shutdown() { /* channel/stub torn down with _impl */ }

cudf::io::text::byte_range_info gcs_grpc_reactor::align_to_physical(
  cudf::io::text::byte_range_info logical, std::size_t file_size)
{
  auto off = std::min<std::size_t>(logical.offset(), file_size);
  auto end = std::min<std::size_t>(logical.offset() + logical.size(), file_size);
  return {static_cast<int64_t>(off), static_cast<int64_t>(end - off)};
}

bool gcs_grpc_reactor::supports(std::string_view path)
{
  auto pos = path.find("://");
  if (pos == std::string_view::npos) return false;
  return path.substr(0, pos) == "gs";
}

std::unique_ptr<gcs_grpc_io_object> gcs_grpc_reactor::create_io_object(std::string)
{
  throw std::logic_error(
    "gcs_grpc_reactor::create_io_object: use gcs_grpc_ioctx::create_io_object "
    "(needs an instance GetObject via the authorizer/channel)");
}

gcs_grpc_object_state gcs_grpc_reactor::stat_object(std::string_view bucket, std::string_view key)
{
  v2::GetObjectRequest req;
  req.set_bucket(bucket_resource(bucket));
  req.set_object(std::string(key));

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(_impl->timeout_s));

  v2::Object obj;
  auto status = _impl->stub->GetObject(&ctx, req, &obj);
  if (!status.ok()) { std::rethrow_exception(to_exception(status, "GetObject")); }

  gcs_grpc_object_state st;
  st.bucket      = std::string(bucket);
  st.key         = std::string(key);
  st.object_size = static_cast<std::size_t>(obj.size());
  st.generation  = obj.generation();
  return st;
}

std::size_t gcs_grpc_reactor::host_read(native_handle_type handle,
                                        std::size_t offset,
                                        std::size_t size,
                                        std::uint8_t* dst)
{
  v2::ReadObjectRequest req;
  req.set_bucket(bucket_resource(handle->bucket));
  req.set_object(handle->key);
  if (handle->generation != 0) { req.set_generation(handle->generation); }
  req.set_read_offset(static_cast<int64_t>(offset));
  req.set_read_limit(static_cast<int64_t>(size));

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(_impl->timeout_s));

  auto reader = _impl->stub->ReadObject(&ctx, req);

  std::size_t written = 0;
  v2::ReadObjectResponse resp;
  while (reader->Read(&resp)) {
    if (resp.has_checksummed_data()) {
      // ChecksummedData.content is `bytes content = 1 [ctype = CORD]` in the GCS
      // v2 proto. The cmake proto step (cmake/gcs_grpc_proto.cmake) strips the
      // `[ctype = CORD]` annotation before protoc runs, so the generated accessor
      // is a plain public `const std::string& content()` — no absl::Cord needed.
      auto const& content = resp.checksummed_data().content();
      auto const n        = std::min<std::size_t>(content.size(), size - written);
      if (n > 0) {
        std::memcpy(dst + written, content.data(), n);
        written += n;
      }
      if (written >= size) break;
    }
  }
  auto status = reader->Finish();
  if (!status.ok()) { std::rethrow_exception(to_exception(status, "ReadObject")); }

  _bytes_read_total.fetch_add(written, std::memory_order_relaxed);
  return written;
}

void gcs_grpc_reactor::host_read_async(host_read_req_type req)
{
  // TODO(grpc-backend): route through a completion-queue reactor for real async.
  // This synchronous-on-a-detached-future shim is correct but not pipelined.
  std::thread([this, req = std::move(req)]() mutable {
    try {
      host_read(req.handle, req.offset, req.size, req.dst);
      if (req.ctx) req.ctx->chunk_done();
    } catch (...) {
      if (req.ctx) req.ctx->chunk_failed(std::current_exception());
    }
  }).detach();
}

void gcs_grpc_reactor::host_enqueue_bulk(std::span<host_read_req_type> batch)
{
  for (auto& r : batch) { host_read_async(r); }
}

void gcs_grpc_reactor::enqueue_bulk(std::span<device_read_req_type> batch)
{
  // Device path: read the range into a pinned staging block, then async H2D.
  // TODO(grpc-backend): match s3_reactor's bounded pinned-staging window +
  // stream-callback completion (no per-chunk cudaStreamSynchronize). This
  // straightforward version reads into staging, copies, and synchronizes per
  // chunk on a worker thread — correct, not yet optimal.
  for (auto& req : batch) {
    auto r = req;  // copy descriptor for the worker
    std::thread([this, r]() mutable {
      try {
        if (_impl->host_mr == nullptr) {
          throw std::runtime_error("gcs_grpc_reactor: device reads require a host memory resource");
        }
        auto staging = _impl->host_mr->allocate_multiple_blocks(r.data_size, nullptr);
        // multiple_blocks_allocation exposes raw block pointers via get_blocks()
        // (std::byte*), not the .at(i)->span API of fixed_multiple_blocks_allocation.
        // r.data_size <= block_size here (single staging block), so block 0 is the
        // contiguous destination.
        auto* host = reinterpret_cast<std::uint8_t*>(staging.get_blocks()[0]);
        host_read(r.handle, r.file_off, r.io_size, host);
        if (r.device_id >= 0) { cudaSetDevice(r.device_id); }
        cudaMemcpyAsync(r.dst, host + r.data_off, r.data_size, cudaMemcpyHostToDevice, r.stream);
        cudaStreamSynchronize(r.stream);
        if (r.ctx) r.ctx->chunk_done();
      } catch (...) {
        if (r.ctx) r.ctx->chunk_failed(std::current_exception());
      }
    }).detach();
  }
}

}  // namespace sirius::io::gcs
