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
#include <atomic>
#include <cctype>
#include <cstring>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace sirius::io::gcs {

namespace {

namespace v2 = google::storage::v2;

/// Build the gRPC resource path the v2 API expects for a bucket.
std::string bucket_resource(std::string_view bucket)
{
  return "projects/_/buckets/" + std::string(bucket);
}

/// Build the `x-goog-request-params` routing header value the GCS gRPC backend
/// requires for ReadObject/GetObject (their google.api.routing annotation uses
/// path_template "{bucket=**}"). Without this header the server rejects the RPC
/// with INVALID_ARGUMENT ("an x-goog-request-params ... property must be
/// provided"). The value is `bucket=<resource path>` with reserved characters
/// percent-encoded (notably the slashes in "projects/_/buckets/<name>").
/// google-cloud-cpp's generated stubs add this automatically; our hand-written
/// stub must do it explicitly.
std::string routing_params(std::string_view bucket)
{
  static constexpr char kHex[] = "0123456789ABCDEF";
  std::string out             = "bucket=";
  for (char c : bucket_resource(bucket)) {
    auto uc = static_cast<unsigned char>(c);
    if (std::isalnum(uc) || c == '-' || c == '_' || c == '.' || c == '~') {
      out += c;
    } else {
      out += '%';
      out += kHex[uc >> 4];
      out += kHex[uc & 0x0F];
    }
  }
  return out;
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
// read_op — per async ReadObject stream state, driven by the CompletionQueue.
// ---------------------------------------------------------------------------
// One outstanding CQ op at a time, so the op pointer itself is the tag and the
// `state` field tells the worker which step just completed. Lifetime: new'd at
// submit, delete'd when the FINISH completion is handled.
using host_read_req_t = gcs_grpc_reactor::host_read_req_type;

struct read_op {
  grpc::ClientContext ctx;
  v2::ReadObjectRequest grpc_req;
  v2::ReadObjectResponse resp;
  std::unique_ptr<grpc::ClientAsyncReader<v2::ReadObjectResponse>> reader;
  host_read_req_t hr;          // keeps handle alive + carries ctx for chunk_done/failed
  std::uint8_t* dst{nullptr};
  std::size_t size{0};
  std::size_t written{0};
  enum class phase { start, reading, finish } state{phase::start};
  grpc::Status status;
};

// ---------------------------------------------------------------------------
// impl — async CompletionQueue reactor. One channel (HTTP/2 multiplexing), one
// CQ, one worker thread driving up to `max_streams` concurrent ReadObject
// streams. Mirrors s3_reactor's curl-multi loop: a bounded in-flight window
// (max_streams ~ s3_reactor's max_connections), refilled as ops complete.
// ---------------------------------------------------------------------------
struct gcs_grpc_reactor::impl {
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<v2::Storage::Stub> stub;
  cucascade::memory::fixed_size_host_memory_resource* host_mr{nullptr};
  long timeout_s{60};
  std::size_t max_streams{16};
  std::atomic<std::uint64_t>* bytes_counter{nullptr};

  grpc::CompletionQueue cq;
  std::thread worker;

  std::mutex mtx;
  std::deque<read_op*> pending;  // ops awaiting an in-flight slot
  std::size_t inflight{0};
  bool stopping{false};

  void fill_request(read_op* op, host_read_req_t const& hr)
  {
    op->hr      = hr;
    op->dst     = hr.dst;
    op->size    = hr.size;
    op->written = 0;
    op->grpc_req.set_bucket(bucket_resource(hr.handle->bucket));
    op->grpc_req.set_object(hr.handle->key);
    if (hr.handle->generation != 0) { op->grpc_req.set_generation(hr.handle->generation); }
    op->grpc_req.set_read_offset(static_cast<int64_t>(hr.offset));
    op->grpc_req.set_read_limit(static_cast<int64_t>(hr.size));
    op->ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(timeout_s));
    op->ctx.AddMetadata("x-goog-request-params", routing_params(hr.handle->bucket));
  }

  // Initiate the async call. Safe to call from any thread (gRPC associates the
  // call with the CQ); all subsequent Read/Finish steps run on the worker.
  void start_op(read_op* op)
  {
    op->state  = read_op::phase::start;
    op->reader = stub->AsyncReadObject(&op->ctx, op->grpc_req, &cq, op);
  }

  // Submit a host read: start it immediately if under the in-flight cap, else
  // queue it. Called from scan/IO threads via host_read_async.
  void submit(host_read_req_t hr)
  {
    auto* op = new read_op();
    fill_request(op, hr);

    std::unique_lock<std::mutex> lk(mtx);
    if (stopping) {
      lk.unlock();
      if (op->hr.ctx) {
        op->hr.ctx->chunk_failed(
          std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor stopping")));
      }
      delete op;
      return;
    }
    if (inflight < max_streams) {
      ++inflight;
      lk.unlock();
      start_op(op);
    } else {
      pending.push_back(op);
    }
  }

  // Deliver the result, free the op, and pull the next pending op into the
  // freed in-flight slot (mirrors s3_reactor::submit_pending refill).
  void retire(read_op* op)
  {
    if (op->status.ok()) {
      if (bytes_counter) bytes_counter->fetch_add(op->written, std::memory_order_relaxed);
      if (op->hr.ctx) op->hr.ctx->chunk_done();
    } else {
      if (op->hr.ctx) op->hr.ctx->chunk_failed(to_exception(op->status, "ReadObject"));
    }
    delete op;

    std::unique_lock<std::mutex> lk(mtx);
    --inflight;
    if (!stopping && !pending.empty()) {
      auto* next = pending.front();
      pending.pop_front();
      ++inflight;
      lk.unlock();
      start_op(next);
    }
  }

  // Single CQ driver: advances each stream's state machine. Many streams share
  // this one thread — true async pipelining over the multiplexed channel.
  void worker_loop()
  {
    void* tag = nullptr;
    bool ok   = false;
    while (cq.Next(&tag, &ok)) {
      auto* op = static_cast<read_op*>(tag);
      switch (op->state) {
        case read_op::phase::start:
          if (!ok) {  // call failed to start -> reap status
            op->state = read_op::phase::finish;
            op->reader->Finish(&op->status, op);
            break;
          }
          op->state = read_op::phase::reading;
          op->reader->Read(&op->resp, op);
          break;

        case read_op::phase::reading:
          if (ok) {
            if (op->resp.has_checksummed_data()) {
              auto const& content = op->resp.checksummed_data().content();
              auto const n = std::min<std::size_t>(content.size(), op->size - op->written);
              if (n > 0) {
                std::memcpy(op->dst + op->written, content.data(), n);
                op->written += n;
              }
            }
            op->reader->Read(&op->resp, op);  // pull the next message
          } else {                            // stream end -> finish
            op->state = read_op::phase::finish;
            op->reader->Finish(&op->status, op);
          }
          break;

        case read_op::phase::finish:
          retire(op);
          break;
      }
    }
  }
};

gcs_grpc_reactor::gcs_grpc_reactor(config cfg) : _cfg(std::move(cfg))
{
  _impl                = std::make_unique<impl>();
  _impl->host_mr       = _cfg.host_memory_resource;
  _impl->timeout_s     = _cfg.request_timeout_s;
  _impl->max_streams   = std::max<std::size_t>(_cfg.max_streams, 1);
  _impl->bytes_counter = &_bytes_read_total;

  if (!_cfg.creds) {
    throw std::invalid_argument("gcs_grpc_reactor: creds (authorizer) is required");
  }

  std::string target;
  std::shared_ptr<grpc::ChannelCredentials> channel_creds;

  if (_cfg.directpath) {
    // DirectPath: the c2p resolver triggers DirectPath negotiation, and
    // GoogleDefaultCredentials carries the ALTS transport creds + compute-SA
    // auth (so the bearer-plugin is not used in this mode). On a co-located GCE
    // VM this bypasses the GFE; gRPC auto-falls back to CFE/TLS otherwise.
    // endpoint is a bare host here (strip any scheme/port defensively).
    auto host = _cfg.endpoint;
    if (auto p = host.find("://"); p != std::string::npos) host = host.substr(p + 3);
    if (auto c = host.find(':'); c != std::string::npos) host = host.substr(0, c);
    target        = "google-c2p:///" + host;
    channel_creds = grpc::GoogleDefaultCredentials();
  } else {
    // Portable path: TLS transport + per-call bearer token from the authorizer.
    auto call_creds = grpc::MetadataCredentialsFromPlugin(
      std::make_unique<authorizer_call_credentials>(_cfg.creds));
    channel_creds = grpc::CompositeChannelCredentials(
      grpc::SslCredentials(grpc::SslCredentialsOptions{}), call_creds);

    target = _cfg.endpoint;
    if (target.find("://") == std::string::npos && target.find(':') == std::string::npos) {
      target += ":443";  // default gRPC TLS port
    }
  }

  _impl->channel = grpc::CreateChannel(target, channel_creds);
  _impl->stub    = v2::Storage::NewStub(_impl->channel);

  // Start the CQ worker last, once channel + stub are live.
  _impl->worker = std::thread([this]() { _impl->worker_loop(); });
}

gcs_grpc_reactor::~gcs_grpc_reactor() { shutdown(); }

void gcs_grpc_reactor::interrupt() {}

void gcs_grpc_reactor::shutdown()
{
  if (!_impl) return;
  {
    std::lock_guard<std::mutex> lk(_impl->mtx);
    if (_impl->stopping) return;
    _impl->stopping = true;
  }
  // Drain: in-flight ops complete (or hit their deadline) and the worker reaps
  // them; once the CQ is shut down and emptied, cq.Next returns false and the
  // worker exits.
  _impl->cq.Shutdown();
  if (_impl->worker.joinable()) { _impl->worker.join(); }

  // Fail any ops that never got an in-flight slot.
  for (auto* op : _impl->pending) {
    if (op->hr.ctx) {
      op->hr.ctx->chunk_failed(
        std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor shut down")));
    }
    delete op;
  }
  _impl->pending.clear();
}

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
  ctx.AddMetadata("x-goog-request-params", routing_params(bucket));

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
  ctx.AddMetadata("x-goog-request-params", routing_params(handle->bucket));

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
  // Hand off to the CompletionQueue reactor: the read runs as an async
  // ReadObject stream multiplexed over the shared channel, bounded by
  // max_streams, with completion delivered via req.ctx (chunk_done/failed).
  _impl->submit(std::move(req));
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
        // FSMR::allocate_multiple_blocks returns a fixed_multiple_blocks_allocation,
        // a move-only handle whose operator-> exposes at(i) (each a span with
        // .data()) — the same access pattern as s3_reactor's staging. r.data_size
        // <= block_size here (single staging block), so block 0 is the contiguous
        // destination.
        auto* host = reinterpret_cast<std::uint8_t*>(staging->at(0).data());
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
