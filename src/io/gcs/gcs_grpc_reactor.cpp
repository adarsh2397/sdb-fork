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
#include "log/logging.hpp"

#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

#include "google/rpc/status.pb.h"
#include "google/storage/v2/storage.grpc.pb.h"
#include "google/storage/v2/storage.pb.h"

#include <cucascade/memory/fixed_size_host_memory_resource.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <optional>
#include <random>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace sirius::io::gcs {

namespace {

namespace v2 = google::storage::v2;

/// Flow-control constants for BidiReadObject sessions (internal; the
/// externally tunable knobs are config.max_streams / num_channels /
/// target_read_bytes).
constexpr std::size_t kBidiMaxOutstandingPerSession = 128;
constexpr std::size_t kMaxRangesPerWrite            = 64;
constexpr std::size_t kMaxSessionsPerLane           = 64;
constexpr auto kStatsLogInterval                    = std::chrono::seconds(10);
/// Rapid Storage may redirect a bidi stream (routing_token handshake) more than
/// once as tokens refresh; cap distinct from hard-failure retries so a genuine
/// "unreachable location" loop still terminates.
constexpr std::size_t kMaxBidiRedirects = 5;

/// Build the gRPC resource path the v2 API expects for a bucket.
std::string bucket_resource(std::string_view bucket)
{
  return "projects/_/buckets/" + std::string(bucket);
}

/// Build the `x-goog-request-params` routing header value the GCS gRPC backend
/// requires for ReadObject/BidiReadObject/GetObject (their google.api.routing
/// annotation uses path_template "{bucket=**}"). Without this header the
/// server rejects the RPC with INVALID_ARGUMENT. The value is
/// `bucket=<resource path>` with reserved characters percent-encoded (notably
/// the slashes in "projects/_/buckets/<name>"). google-cloud-cpp's generated
/// stubs add this automatically; our hand-written stub must do it explicitly.
/// Percent-encode reserved characters (RFC 3986 unreserved set passes through).
std::string percent_encode(std::string_view in)
{
  static constexpr char kHex[] = "0123456789ABCDEF";
  std::string out;
  out.reserve(in.size());
  for (char c : in) {
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

std::string routing_params(std::string_view bucket)
{
  return "bucket=" + percent_encode(bucket_resource(bucket));
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
    auto authd = _creds->authorize(sirius::io::s3::s3_object_ref{"", ""},
                                   sirius::io::s3::s3_request_method::GET,
                                   std::chrono::seconds{60});
    for (auto const& [k, v] : authd.headers) {
      std::string lk = k;
      std::transform(
        lk.begin(), lk.end(), lk.begin(), [](unsigned char c) { return std::tolower(c); });
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
    "gcs_grpc: " + std::string(what) +
    " failed: " + std::to_string(static_cast<int>(s.error_code())) + " " + s.error_message()));
}

/// Transient statuses worth retrying per GCS gRPC guidance. INTERNAL is
/// included because GFE/DirectPath emit it for transient stream resets.
bool is_retriable(grpc::StatusCode code)
{
  switch (code) {
    case grpc::StatusCode::UNAVAILABLE:
    case grpc::StatusCode::DEADLINE_EXCEEDED:
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
    case grpc::StatusCode::ABORTED:
    case grpc::StatusCode::INTERNAL:
      return true;
    default:
      return false;
  }
}

/// Statuses that mean "BidiReadObject is not served here" when they arrive
/// before any data on a fresh session (auto-mode probe failure).
bool is_bidi_unsupported(grpc::StatusCode code)
{
  switch (code) {
    case grpc::StatusCode::UNIMPLEMENTED:
    case grpc::StatusCode::INVALID_ARGUMENT:
    case grpc::StatusCode::FAILED_PRECONDITION:
      return true;
    default:
      return false;
  }
}

/// Rapid Storage redirect handshake: a zonal bucket aborts a fresh
/// BidiReadObject stream and returns a BidiReadObjectRedirectedError detail
/// carrying a routing_token (and usually a read_handle) that the client must
/// echo back when reopening the stream so it is routed to the right location.
/// The detail rides in the `grpc-status-details-bin` trailer, which gRPC
/// surfaces as the serialized google.rpc.Status via error_details().
std::optional<v2::BidiReadObjectRedirectedError> extract_bidi_redirect(grpc::Status const& status)
{
  auto const& details = status.error_details();
  if (details.empty()) return std::nullopt;
  google::rpc::Status rpc_status;
  if (!rpc_status.ParseFromString(details)) return std::nullopt;
  for (auto const& any : rpc_status.details()) {
    v2::BidiReadObjectRedirectedError redirect;
    if (any.UnpackTo(&redirect)) { return redirect; }
  }
  return std::nullopt;
}

std::chrono::milliseconds backoff_for_attempt(std::size_t attempt,
                                              std::chrono::milliseconds base,
                                              std::chrono::milliseconds jitter)
{
  thread_local std::mt19937 rng{std::random_device{}()};
  auto exp = base.count() << std::min<std::size_t>(attempt, 6);  // cap 64x
  auto jit =
    jitter.count() > 0 ? std::uniform_int_distribution<long long>(0, jitter.count())(rng) : 0;
  return std::chrono::milliseconds(exp + jit);
}

const char* bidi_mode_name(gcs_bidi_mode m)
{
  switch (m) {
    case gcs_bidi_mode::off: return "off";
    case gcs_bidi_mode::on: return "on";
    default: return "auto";
  }
}

// ---------------------------------------------------------------------------
// cq_event — common envelope for every CompletionQueue tag. Each lane worker
// does `static_cast<cq_event*>(tag)->on_complete(ok)`, so unary ops, bidi
// session sub-events, backoff alarms and submit kicks all share one loop.
// ---------------------------------------------------------------------------
struct cq_event {
  virtual ~cq_event()               = default;
  virtual void on_complete(bool ok) = 0;
};

// ---------------------------------------------------------------------------
// range_state — one logical range read: destination scatter-gather segments
// plus a persistent write cursor. Survives transport retries (unary
// re-attempts and bidi session restarts resume at offset + written) and
// transport switches (bidi -> unary fallback).
// ---------------------------------------------------------------------------
struct range_state {
  gcs_grpc_native_handle handle;
  std::size_t offset{0};  ///< original file offset
  std::size_t total{0};   ///< total bytes to deliver
  std::vector<cudf::host_span<std::byte>> segments;
  std::shared_ptr<sirius::io::request_context> cctx;
  std::size_t attempts{0};

  std::size_t written{0};
  std::size_t seg_idx{0};
  std::size_t seg_off{0};

  [[nodiscard]] std::size_t remaining() const { return total - written; }
  [[nodiscard]] std::size_t resume_offset() const { return offset + written; }

  /// Append stream-ordered payload across segment boundaries; ignores bytes
  /// past `total` (defensive against over-delivery).
  std::size_t append(void const* src, std::size_t n)
  {
    n                  = std::min(n, remaining());
    auto const* p      = static_cast<std::byte const*>(src);
    std::size_t copied = 0;
    while (copied < n && seg_idx < segments.size()) {
      auto seg          = segments[seg_idx];
      std::size_t avail = seg.size() - seg_off;
      std::size_t take  = std::min(n - copied, avail);
      std::memcpy(seg.data() + seg_off, p + copied, take);
      copied += take;
      seg_off += take;
      if (seg_off == seg.size()) {
        ++seg_idx;
        seg_off = 0;
      }
    }
    written += copied;
    return copied;
  }
};

}  // namespace

// ---------------------------------------------------------------------------
// impl — pool of channel "lanes". Each lane owns one gRPC channel, one
// CompletionQueue and one worker thread; submissions round-robin across lanes.
// All stream operations are initiated on the lane's worker thread (submits
// arrive via a zero-delay alarm "kick"), which makes the bidi Write
// sequencing single-writer by construction.
// ---------------------------------------------------------------------------
struct gcs_grpc_reactor::impl {
  struct lane;

  // -- unary ReadObject op --------------------------------------------------
  struct read_op final : cq_event {
    lane* ln{nullptr};
    std::unique_ptr<range_state> rs;
    std::unique_ptr<grpc::ClientContext> ctx;  // fresh per attempt
    v2::ReadObjectRequest grpc_req;
    v2::ReadObjectResponse resp;
    std::unique_ptr<grpc::ClientAsyncReader<v2::ReadObjectResponse>> reader;
    grpc::Alarm alarm;
    enum class phase { start, reading, finish, backoff } state{phase::start};
    grpc::Status status;

    void on_complete(bool ok) override;
  };

  // -- bidi session ----------------------------------------------------------
  struct bidi_session;

  struct bidi_tag final : cq_event {
    enum kind_t { START, READ, WRITE, FINISH };
    bidi_session* s{nullptr};
    kind_t kind{START};
    void on_complete(bool ok) override;
  };

  struct bidi_session {
    lane* ln{nullptr};
    gcs_grpc_native_handle handle;
    std::string map_key;

    std::unique_ptr<grpc::ClientContext> ctx;
    std::unique_ptr<
      grpc::ClientAsyncReaderWriter<v2::BidiReadObjectRequest, v2::BidiReadObjectResponse>>
      rw;
    v2::BidiReadObjectResponse resp;
    grpc::Status status;

    enum class phase { idle, starting, ready, closing, finishing } state{phase::idle};
    bool read_inflight{false};
    bool write_inflight{false};
    bool counted{false};       ///< occupies an in-flight window slot
    bool saw_response{false};  ///< any response arrived on the CURRENT stream
    std::size_t attempts{0};   ///< stream (re)starts after transient failures
    std::size_t redirects{0};  ///< Rapid Storage routing_token redirects followed

    // Rapid Storage redirect state, echoed on the next stream open so the
    // server routes to the correct location / resumes without revalidation.
    std::string routing_token;              ///< from BidiReadObjectRedirectedError
    std::optional<std::string> read_handle;  ///< bytes; opaque server handle

    std::int64_t next_read_id{1};
    std::deque<std::unique_ptr<range_state>> pending;
    std::unordered_map<std::int64_t, std::unique_ptr<range_state>> outstanding;

    bidi_tag start_tag;
    bidi_tag read_tag;
    bidi_tag write_tag;
    bidi_tag finish_tag;

    [[nodiscard]] bool has_work() const { return !pending.empty() || !outstanding.empty(); }
  };

  // -- submit kick -----------------------------------------------------------
  struct kick_tag final : cq_event {
    lane* ln{nullptr};
    void on_complete(bool ok) override;
  };

  // -- lane ------------------------------------------------------------------
  struct lane {
    impl* owner{nullptr};
    std::size_t index{0};
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<v2::Storage::Stub> stub;
    grpc::CompletionQueue cq;
    std::thread worker;

    // Guards incoming/kick_armed (cross-thread submits) and the live-call
    // registries (so shutdown() can cancel from another thread).
    std::mutex mtx;
    std::deque<std::unique_ptr<range_state>> incoming;
    bool kick_armed{false};
    grpc::Alarm kick_alarm;
    kick_tag kick;

    // Worker-thread state; registry containers additionally guarded by mtx
    // at mutation points (see route_bidi / start_unary / reap paths).
    std::size_t inflight{0};  ///< unary streams + active bidi sessions
    std::size_t window{16};
    std::deque<read_op*> pending_ops;            ///< unary ops awaiting a slot
    std::deque<bidi_session*> sessions_waiting;  ///< sessions with work awaiting a slot
    std::unordered_map<std::string, std::unique_ptr<bidi_session>> sessions;
    std::unordered_map<read_op*, grpc::ClientContext*> live_op_ctxs;
    std::unordered_set<read_op*> backoff_ops;  ///< ops with a pending retry alarm

    void worker_loop()
    {
      void* tag = nullptr;
      bool ok   = false;
      while (cq.Next(&tag, &ok)) {
        static_cast<cq_event*>(tag)->on_complete(ok);
      }
    }

    /// True once every started RPC has been reaped (call under mtx).
    [[nodiscard]] bool drained() const
    {
      if (!live_op_ctxs.empty() || !backoff_ops.empty() || !incoming.empty()) return false;
      for (auto const& [k, s] : sessions) {
        if (s->rw) return false;
      }
      return true;
    }
  };

  // -- device-path op (staged pinned read + async H2D) -----------------------
  struct device_op {
    impl* owner{nullptr};
    device_read_req_type r{};
    cucascade::memory::fixed_size_host_memory_resource::fixed_multiple_blocks_allocation staging;
  };

  /// cudaLaunchHostFunc trampoline: fires the caller's completion and returns
  /// the staging blocks + device slot. Runs on CUDA's internal thread — no
  /// CUDA API calls allowed here.
  static void device_op_done(void* user)
  {
    auto* dop = static_cast<device_op*>(user);
    auto* im  = dop->owner;
    if (dop->r.ctx) dop->r.ctx->chunk_done();
    im->device_chunks.fetch_add(1, std::memory_order_relaxed);
    delete dop;  // frees the staging blocks
    im->device_slots.release();
    im->device_pending.fetch_sub(1, std::memory_order_acq_rel);
  }

  // -- data ------------------------------------------------------------------
  config cfg;
  cucascade::memory::fixed_size_host_memory_resource* host_mr{nullptr};
  std::atomic<std::uint64_t>* bytes_counter{nullptr};

  std::vector<std::unique_ptr<lane>> lanes;
  std::atomic<std::size_t> next_lane{0};
  std::atomic<bool> stopping{false};

  // Bidi availability per bucket: absent = unknown (auto probes), value = known.
  std::mutex bidi_mtx;
  std::unordered_map<std::string, bool> bucket_bidi_ok;
  // Per-bucket routing_token cache (guarded by bidi_mtx). The routing_token is a
  // bucket/zone-level routing hint (NOT per-object — unlike read_handle), so once
  // the first object in a bucket learns it via a redirect, every later session
  // (this query and future queries in-process) seeds its FIRST request with it
  // and skips the redirect handshake entirely. A stale token just triggers one
  // more redirect, which refreshes the cache — self-healing, no TTL needed.
  std::unordered_map<std::string, std::string> bucket_routing_token;

  // Bounded pinned-staging window for the device path.
  std::counting_semaphore<> device_slots{0};
  std::size_t device_window{0};
  std::atomic<std::size_t> device_pending{0};  ///< H2D host-funcs not yet fired

  // Stats (cumulative; snapshot exposed via reactor::stats()).
  std::atomic<std::uint64_t> ranges_completed{0};
  std::atomic<std::uint64_t> sg_reads{0};
  std::atomic<std::uint64_t> unary_streams{0};
  std::atomic<std::uint64_t> bidi_sessions_opened{0};
  std::atomic<std::uint64_t> bidi_ranges{0};
  std::atomic<std::uint64_t> bidi_fallbacks{0};
  std::atomic<std::uint64_t> bidi_redirects{0};
  std::atomic<std::uint64_t> retries{0};
  std::atomic<std::uint64_t> retry_exhausted{0};
  std::atomic<std::uint64_t> device_chunks{0};

  // One-shot feature logs (so a single query run shows which paths are live).
  std::atomic<bool> logged_first_unary{false};
  std::atomic<bool> logged_first_sg{false};
  std::atomic<bool> logged_first_device{false};
  std::atomic<bool> logged_first_redirect{false};
  std::atomic<bool> logged_bidi_needs_directpath{false};

  /// The gRPC target the channels dial ("google-c2p:///..." or host:443);
  /// echoed in routing-failure logs so they self-diagnose CFE-vs-DirectPath.
  std::string channel_target;

  // Periodic throughput log.
  std::thread stats_thread;
  std::mutex stats_mtx;
  std::condition_variable stats_cv;

  bool is_stopping() const { return stopping.load(std::memory_order_relaxed); }

  // ---------------------------------------------------------------------------
  // Submission (any thread)
  // ---------------------------------------------------------------------------

  lane& pick_lane()
  {
    return *lanes[next_lane.fetch_add(1, std::memory_order_relaxed) % lanes.size()];
  }

  /// Enqueue a range onto @p ln and kick its worker via a zero-delay alarm.
  /// The Set happens under the lane mutex so shutdown() (which flips
  /// `stopping` and then acquires each lane mutex) can never observe a Set
  /// racing past its CQ shutdown.
  void enqueue_on(lane& ln, std::unique_ptr<range_state> rs)
  {
    std::unique_lock<std::mutex> lk(ln.mtx);
    if (is_stopping()) {
      lk.unlock();
      fail_range(std::move(rs),
                 std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor stopping")));
      return;
    }
    ln.incoming.push_back(std::move(rs));
    if (!ln.kick_armed) {
      ln.kick_armed = true;
      ln.kick_alarm.Set(&ln.cq, std::chrono::system_clock::now(), &ln.kick);
    }
  }

  void submit_range(std::unique_ptr<range_state> rs)
  {
    if (rs->total == 0) {
      // Guard: read_offset/read_limit of 0 means "to end of object" in the
      // GCS proto — never send a zero-size range to the wire.
      complete_range(std::move(rs));
      return;
    }
    if (is_stopping()) {
      fail_range(std::move(rs),
                 std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor stopping")));
      return;
    }
    enqueue_on(pick_lane(), std::move(rs));
  }

  // ---------------------------------------------------------------------------
  // Worker-side routing (lane worker thread only)
  // ---------------------------------------------------------------------------

  void drain_incoming(lane& ln)
  {
    std::deque<std::unique_ptr<range_state>> work;
    {
      std::lock_guard<std::mutex> lk(ln.mtx);
      work.swap(ln.incoming);
      ln.kick_armed = false;
    }
    for (auto& rs : work) {
      route(ln, std::move(rs));
    }
    pump(ln);
  }

  /// True when this range should go over a bidi session on this lane.
  bool want_bidi(std::string const& bucket)
  {
    if (cfg.bidi_reads == gcs_bidi_mode::off) return false;
    if (cfg.bidi_reads == gcs_bidi_mode::on) return true;
    // auto mode: BidiReadObject on zonal buckets is only reachable over
    // DirectPath — the redirect routing_token is consumed by the client-side
    // c2p/RLS routing stack, which only exists on a DirectPath channel. Via
    // the CFE the reopened stream just re-redirects forever (while unary reads
    // are proxied fine). Don't even probe without DirectPath.
    if (!cfg.directpath) {
      if (!logged_bidi_needs_directpath.exchange(true)) {
        SIRIUS_LOG_WARN(
          "gcs_grpc: bidi_reads=auto but grpc_directpath=false — using unary ReadObject. "
          "BidiReadObject (Rapid fast path) requires DirectPath: set grpc_directpath: true on a "
          "GCE VM co-located with the zonal bucket.");
      }
      return false;
    }
    std::lock_guard<std::mutex> lk(bidi_mtx);
    auto it = bucket_bidi_ok.find(bucket);
    return it == bucket_bidi_ok.end() || it->second;  // unknown -> probe
  }

  void route(lane& ln, std::unique_ptr<range_state> rs)
  {
    if (is_stopping()) {
      fail_range(std::move(rs),
                 std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor stopping")));
      return;
    }
    if (want_bidi(rs->handle->bucket)) {
      route_bidi(ln, std::move(rs));
    } else {
      route_unary(ln, std::move(rs));
    }
  }

  void route_unary(lane& ln, std::unique_ptr<range_state> rs)
  {
    auto* op = new read_op();
    op->ln   = &ln;
    op->rs   = std::move(rs);
    ln.pending_ops.push_back(op);
  }

  void route_bidi(lane& ln, std::unique_ptr<range_state> rs)
  {
    auto key =
      rs->handle->bucket + "/" + rs->handle->key + "@" + std::to_string(rs->handle->generation);
    auto it = ln.sessions.find(key);
    if (it == ln.sessions.end()) {
      if (ln.sessions.size() >= kMaxSessionsPerLane) { evict_idle_session(ln); }
      auto s            = std::make_unique<bidi_session>();
      s->ln             = &ln;
      s->handle         = rs->handle;
      s->map_key        = key;
      // Seed the routing_token from the per-bucket cache so this session's very
      // first request already carries it and skips the redirect handshake. Only
      // the first object in a bucket pays the redirect; everything after (incl.
      // subsequent queries) starts pre-routed.
      {
        std::lock_guard<std::mutex> lk(bidi_mtx);
        if (auto it = bucket_routing_token.find(rs->handle->bucket);
            it != bucket_routing_token.end()) {
          s->routing_token = it->second;
        }
      }
      s->start_tag.s    = s.get();
      s->start_tag.kind = bidi_tag::START;
      s->read_tag.s     = s.get();
      s->read_tag.kind  = bidi_tag::READ;
      s->write_tag.s    = s.get();
      s->write_tag.kind = bidi_tag::WRITE;
      s->finish_tag.s   = s.get();
      s->finish_tag.kind = bidi_tag::FINISH;
      {
        std::lock_guard<std::mutex> lk(ln.mtx);
        it = ln.sessions.emplace(key, std::move(s)).first;
      }
    }
    auto* s = it->second.get();
    s->pending.push_back(std::move(rs));
    bidi_ranges.fetch_add(1, std::memory_order_relaxed);
    if (!s->counted && (s->state == bidi_session::phase::idle ||
                        s->state == bidi_session::phase::ready)) {
      ln.sessions_waiting.push_back(s);
    }
  }

  void evict_idle_session(lane& ln)
  {
    for (auto& [k, s] : ln.sessions) {
      if (!s->counted && !s->has_work() && s->state == bidi_session::phase::ready) {
        // Cancel; the posted Read fails, FINISH reaps and erases the session.
        s->state = bidi_session::phase::closing;
        s->ctx->TryCancel();
        return;
      }
    }
  }

  /// Start queued work while the lane window has room. Sessions get slots
  /// before unary ops (they carry many ranges per slot). No new stream work
  /// is initiated once shutdown began.
  void pump(lane& ln)
  {
    if (is_stopping()) return;
    while (ln.inflight < ln.window &&
           (!ln.sessions_waiting.empty() || !ln.pending_ops.empty())) {
      if (!ln.sessions_waiting.empty()) {
        auto* s = ln.sessions_waiting.front();
        ln.sessions_waiting.pop_front();
        if (s->counted || !s->has_work()) continue;
        s->counted = true;
        ++ln.inflight;
        if (s->state == bidi_session::phase::idle) {
          start_session(*s);
        } else if (s->state == bidi_session::phase::ready) {
          pump_session_writes(*s);
        }
        continue;
      }
      auto* op = ln.pending_ops.front();
      ln.pending_ops.pop_front();
      ++ln.inflight;
      start_unary(op);
    }
  }

  void release_slot(lane& ln)
  {
    --ln.inflight;
    pump(ln);
  }

  // ---------------------------------------------------------------------------
  // Unary ReadObject state machine
  // ---------------------------------------------------------------------------

  void start_unary(read_op* op)
  {
    auto& rs = *op->rs;
    op->ctx  = std::make_unique<grpc::ClientContext>();
    op->ctx->set_deadline(std::chrono::system_clock::now() +
                          std::chrono::seconds(cfg.request_timeout_s));
    op->ctx->AddMetadata("x-goog-request-params", routing_params(rs.handle->bucket));

    op->grpc_req.Clear();
    op->grpc_req.set_bucket(bucket_resource(rs.handle->bucket));
    op->grpc_req.set_object(rs.handle->key);
    if (rs.handle->generation != 0) { op->grpc_req.set_generation(rs.handle->generation); }
    op->grpc_req.set_read_offset(static_cast<int64_t>(rs.resume_offset()));
    op->grpc_req.set_read_limit(static_cast<int64_t>(rs.remaining()));

    {
      std::lock_guard<std::mutex> lk(op->ln->mtx);
      op->ln->live_op_ctxs[op] = op->ctx.get();
    }

    unary_streams.fetch_add(1, std::memory_order_relaxed);
    if (!logged_first_unary.exchange(true)) {
      SIRIUS_LOG_INFO(
        "gcs_grpc: unary ReadObject path active (first stream: {} bytes of gs://{}/{})",
        rs.remaining(),
        rs.handle->bucket,
        rs.handle->key);
    }

    op->state  = read_op::phase::start;
    op->reader = op->ln->stub->AsyncReadObject(op->ctx.get(), op->grpc_req, &op->ln->cq, op);
  }

  void handle_unary_event(read_op* op, bool ok)
  {
    auto& ln = *op->ln;
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
            op->rs->append(content.data(), content.size());
          }
          op->reader->Read(&op->resp, op);  // pull the next message
        } else {                            // stream end -> finish
          op->state = read_op::phase::finish;
          op->reader->Finish(&op->status, op);
        }
        break;

      case read_op::phase::finish: {
        {
          std::lock_guard<std::mutex> lk(ln.mtx);
          ln.live_op_ctxs.erase(op);
        }
        bool const stop = is_stopping();
        if (op->status.ok() || (op->rs->remaining() == 0 && op->rs->written > 0)) {
          complete_range(std::move(op->rs));
          delete op;
          release_slot(ln);
        } else if (!stop && is_retriable(op->status.error_code()) &&
                   op->rs->attempts < cfg.max_retry_attempts) {
          ++op->rs->attempts;
          retries.fetch_add(1, std::memory_order_relaxed);
          auto delay =
            backoff_for_attempt(op->rs->attempts, cfg.retry_backoff_base, cfg.retry_jitter);
          SIRIUS_LOG_WARN(
            "gcs_grpc: retrying ReadObject gs://{}/{} (attempt {}/{}, status={} {}, resume at "
            "+{} of {} bytes, backoff {}ms)",
            op->rs->handle->bucket,
            op->rs->handle->key,
            op->rs->attempts,
            cfg.max_retry_attempts,
            static_cast<int>(op->status.error_code()),
            op->status.error_message(),
            op->rs->written,
            op->rs->total,
            delay.count());
          op->state = read_op::phase::backoff;
          op->reader.reset();
          op->ctx.reset();
          {
            std::lock_guard<std::mutex> lk(ln.mtx);
            ln.backoff_ops.insert(op);
            op->alarm.Set(&ln.cq, std::chrono::system_clock::now() + delay, op);
          }
          // Release the stream slot during backoff so healthy work proceeds.
          release_slot(ln);
        } else {
          if (!op->status.ok() && op->rs->attempts >= cfg.max_retry_attempts) {
            retry_exhausted.fetch_add(1, std::memory_order_relaxed);
            SIRIUS_LOG_ERROR("gcs_grpc: ReadObject gs://{}/{} failed after {} attempts: {} {}",
                             op->rs->handle->bucket,
                             op->rs->handle->key,
                             op->rs->attempts,
                             static_cast<int>(op->status.error_code()),
                             op->status.error_message());
          }
          fail_range(std::move(op->rs), to_exception(op->status, "ReadObject"));
          delete op;
          release_slot(ln);
        }
        break;
      }

      case read_op::phase::backoff: {
        {
          std::lock_guard<std::mutex> lk(ln.mtx);
          ln.backoff_ops.erase(op);
        }
        if (!ok || is_stopping()) {  // alarm canceled / shutdown
          fail_range(std::move(op->rs),
                     std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor shut down")));
          delete op;
          break;
        }
        // Re-enter the lane queue; pump() assigns a fresh slot.
        ln.pending_ops.push_front(op);
        pump(ln);
        break;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // BidiReadObject session state machine (Rapid Storage fast path)
  // ---------------------------------------------------------------------------

  void start_session(bidi_session& s)
  {
    s.ctx = std::make_unique<grpc::ClientContext>();
    // Long-lived multi-range stream: no per-call deadline (channel keepalive
    // detects dead peers); ranges themselves are retried on stream failure.
    //
    // Rapid Storage redirect: a pending routing_token must be echoed in BOTH
    // the x-goog-request-params header AND BidiReadObjectSpec.routing_token
    // (set on the first write, see pump_session_writes). Spec-only does NOT
    // work: without the token in the routing header the DirectPath/c2p layer
    // keeps routing the reopened stream to the same backend, which redirects
    // again (observed: redirects_so_far hits the cap and fails).
    //
    // CRITICAL: the routing_token is appended RAW — it must NOT be
    // percent-encoded, even though the bucket param is. google/java-storage has
    // an explicit test (`redirectTokenMustNotBeUrlEncoded`) asserting the
    // server-issued token is passed through verbatim; encoding it (a 64-char
    // token typically carries base64 '+' '/' '=') corrupts routing and the
    // stream re-redirects forever. The bucket stays percent-encoded (proven
    // accepted — unary ReadObject uses the same routing_params and works).
    // Build x-goog-request-params EXACTLY like google-cloud-cpp's OpenObject
    // RequestParams() (storage/internal/async/open_object.cc): BOTH the bucket
    // and the routing_token are RAW — NOT percent-encoded. That file emits
    //   absl::StrCat("bucket=", read_spec.bucket(), "&routing_token=", token)
    // i.e. `bucket=projects/_/buckets/<name>&routing_token=<token>` with literal
    // slashes. This is the hand-written bidi-read open path and is what the
    // zonal server's redirect routing expects. Percent-encoding the bucket (as
    // routing_params() does for the unary path, which the server tolerates
    // there) makes the redirect loop forever. Do NOT switch this to
    // routing_params().
    std::string params = "bucket=" + bucket_resource(s.handle->bucket);
    if (!s.routing_token.empty()) { params += "&routing_token=" + s.routing_token; }
    // Ground-truth wire dump on any tokened (re)open: the EXACT x-goog-request-
    // params value the reopened stream carries, so we can compare byte-for-byte
    // against what a working client sends instead of guessing at placement.
    if (!s.routing_token.empty()) {
      SIRIUS_LOG_INFO(
        "gcs_grpc: bidi (re)open gs://{}/{} redirect#{} x-goog-request-params=[{}] "
        "(routing_token {} bytes raw, read_handle {})",
        s.handle->bucket,
        s.handle->key,
        s.redirects,
        params,
        s.routing_token.size(),
        s.read_handle ? "set" : "unset");
    }
    s.ctx->AddMetadata("x-goog-request-params", params);
    s.saw_response = false;
    s.state        = bidi_session::phase::starting;
    s.rw           = s.ln->stub->PrepareAsyncBidiReadObject(s.ctx.get(), &s.ln->cq);
    s.rw->StartCall(&s.start_tag);
    bidi_sessions_opened.fetch_add(1, std::memory_order_relaxed);
  }

  /// Move pending ranges into a BidiReadObjectRequest (respecting flow
  /// control) and issue the Write. First write on a stream carries the
  /// read_object_spec.
  void pump_session_writes(bidi_session& s)
  {
    if (is_stopping()) return;
    if (s.state != bidi_session::phase::ready || s.write_inflight) return;
    if (s.pending.empty() || s.outstanding.size() >= kBidiMaxOutstandingPerSession) return;

    v2::BidiReadObjectRequest req;
    if (s.next_read_id == 1) {  // first write on this stream
      auto* spec = req.mutable_read_object_spec();
      spec->set_bucket(bucket_resource(s.handle->bucket));
      spec->set_object(s.handle->key);
      if (s.handle->generation != 0) { spec->set_generation(s.handle->generation); }
      // Echo redirect state so the server routes to the correct location and
      // resumes the read without re-validating (Rapid Storage handshake).
      if (!s.routing_token.empty()) { spec->set_routing_token(s.routing_token); }
      if (s.read_handle) { spec->mutable_read_handle()->set_handle(*s.read_handle); }
    }
    std::size_t n = 0;
    while (!s.pending.empty() && n < kMaxRangesPerWrite &&
           s.outstanding.size() < kBidiMaxOutstandingPerSession) {
      auto rs = std::move(s.pending.front());
      s.pending.pop_front();
      auto rid = s.next_read_id++;
      auto* rr = req.add_read_ranges();
      rr->set_read_offset(static_cast<int64_t>(rs->resume_offset()));
      rr->set_read_length(static_cast<int64_t>(rs->remaining()));
      rr->set_read_id(rid);
      s.outstanding.emplace(rid, std::move(rs));
      ++n;
    }
    if (n == 0) return;
    s.write_inflight = true;
    s.rw->Write(req, &s.write_tag);
  }

  /// Issue Finish once no read/write is outstanding on a broken stream.
  void maybe_finish(bidi_session& s)
  {
    if (s.state == bidi_session::phase::closing && !s.read_inflight && !s.write_inflight) {
      s.state = bidi_session::phase::finishing;
      s.rw->Finish(&s.status, &s.finish_tag);
    }
  }

  void handle_bidi_event(bidi_session& s, bidi_tag::kind_t kind, bool ok)
  {
    auto& ln = *s.ln;
    switch (kind) {
      case bidi_tag::START:
        if (!ok || is_stopping()) {
          s.state = bidi_session::phase::finishing;
          s.rw->Finish(&s.status, &s.finish_tag);
          return;
        }
        s.state = bidi_session::phase::ready;
        // Keep exactly one Read outstanding for the stream's lifetime.
        s.read_inflight = true;
        s.rw->Read(&s.resp, &s.read_tag);
        pump_session_writes(s);
        return;

      case bidi_tag::WRITE:
        s.write_inflight = false;
        if (!ok) {
          // Stream broken; the outstanding Read fails next, then FINISH reaps.
          s.state = bidi_session::phase::closing;
          maybe_finish(s);
          return;
        }
        pump_session_writes(s);
        return;

      case bidi_tag::READ:
        if (!ok) {
          s.read_inflight = false;
          if (s.state != bidi_session::phase::finishing) {
            s.state = bidi_session::phase::closing;
          }
          maybe_finish(s);
          return;
        }
        if (!s.saw_response) {
          s.saw_response = true;
          if (cfg.bidi_reads == gcs_bidi_mode::automatic) {
            mark_bidi_supported(s.handle->bucket);
          }
        }
        for (auto const& rd : s.resp.object_data_ranges()) {
          if (!rd.has_read_range()) continue;
          auto rid = rd.read_range().read_id();
          auto it  = s.outstanding.find(rid);
          if (it == s.outstanding.end()) continue;  // stale/duplicated range data
          if (rd.has_checksummed_data()) {
            auto const& content = rd.checksummed_data().content();
            it->second->append(content.data(), content.size());
          }
          if (rd.range_end() || it->second->remaining() == 0) {
            complete_range(std::move(it->second));
            s.outstanding.erase(it);
          }
        }
        s.resp.Clear();
        if (is_stopping()) {
          s.read_inflight = false;
          s.state         = bidi_session::phase::closing;
          maybe_finish(s);
          return;
        }
        s.rw->Read(&s.resp, &s.read_tag);
        pump_session_writes(s);
        // All ranges served and nothing queued: give the window slot back but
        // keep the stream open for the next task hitting this object.
        if (s.counted && !s.has_work()) {
          s.counted = false;
          release_slot(ln);
        }
        return;

      case bidi_tag::FINISH:
        // Finish is only ever posted with no read/write outstanding, so this
        // is terminal for the current stream.
        reap_session(s);  // may destroy s — nothing after this
        return;
    }
  }

  /// Tear down the current stream so the session can be reopened (retry or
  /// redirect). Leaves pending/outstanding untouched — the caller requeues.
  void reset_session_stream(bidi_session& s)
  {
    s.rw.reset();
    s.ctx.reset();
    s.state         = bidi_session::phase::idle;
    s.next_read_id  = 1;
    s.read_inflight = s.write_inflight = false;
  }

  /// Terminal handling once Finish completed: follow a Rapid Storage redirect,
  /// retry, fall back or fail all ranges the session still holds; erase the
  /// session unless it restarts.
  void reap_session(bidi_session& s)
  {
    auto& ln        = *s.ln;
    bool const stop = is_stopping();

    // Collect every incomplete range (outstanding resume mid-way).
    std::deque<std::unique_ptr<range_state>> leftovers;
    for (auto& [rid, rs] : s.outstanding) {
      leftovers.push_back(std::move(rs));
    }
    s.outstanding.clear();
    while (!s.pending.empty()) {
      leftovers.push_back(std::move(s.pending.front()));
      s.pending.pop_front();
    }

    if (s.counted) {
      s.counted = false;
      --ln.inflight;  // release without pumping yet; pump at the end
    }

    bool const probe_failed = !s.saw_response && cfg.bidi_reads == gcs_bidi_mode::automatic &&
                              is_bidi_unsupported(s.status.error_code());

    // Rapid Storage redirect handshake: a zonal bucket aborts the stream with a
    // routing_token / read_handle we must echo back on reopen so it routes to
    // the correct location. This is the expected path for Rapid buckets — check
    // it before the generic retriable-restart branch (ABORTED is retriable, but
    // restarting WITHOUT the token just re-aborts).
    // NOTE: routing_token is read via routing_token()/empty() rather than a
    // has_ accessor — it is a plain proto3 string and generates no has_ method.
    // read_handle is a message field, so has_read_handle() is always available.
    std::optional<v2::BidiReadObjectRedirectedError> redirect;
    if (!stop && !leftovers.empty()) { redirect = extract_bidi_redirect(s.status); }
    bool const follow_redirect =
      redirect.has_value() &&
      (!redirect->routing_token().empty() || redirect->has_read_handle()) &&
      s.redirects < kMaxBidiRedirects;

    // Decisive diagnostic for the "not available from this location" case: was
    // the redirect detail actually present + parsed, and what did it carry?
    // detail_bytes==0 => server sent no grpc-status-details-bin (parse can't
    // help — look at trailing metadata); parsed=0 with detail_bytes>0 => the
    // detail is a different Any type than BidiReadObjectRedirectedError.
    if (!s.status.ok()) {
      SIRIUS_LOG_INFO(
        "gcs_grpc: bidi stream ended gs://{}/{} status={} \"{}\" saw_response={} "
        "detail_bytes={} redirect_parsed={} routing_token_len={} read_handle_present={} "
        "redirects_so_far={}",
        s.handle->bucket,
        s.handle->key,
        static_cast<int>(s.status.error_code()),
        s.status.error_message(),
        s.saw_response,
        s.status.error_details().size(),
        redirect.has_value(),
        redirect.has_value() ? redirect->routing_token().size() : 0,
        redirect.has_value() && redirect->has_read_handle(),
        s.redirects);
    }

    if (stop) {
      for (auto& rs : leftovers) {
        fail_range(std::move(rs),
                   std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor shut down")));
      }
    } else if (follow_redirect) {
      ++s.redirects;
      bidi_redirects.fetch_add(1, std::memory_order_relaxed);
      if (!redirect->routing_token().empty()) {
        s.routing_token = redirect->routing_token();
        // Publish to the per-bucket cache so future sessions (this query and
        // later ones) skip the redirect by seeding this token up front.
        std::lock_guard<std::mutex> lk(bidi_mtx);
        bucket_routing_token[s.handle->bucket] = s.routing_token;
      }
      if (redirect->has_read_handle()) { s.read_handle = redirect->read_handle().handle(); }
      if (!logged_first_redirect.exchange(true)) {
        SIRIUS_LOG_INFO(
          "gcs_grpc: Rapid Storage redirect handshake active for bucket={} — reopening "
          "BidiReadObject with routing_token (this is expected for zonal buckets)",
          s.handle->bucket);
      }
      SIRIUS_LOG_INFO(
        "gcs_grpc: following BidiReadObject redirect for gs://{}/{} (redirect {}/{}, "
        "routing_token={}, read_handle={}, {} ranges resuming)",
        s.handle->bucket,
        s.handle->key,
        s.redirects,
        kMaxBidiRedirects,
        s.routing_token.empty() ? "<none>" : "<set>",
        s.read_handle ? "<set>" : "<none>",
        leftovers.size());
      reset_session_stream(s);
      for (auto& rs : leftovers) {
        s.pending.push_back(std::move(rs));  // resume offsets preserved; not a failure
      }
      ln.sessions_waiting.push_back(&s);
      pump(ln);
      return;  // session stays in the map
    } else if (probe_failed) {
      {
        std::lock_guard<std::mutex> lk(bidi_mtx);
        bucket_bidi_ok[s.handle->bucket] = false;
      }
      SIRIUS_LOG_WARN(
        "gcs_grpc: BidiReadObject unavailable for bucket={} (status={} {}); falling back to "
        "unary ReadObject ({} ranges rerouted)",
        s.handle->bucket,
        static_cast<int>(s.status.error_code()),
        s.status.error_message(),
        leftovers.size());
      bidi_fallbacks.fetch_add(leftovers.size(), std::memory_order_relaxed);
      for (auto& rs : leftovers) {
        route_unary(ln, std::move(rs));
      }
    } else if (!leftovers.empty() &&
               (s.status.ok() || is_retriable(s.status.error_code())) &&
               s.attempts < cfg.max_retry_attempts) {
      // Stream ended (server rotation or transient error) with ranges still
      // in flight: restart the stream and resume the ranges.
      ++s.attempts;
      if (!s.status.ok()) {
        retries.fetch_add(1, std::memory_order_relaxed);
        SIRIUS_LOG_WARN(
          "gcs_grpc: bidi session gs://{}/{} broke (status={} {}); restart {}/{} with {} ranges",
          s.handle->bucket,
          s.handle->key,
          static_cast<int>(s.status.error_code()),
          s.status.error_message(),
          s.attempts,
          cfg.max_retry_attempts,
          leftovers.size());
      }
      reset_session_stream(s);
      for (auto& rs : leftovers) {
        if (!s.status.ok()) ++rs->attempts;
        s.pending.push_back(std::move(rs));
      }
      ln.sessions_waiting.push_back(&s);
      pump(ln);
      return;  // session stays in the map
    } else if (!leftovers.empty()) {
      // A redirect we couldn't resolve (loop hit the cap, or no token) means
      // the reopened stream never lands on a machine that can serve the zonal
      // object. The routing_token is honored by the client-side DirectPath
      // c2p/RLS routing stack — if the channel is actually talking to the CFE
      // (grpc_directpath=false, or silent c2p fallback), every reopen goes
      // back to the CFE and re-redirects while unary reads work fine.
      bool const location_issue = redirect.has_value() || s.redirects > 0 ||
                                  s.status.error_code() == grpc::StatusCode::ABORTED;
      retry_exhausted.fetch_add(leftovers.size(), std::memory_order_relaxed);
      if (location_issue) {
        SIRIUS_LOG_ERROR(
          "gcs_grpc: BidiReadObject gs://{}/{} unresolvable redirect loop (status={} {}, "
          "redirects_followed={}, directpath={}, target={}). The routing_token is honored by the "
          "client-side DirectPath xDS/RLS routing — if that stack is not engaging in THIS process "
          "the reopen keeps landing on the same backend. Verify with tools/gcs_bidi_probe and "
          "GRPC_TRACE=google_c2p_resolver,rls_lb,xds_client GRPC_VERBOSITY=DEBUG. ({} ranges "
          "failed)",
          s.handle->bucket,
          s.handle->key,
          static_cast<int>(s.status.error_code()),
          s.status.error_message(),
          s.redirects,
          cfg.directpath,
          channel_target,
          leftovers.size());
      } else {
        SIRIUS_LOG_ERROR("gcs_grpc: bidi session gs://{}/{} failed terminally: {} {} ({} ranges)",
                         s.handle->bucket,
                         s.handle->key,
                         static_cast<int>(s.status.error_code()),
                         s.status.error_message(),
                         leftovers.size());
      }
      for (auto& rs : leftovers) {
        fail_range(std::move(rs), to_exception(s.status, "BidiReadObject"));
      }
    }

    // Erase the session (destroys `s` — done last, nothing may touch it
    // after). Drop any stale wait-queue entries first so pump() never
    // dereferences the dead session.
    ln.sessions_waiting.erase(
      std::remove(ln.sessions_waiting.begin(), ln.sessions_waiting.end(), &s),
      ln.sessions_waiting.end());
    auto key = s.map_key;
    {
      std::lock_guard<std::mutex> lk(ln.mtx);
      ln.sessions.erase(key);
    }
    pump(ln);
  }

  void mark_bidi_supported(std::string const& bucket)
  {
    bool log_it = false;
    {
      std::lock_guard<std::mutex> lk(bidi_mtx);
      auto [it, inserted] = bucket_bidi_ok.emplace(bucket, true);
      log_it              = inserted || !it->second;
      it->second          = true;
    }
    if (log_it) {
      SIRIUS_LOG_INFO(
        "gcs_grpc: BidiReadObject ACTIVE for bucket={} (Rapid Storage multi-range fast path)",
        bucket);
    }
  }

  // ---------------------------------------------------------------------------
  // Completion + failure
  // ---------------------------------------------------------------------------

  void complete_range(std::unique_ptr<range_state> rs)
  {
    if (bytes_counter) bytes_counter->fetch_add(rs->written, std::memory_order_relaxed);
    ranges_completed.fetch_add(1, std::memory_order_relaxed);
    if (rs->cctx) rs->cctx->chunk_done();
  }

  void fail_range(std::unique_ptr<range_state> rs, std::exception_ptr ep)
  {
    if (rs && rs->cctx) rs->cctx->chunk_failed(std::move(ep));
  }

  // ---------------------------------------------------------------------------
  // Periodic stats log — makes transport behaviour verifiable per query run.
  // ---------------------------------------------------------------------------

  void stats_loop()
  {
    std::uint64_t last_bytes = 0;
    auto last_time           = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lk(stats_mtx);
    while (!is_stopping()) {
      stats_cv.wait_for(lk, kStatsLogInterval);
      if (is_stopping()) break;
      auto now   = std::chrono::steady_clock::now();
      auto bytes = bytes_counter ? bytes_counter->load(std::memory_order_relaxed) : 0;
      if (bytes == last_bytes) {
        last_time = now;
        continue;  // idle — don't spam the log
      }
      auto secs = std::chrono::duration<double>(now - last_time).count();
      auto mib  = static_cast<double>(bytes - last_bytes) / (1024.0 * 1024.0);
      SIRIUS_LOG_INFO(
        "gcs_grpc stats: +{:.1f} MiB ({:.1f} MiB/s) | totals: bytes={} ranges={} sg_reads={} "
        "unary_streams={} bidi_sessions={} bidi_ranges={} bidi_fallbacks={} bidi_redirects={} "
        "retries={} retry_exhausted={} device_chunks={}",
        mib,
        secs > 0 ? mib / secs : 0.0,
        bytes,
        ranges_completed.load(std::memory_order_relaxed),
        sg_reads.load(std::memory_order_relaxed),
        unary_streams.load(std::memory_order_relaxed),
        bidi_sessions_opened.load(std::memory_order_relaxed),
        bidi_ranges.load(std::memory_order_relaxed),
        bidi_fallbacks.load(std::memory_order_relaxed),
        bidi_redirects.load(std::memory_order_relaxed),
        retries.load(std::memory_order_relaxed),
        retry_exhausted.load(std::memory_order_relaxed),
        device_chunks.load(std::memory_order_relaxed));
      last_bytes = bytes;
      last_time  = now;
    }
  }
};

// ---------------------------------------------------------------------------
// cq_event dispatch shims
// ---------------------------------------------------------------------------

void gcs_grpc_reactor::impl::read_op::on_complete(bool ok)
{
  ln->owner->handle_unary_event(this, ok);
}

void gcs_grpc_reactor::impl::bidi_tag::on_complete(bool ok)
{
  s->ln->owner->handle_bidi_event(*s, kind, ok);
}

void gcs_grpc_reactor::impl::kick_tag::on_complete(bool /*ok*/)
{
  // Runs on the lane worker for both normal kicks and shutdown-flushed alarms;
  // drain_incoming()'s route() fails queued ranges when stopping.
  ln->owner->drain_incoming(*ln);
}

// ---------------------------------------------------------------------------
// Reactor construction / teardown
// ---------------------------------------------------------------------------

gcs_grpc_reactor::gcs_grpc_reactor(config cfg) : _cfg(std::move(cfg))
{
  if (!_cfg.creds) {
    throw std::invalid_argument("gcs_grpc_reactor: creds (authorizer) is required");
  }

  _impl                = std::make_unique<impl>();
  _impl->cfg           = _cfg;
  _impl->host_mr       = _cfg.host_memory_resource;
  _impl->bytes_counter = &_bytes_read_total;

  auto const n_lanes = std::max<std::size_t>(_cfg.num_channels, 1);
  auto const window  = std::max<std::size_t>(_cfg.max_streams / n_lanes, 1);

  _impl->device_window = std::max<std::size_t>(_cfg.max_streams, 8);
  _impl->device_slots.release(static_cast<std::ptrdiff_t>(_impl->device_window));

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

  _impl->channel_target = target;

  _impl->lanes.reserve(n_lanes);
  for (std::size_t i = 0; i < n_lanes; ++i) {
    auto ln     = std::make_unique<impl::lane>();
    ln->owner   = _impl.get();
    ln->index   = i;
    ln->window  = window;
    ln->kick.ln = ln.get();

    grpc::ChannelArguments args;
    // Distinct channel args force a distinct subchannel (its own TCP/ALTS
    // connection) per lane — otherwise gRPC dedupes them into one connection.
    args.SetInt("grpc.sirius_lane", static_cast<int>(i));
    args.SetMaxReceiveMessageSize(-1);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);

    ln->channel = grpc::CreateCustomChannel(target, channel_creds, args);
    ln->stub    = v2::Storage::NewStub(ln->channel);
    _impl->lanes.push_back(std::move(ln));
  }

  // Start workers once channels + stubs are live.
  for (auto& ln : _impl->lanes) {
    ln->worker = std::thread([l = ln.get()]() { l->worker_loop(); });
  }
  _impl->stats_thread = std::thread([im = _impl.get()]() { im->stats_loop(); });

  SIRIUS_LOG_INFO(
    "gcs_grpc: reactor up | channels={} cq_workers={} max_streams={} (per-lane window {}) "
    "bidi_reads={} directpath={} target={} target_read_bytes={} retry(max={} base={}ms "
    "jitter={}ms) timeout={}s",
    n_lanes,
    n_lanes,
    _cfg.max_streams,
    window,
    bidi_mode_name(_cfg.bidi_reads),
    _cfg.directpath,
    target,
    _cfg.target_read_bytes,
    _cfg.max_retry_attempts,
    _cfg.retry_backoff_base.count(),
    _cfg.retry_jitter.count(),
    _cfg.request_timeout_s);
}

gcs_grpc_reactor::~gcs_grpc_reactor() { shutdown(); }

void gcs_grpc_reactor::interrupt() {}

void gcs_grpc_reactor::shutdown()
{
  if (!_impl) return;
  bool expected = false;
  if (!_impl->stopping.compare_exchange_strong(expected, true)) return;

  // Wake + stop the stats logger first (it only reads counters).
  _impl->stats_cv.notify_all();
  if (_impl->stats_thread.joinable()) { _impl->stats_thread.join(); }

  // 1) Cancel every live RPC and pending retry alarm. The workers keep
  //    running and reap the resulting completions: handlers observe
  //    `stopping`, fail their ranges and never start new stream work.
  for (auto& ln : _impl->lanes) {
    std::lock_guard<std::mutex> lk(ln->mtx);
    for (auto& [op, ctx] : ln->live_op_ctxs) {
      ctx->TryCancel();
    }
    for (auto* op : ln->backoff_ops) {
      op->alarm.Cancel();
    }
    for (auto& [key, s] : ln->sessions) {
      if (s->ctx) s->ctx->TryCancel();
    }
  }

  // 2) Wait for every started RPC to be reaped — a CompletionQueue may only
  //    be Shutdown() once no further work will be added, and reaping (Finish)
  //    counts as work. Bounded by the request timeout as a hard cap.
  auto const deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(_cfg.request_timeout_s + 5);
  for (auto& ln : _impl->lanes) {
    for (;;) {
      {
        std::lock_guard<std::mutex> lk(ln->mtx);
        if (ln->drained()) break;
      }
      if (std::chrono::steady_clock::now() > deadline) {
        SIRIUS_LOG_ERROR(
          "gcs_grpc: lane {} did not drain before shutdown deadline; forcing CQ shutdown",
          ln->index);
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }

  // 3) Wait for pending device-path H2D completions (their host funcs touch
  //    impl state). Copies are already queued on caller streams.
  {
    auto const dev_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (_impl->device_pending.load(std::memory_order_acquire) != 0 &&
           std::chrono::steady_clock::now() < dev_deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if (_impl->device_pending.load(std::memory_order_acquire) != 0) {
      SIRIUS_LOG_ERROR("gcs_grpc: {} device-path completions still pending at shutdown",
                       _impl->device_pending.load(std::memory_order_relaxed));
    }
  }

  // 4) Now the CQs are quiescent: shut them down and join the workers.
  for (auto& ln : _impl->lanes) {
    ln->cq.Shutdown();
  }
  for (auto& ln : _impl->lanes) {
    if (ln->worker.joinable()) { ln->worker.join(); }
  }

  // 5) Fail anything that never reached a stream: queued submissions, unary
  //    ops awaiting a slot, and ranges still held by (never-started) sessions.
  auto fail_ep = std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor shut down"));
  for (auto& ln : _impl->lanes) {
    for (auto& rs : ln->incoming) {
      _impl->fail_range(std::move(rs), fail_ep);
    }
    ln->incoming.clear();
    for (auto* op : ln->pending_ops) {
      _impl->fail_range(std::move(op->rs), fail_ep);
      delete op;
    }
    ln->pending_ops.clear();
    for (auto& [key, s] : ln->sessions) {
      for (auto& [rid, rs] : s->outstanding) {
        _impl->fail_range(std::move(rs), fail_ep);
      }
      s->outstanding.clear();
      while (!s->pending.empty()) {
        _impl->fail_range(std::move(s->pending.front()), fail_ep);
        s->pending.pop_front();
      }
    }
    ln->sessions.clear();
  }

  auto st = stats();
  SIRIUS_LOG_INFO(
    "gcs_grpc: reactor shutdown | totals: bytes={} ranges={} sg_reads={} unary_streams={} "
    "bidi_sessions={} bidi_ranges={} bidi_fallbacks={} bidi_redirects={} retries={} "
    "retry_exhausted={} device_chunks={}",
    st.bytes_read,
    st.ranges_completed,
    st.sg_reads,
    st.unary_streams,
    st.bidi_sessions,
    st.bidi_ranges,
    st.bidi_fallbacks,
    st.bidi_redirects,
    st.retries,
    st.retry_exhausted,
    st.device_chunks);
}

gcs_grpc_reactor::stats_snapshot gcs_grpc_reactor::stats() const noexcept
{
  stats_snapshot s;
  if (!_impl) return s;
  s.bytes_read       = _bytes_read_total.load(std::memory_order_relaxed);
  s.ranges_completed = _impl->ranges_completed.load(std::memory_order_relaxed);
  s.sg_reads         = _impl->sg_reads.load(std::memory_order_relaxed);
  s.unary_streams    = _impl->unary_streams.load(std::memory_order_relaxed);
  s.bidi_sessions    = _impl->bidi_sessions_opened.load(std::memory_order_relaxed);
  s.bidi_ranges      = _impl->bidi_ranges.load(std::memory_order_relaxed);
  s.bidi_fallbacks   = _impl->bidi_fallbacks.load(std::memory_order_relaxed);
  s.bidi_redirects   = _impl->bidi_redirects.load(std::memory_order_relaxed);
  s.retries          = _impl->retries.load(std::memory_order_relaxed);
  s.retry_exhausted  = _impl->retry_exhausted.load(std::memory_order_relaxed);
  s.device_chunks    = _impl->device_chunks.load(std::memory_order_relaxed);
  return s;
}

// ---------------------------------------------------------------------------
// Static helpers
// ---------------------------------------------------------------------------

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
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::seconds(_cfg.request_timeout_s));
  ctx.AddMetadata("x-goog-request-params", routing_params(bucket));

  v2::Object obj;
  auto status = _impl->lanes.front()->stub->GetObject(&ctx, req, &obj);
  if (!status.ok()) { std::rethrow_exception(to_exception(status, "GetObject")); }

  gcs_grpc_object_state st;
  st.bucket      = std::string(bucket);
  st.key         = std::string(key);
  st.object_size = static_cast<std::size_t>(obj.size());
  st.generation  = obj.generation();
  return st;
}

// ---------------------------------------------------------------------------
// Read entry points
// ---------------------------------------------------------------------------

std::size_t gcs_grpc_reactor::host_read(native_handle_type handle,
                                        std::size_t offset,
                                        std::size_t size,
                                        std::uint8_t* dst)
{
  // Synchronous helper (footer stats, small sync callers): plain blocking
  // server-streaming ReadObject on lane 0's stub, no CQ involvement.
  v2::ReadObjectRequest req;
  req.set_bucket(bucket_resource(handle->bucket));
  req.set_object(handle->key);
  if (handle->generation != 0) { req.set_generation(handle->generation); }
  req.set_read_offset(static_cast<int64_t>(offset));
  req.set_read_limit(static_cast<int64_t>(size));

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::seconds(_cfg.request_timeout_s));
  ctx.AddMetadata("x-goog-request-params", routing_params(handle->bucket));

  auto reader = _impl->lanes.front()->stub->ReadObject(&ctx, req);

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
  auto rs    = std::make_unique<range_state>();
  rs->handle = std::move(req.handle);
  rs->offset = req.offset;
  rs->total  = req.size;
  rs->segments.push_back(
    cudf::host_span<std::byte>(reinterpret_cast<std::byte*>(req.dst), req.size));
  rs->cctx = std::move(req.ctx);
  _impl->submit_range(std::move(rs));
}

void gcs_grpc_reactor::host_enqueue_bulk(std::span<host_read_req_type> batch)
{
  // Each range routes through submit_range, so ranges of the same object
  // aggregate onto bidi sessions across the lane pool automatically.
  for (auto& r : batch) {
    host_read_async(std::move(r));
  }
}

void gcs_grpc_reactor::host_read_sg_async(host_read_sg_req_type req)
{
  _impl->sg_reads.fetch_add(1, std::memory_order_relaxed);
  if (!_impl->logged_first_sg.exchange(true)) {
    SIRIUS_LOG_INFO(
      "gcs_grpc: scatter-gather read path active (first request: {} bytes across {} segments, "
      "split target {} bytes)",
      req.size,
      req.segments.size(),
      _cfg.target_read_bytes);
  }

  auto const target = std::max<std::size_t>(_cfg.target_read_bytes, 1UL << 20);
  auto const n_sub  = req.size == 0 ? 0 : (req.size + target - 1) / target;

  if (n_sub <= 1) {
    auto rs      = std::make_unique<range_state>();
    rs->handle   = std::move(req.handle);
    rs->offset   = req.offset;
    rs->total    = req.size;
    rs->segments = std::move(req.segments);
    rs->cctx     = std::move(req.ctx);
    _impl->submit_range(std::move(rs));
    return;
  }

  // Split into sub-ranges of ~target bytes so they parallelize across
  // streams/lanes. The outer request_context (pending == 1) is resolved via an
  // inner context counting the sub-ranges.
  auto outer = std::move(req.ctx);
  auto inner = sirius::io::request_context::create(
    n_sub, req.size, [outer](std::size_t, std::exception_ptr ep) {
      if (!outer) return;
      if (ep) {
        outer->chunk_failed(std::move(ep));
      } else {
        outer->chunk_done();
      }
    });

  std::size_t seg_idx = 0;
  std::size_t seg_off = 0;
  std::size_t cur_off = req.offset;
  std::size_t left    = req.size;
  while (left > 0) {
    auto sub_size = std::min(target, left);
    auto rs       = std::make_unique<range_state>();
    rs->handle    = req.handle;
    rs->offset    = cur_off;
    rs->total     = sub_size;
    rs->cctx      = inner;

    // Carve `sub_size` bytes of destination segments (subspans at the edges).
    std::size_t need = sub_size;
    while (need > 0 && seg_idx < req.segments.size()) {
      auto seg          = req.segments[seg_idx];
      std::size_t avail = seg.size() - seg_off;
      std::size_t take  = std::min(need, avail);
      rs->segments.push_back(seg.subspan(seg_off, take));
      seg_off += take;
      need -= take;
      if (seg_off == seg.size()) {
        ++seg_idx;
        seg_off = 0;
      }
    }
    _impl->submit_range(std::move(rs));
    cur_off += sub_size;
    left -= sub_size;
  }
}

// ---------------------------------------------------------------------------
// Device path: staged pinned read + stream-ordered H2D, completion via
// cudaLaunchHostFunc — no detached threads, no per-chunk stream synchronize.
// ---------------------------------------------------------------------------

void gcs_grpc_reactor::enqueue_bulk(std::span<device_read_req_type> batch)
{
  if (!batch.empty() && !_impl->logged_first_device.exchange(true)) {
    SIRIUS_LOG_INFO(
      "gcs_grpc: device read path active (pinned staging window {} chunks, async H2D + "
      "host-func completion)",
      _impl->device_window);
  }

  for (auto& req : batch) {
    if (_impl->host_mr == nullptr) {
      if (req.ctx) {
        req.ctx->chunk_failed(std::make_exception_ptr(
          std::runtime_error("gcs_grpc_reactor: device reads require a host memory resource")));
      }
      continue;
    }
    // Bounded pinned footprint: block the (scan) caller when the staging
    // window is exhausted — natural backpressure, matching s3_reactor's
    // bounded staging model.
    if (!_impl->device_slots.try_acquire_for(std::chrono::seconds(60))) {
      if (req.ctx) {
        req.ctx->chunk_failed(std::make_exception_ptr(
          std::runtime_error("gcs_grpc_reactor: timed out waiting for a device staging slot")));
      }
      continue;
    }

    auto* dop  = new impl::device_op();
    dop->owner = _impl.get();
    dop->r     = req;
    try {
      dop->staging = _impl->host_mr->allocate_multiple_blocks(dop->r.io_size, nullptr);
    } catch (...) {
      auto ep = std::current_exception();
      _impl->device_slots.release();
      if (dop->r.ctx) dop->r.ctx->chunk_failed(ep);
      delete dop;
      continue;
    }

    // Describe the staged read as a scatter-gather range over the pinned
    // blocks; it flows through the same bidi/unary transport as host reads.
    std::vector<cudf::host_span<std::byte>> segments;
    {
      std::size_t left = dop->r.io_size;
      for (std::size_t b = 0; left > 0; ++b) {
        auto blk         = dop->staging->at(b);
        std::size_t take = std::min<std::size_t>(blk.size(), left);
        segments.emplace_back(reinterpret_cast<std::byte*>(blk.data()), take);
        left -= take;
      }
    }

    auto inner = sirius::io::request_context::create(
      1, dop->r.io_size, [im = _impl.get(), dop](std::size_t, std::exception_ptr ep) {
        // Runs on a lane worker thread once the staged network read finished.
        auto fail = [&](std::exception_ptr e) {
          if (dop->r.ctx) dop->r.ctx->chunk_failed(std::move(e));
          im->device_slots.release();
          delete dop;
        };
        if (ep) {
          fail(std::move(ep));
          return;
        }
        if (dop->r.device_id >= 0) {
          if (auto err = cudaSetDevice(dop->r.device_id); err != cudaSuccess) {
            fail(std::make_exception_ptr(std::runtime_error(
              std::string("gcs_grpc: cudaSetDevice failed: ") + cudaGetErrorString(err))));
            return;
          }
        }
        // Copy [data_off, data_off + data_size) out of the staged blocks.
        std::size_t pos  = 0;  // position within the staged io range
        std::size_t out  = 0;
        std::size_t skip = dop->r.data_off;
        std::size_t left = dop->r.data_size;
        for (std::size_t b = 0; left > 0 && pos < dop->r.io_size; ++b) {
          auto blk        = dop->staging->at(b);
          std::size_t len = std::min<std::size_t>(blk.size(), dop->r.io_size - pos);
          pos += len;
          if (skip >= len) {
            skip -= len;
            continue;
          }
          auto* src        = reinterpret_cast<std::uint8_t*>(blk.data()) + skip;
          std::size_t take = std::min(len - skip, left);
          skip             = 0;
          if (auto err = cudaMemcpyAsync(
                dop->r.dst + out, src, take, cudaMemcpyHostToDevice, dop->r.stream);
              err != cudaSuccess) {
            fail(std::make_exception_ptr(std::runtime_error(
              std::string("gcs_grpc: cudaMemcpyAsync failed: ") + cudaGetErrorString(err))));
            return;
          }
          out += take;
          left -= take;
        }
        im->device_pending.fetch_add(1, std::memory_order_acq_rel);
        if (auto err = cudaLaunchHostFunc(dop->r.stream, &impl::device_op_done, dop);
            err != cudaSuccess) {
          im->device_pending.fetch_sub(1, std::memory_order_acq_rel);
          fail(std::make_exception_ptr(std::runtime_error(
            std::string("gcs_grpc: cudaLaunchHostFunc failed: ") + cudaGetErrorString(err))));
          return;
        }
      });

    auto rs      = std::make_unique<range_state>();
    rs->handle   = dop->r.handle;
    rs->offset   = dop->r.file_off;
    rs->total    = dop->r.io_size;
    rs->segments = std::move(segments);
    rs->cctx     = std::move(inner);
    _impl->submit_range(std::move(rs));
  }
}

}  // namespace sirius::io::gcs
