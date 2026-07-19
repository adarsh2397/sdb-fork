# GCS backend (`src/io/gcs/`) — context for next time

GPU-native Parquet reading from Google Cloud Storage for Sirius. This directory
holds two GCS transports plus OAuth2 auth. Scoped notes; the root `CLAUDE.md`
still governs build/test/architecture conventions.

## Two transports (selectable per `gcs_config.transport`)

1. **XML / S3-compatible (`transport: xml`, default)** — `gcs_async_ioctx`
   subclasses `s3_ioctx`, reusing the libcurl-multi `s3_reactor` wholesale;
   overrides only `supports()` (accepts `gs://`) and `create_io_object()`.
   There is also a legacy blocking `gcs_ioctx` (subclass of `s3_blocking_ioctx`)
   — no longer wired into `SiriusContext`, kept compiling only.
2. **Native gRPC (`transport: grpc`)** — `gcs_grpc_ioctx` is a
   `templated_ioctx<gcs_grpc_reactor>`. Talks `google.storage.v2` `ReadObject`.
   This is the path under active optimization.

Both plug into the same `templated_ioctx`/`io_reactor_c` machinery, so the scan
path, prefetch cache, and pipeline are transport-agnostic — only the transport
differs. `SiriusContext::initialize()` builds the backend, calls
`initialize_cache()` on it (when prefetch cache enabled), and pushes it onto the
scan-manager's `borrowed_io_ctxs` so `gs://` paths route to it.

### File map
- `gcs_grpc_reactor.{hpp,cpp}` — async CompletionQueue reactor (the hot path).
- `gcs_grpc_ioctx.{hpp,cpp}` — `supports()`/`create_io_object()` for gRPC.
- `gcs_async_ioctx.{hpp,cpp}` — XML async backend over `s3_ioctx`.
- `gcs_ioctx.{hpp,cpp}` — legacy blocking XML backend (unwired).
- `gcs_oauth2_authorizer.{hpp,cpp}` — `gcs_metadata_server_authorizer`
  (GCE metadata-server OAuth2 token, cached ~1h, refreshes 60s before expiry).
- `../../cmake/gcs_grpc_proto.cmake` — proto codegen (see Build gotchas).

## Auth (`s3_request_authorizer` chain, precedence order)
1. `static_bearer_token` (debug only; expires ~1h)
2. `use_metadata_server: true` → `gcs_metadata_server_authorizer` (recommended on GCE)
3. HMAC keys (`hmac_access_key`/`hmac_secret_key`) — SigV4, XML path
For gRPC the bearer token is attached as gRPC **call credentials**
(`MetadataCredentialsFromPlugin` → `GetMetadata` sets `authorization: Bearer …`).
**DirectPath mode bypasses this** and uses `GoogleDefaultCredentials` instead.

## gRPC reactor design (`gcs_grpc_reactor.cpp`)
- **Channel-lane pool**: `grpc_channels` (default 4) lanes, each with its own
  gRPC channel (distinct `grpc.sirius_lane` channel arg → own TCP/ALTS
  connection), CompletionQueue and worker thread. Submissions round-robin
  across lanes; each lane runs a bounded window of
  `grpc_max_streams / grpc_channels` concurrent streams. Every CQ tag is a
  `cq_event` (virtual `on_complete(ok)`), so unary ops, bidi sub-events,
  retry alarms and submit "kicks" share one worker loop. ALL stream ops are
  initiated on the lane worker (submits arrive via a zero-delay `grpc::Alarm`
  kick) — single-writer bidi sequencing by construction.
- **BidiReadObject sessions** (`grpc_bidi_reads: auto|on|off`, default auto):
  ranges for the same object multiplex over a persistent per-(lane, object)
  `BidiReadObject` stream — the Rapid Storage (zonal bucket) fast path. Ranges
  carry `read_id`s, responses demux `read_id → range_state`. `auto` probes per
  bucket and permanently falls back to unary `ReadObject` on
  UNIMPLEMENTED/INVALID_ARGUMENT/FAILED_PRECONDITION before first data.
  Flow control: ≤128 outstanding ranges/session, ≤64 ranges/Write.
  - **Requires DirectPath — hard requirement, now enforced.** The redirect
    `routing_token` is consumed by the CLIENT-side DirectPath routing stack
    (c2p/xDS + RLS does a per-RPC lookup keyed on `x-goog-request-params` and
    connects the stream to the zonal storage server — google-cloud-go
    force-registers `balancer/rls` + `xds/googledirectpath` for exactly this).
    Over the CFE the token has no routing layer to act on: the reopened stream
    lands on the CFE again and re-redirects forever, while unary reads are
    proxied fine. **`unary works + bidi redirect-loops` = the process is
    talking to the CFE** (grpc_directpath=false, or silent c2p fallback —
    verify with `GRPC_TRACE=google_c2p_resolver,rls_lb,xds_client`).
    Consequently: `auto` mode uses unary (with a one-shot WARN) when
    `grpc_directpath=false`. An unresolvable redirect loop FAILS the ranges
    (user explicitly rejected auto-degrade-to-unary) with a diagnostic ERROR
    echoing `directpath=`/`target=`.
  - **`tools/gcs_bidi_probe.cc`** is a standalone ~170-line sync-API repro of
    the redirect handshake using the same generated stubs + pixi gRPC (build
    command in its header). Probe OK ⇒ bug is in the reactor; probe loops ⇒
    environment/gRPC-build.
  - **ROOT CAUSE FOUND (2026-07-03): the pixi dependency was `grpc-cpp = "*"`
    — conda-forge froze that package name at ~1.51; the current package is
    `libgrpc`.** A ~1.51 core (a) has RLS-in-xDS disabled (default-on only
    ≥ 1.56) → the token never routes → endless redirect loop; (b) when forced
    with `GRPC_EXPERIMENTAL_XDS_RLS_LB=true`, its experimental RLS emits an
    uppercase `X-Google-RLS-Data` header that its own validation rejects →
    `INTERNAL: illegal header key` (observed via the probe — this error
    PROVES c2p+xDS+RLS all engaged). Fixed: `libgrpc = ">=1.62"` in
    pixi.toml. After changing it: `pixi install`, then FULL rebuild
    (`rm -rf build && pixi run make` — new gRPC ABI + regenerated stubs). No
    env var needed on modern cores.
  - A zonal bucket first aborts the stream with
    `ABORTED (10) "not available from this location"` carrying a
    `BidiReadObjectRedirectedError` (`routing_token` field 14, `read_handle`
    field 13). We reopen the stream echoing the token in **BOTH** the
    `x-goog-request-params` header **and** `BidiReadObjectSpec.routing_token`,
    plus `read_handle` in the spec. **The bidi header must be built byte-for-byte
    like google-cloud-cpp `OpenObject::RequestParams`
    (storage/internal/async/open_object.cc): BOTH bucket AND routing_token
    RAW** — `bucket=projects/_/buckets/<name>&routing_token=<tok>` with literal
    slashes, NOTHING percent-encoded. **The percent-encoded bucket was the root
    cause of the endless redirect loop** — unary tolerates the encoded bucket
    (so `routing_params()` stays for the unary path) but the bidi redirect
    routing needs it raw. Header is mandatory (spec-only loops); token-raw also
    confirmed by java-storage test `redirectTokenMustNotBeUrlEncoded` (09c426b).
    The public googleapis `storage.proto` routing annotation lists only
    `bucket`; it lags the internal proto — trust google-cloud-cpp + observed
    behaviour.
    Unary `ReadObject` works on DirectPath without this dance; `grpc_bidi_reads:
    off` is the always-safe fallback.
- **Scatter-gather reads** (`host_read_sg_async`): one contiguous file range
  delivered into a block-fragmented destination (the pinned 1 MiB blocks) —
  ONE transport request per merged column-chunk range instead of one per
  block. Big ranges are split into `grpc_target_read_bytes` (16 MiB) sub-reads
  so they still parallelize. Wired: `parquet_scan_task::read_range_into_allocation`
  → `sirius_datasource::host_read_segments_async` → ioctx (cache-aware) →
  reactor. Non-SG backends (XML/uring) transparently fall back per segment.
- **Rapid Storage redirect handshake** (the `ABORTED (10) "object is not
  available from this location"` case): a zonal bucket aborts a fresh
  `BidiReadObject` stream and returns a `BidiReadObjectRedirectedError` detail
  (in the `grpc-status-details-bin` trailer → `status.error_details()` →
  `google.rpc.Status.details` Any) carrying a `routing_token` + optional
  `read_handle`. `reap_session` checks this via `extract_bidi_redirect()`
  BEFORE the generic retriable-restart branch (ABORTED is retriable, but
  restarting WITHOUT the token just re-aborts — the original bug). On redirect
  we store the token/handle on the session and reopen the stream echoing them
  in both the `read_object_spec` (`set_routing_token` / `mutable_read_handle`)
  AND the `x-goog-request-params` header (`&routing_token=…`). Capped at
  `kMaxBidiRedirects` (5), separate from hard-failure retries. If it still
  can't route (no token / cap hit), it fails with an explicit "not available
  from this location … ensure grpc_directpath=true or grpc_bidi_reads=off"
  error rather than a bare status code.
- **Retries + resumption**: UNAVAILABLE / DEADLINE_EXCEEDED / RESOURCE_EXHAUSTED
  / ABORTED / INTERNAL retry up to `max_retry_attempts` with exponential
  backoff (base × 2^n + jitter) scheduled as `grpc::Alarm`s on the CQ, resuming
  at `offset + written` (`range_state` cursor survives attempts AND transport
  switches). Bidi stream breaks restart the session and resubmit incomplete
  ranges at their resume offsets.
- **`x-goog-request-params` routing header** is REQUIRED on every
  `ReadObject`/`BidiReadObject`/`GetObject` (`google.api.routing` annotation)
  or the server returns `INVALID_ARGUMENT`. We build it manually
  (`routing_params()`): `bucket=projects%2F_%2Fbuckets%2F<name>`.
- **DirectPath** (`grpc_directpath: true`): target `google-c2p:///<host>` +
  `GoogleDefaultCredentials` (ALTS + compute-SA auth). Bypasses the GFE on a
  co-located GCE VM — the ONLY way Rapid/zonal buckets are actually fast.
  Off-GCE must be `false`; gRPC auto-falls back to CFE/TLS otherwise.
- **Device path (`enqueue_bulk`)**: staged read into pinned blocks (flows
  through the same bidi/unary transport as a SG range) → `cudaMemcpyAsync` →
  `cudaLaunchHostFunc` completion. Bounded staging window (semaphore,
  `max_streams` slots) backpressures callers. No detached threads, no
  per-chunk `cudaStreamSynchronize`. Still not on the parquet hot path.
- **Shutdown ordering matters**: `stopping` flag → TryCancel every live
  RPC/alarm → wait for lanes to drain (a CQ may only be `Shutdown()` once no
  further work — incl. `Finish` — will be added) → CQ shutdown → join → fail
  never-started ranges. Don't "simplify" this.

## Caching (all gated by `enable_prefetch_cache: true`)
- **Footer/parquet metadata** — cached per file; reused on run 2+ (skips footer
  fetch). Includes column-chunk offsets + min/max stats (used by row-group
  pruning). Stored even when `enable_chunk_prewarm: false`.
- **File size** — cached in `create_io_object` so the per-file `GetObject`
  (size stat) is paid only on run 1 (immutability assumption: parquet files
  don't change in place).
- **Column-chunk DATA** — cached/prefetched ONLY when `enable_chunk_prewarm:
  true`. With it `false`, the cache populates only metadata; chunk reads always
  go to GCS (no read-through population). This is the "reuse metadata, re-read
  data every query" mode.
- **NOT cached: the directory file LIST** — glob expansion is DuckDB's bind
  phase; it re-`LIST`s every query. Use an explicit file array in the view to
  avoid re-listing.

### Cache matrix
| `enable_prefetch_cache` | `enable_chunk_prewarm` | footers+size reused | chunk data cached |
|---|---|---|---|
| true | false | yes | no (re-read) |
| true | true | yes | yes (retained) |
| false | (either) | no | no |

## IO/compute overlap
Two independent concurrent executors connected by the data repo:
- **scan executor** (`duckdb_scan.num_threads`) reads compressed bytes (IO).
- **per-GPU pipeline executor** (`pipeline.num_threads`, one per device) runs
  cuDF decode + compute.
Overlap is inherent (no caching needed); `num_threads` is the depth knob.
Chunk prewarm adds deeper look-ahead but couples prefetch with retention.

## Config knobs (`gcs_config`, under top-level `sirius:`)
```yaml
sirius:
  gcs_config:
    transport: grpc                 # xml | grpc
    grpc_endpoint: "storage.googleapis.com"
    grpc_max_streams: 128           # TOTAL stream concurrency, split across channels
    grpc_channels: 4                # lanes: own TCP conn + CQ + worker thread each
    grpc_bidi_reads: auto           # off | on | auto — BidiReadObject (Rapid fast path)
    grpc_target_read_bytes: 16MiB   # split size for large scatter-gather reads
    grpc_directpath: false          # true ONLY on co-located GCE VM + Rapid bucket
    use_metadata_server: true       # OAuth2 bearer via metadata server
    metadata_service_account: "default"
    # static_bearer_token / hmac_access_key / hmac_secret_key (alternatives)
  executor:
    scan_manager:
      use_sirius_datasource: true   # REQUIRED for gs:// (auto-forced multi-GPU)
      enable_prefetch_cache: true   # master switch for footer/size/chunk cache
      enable_chunk_prewarm: false   # chunk DATA caching/prefetch (metadata always cached)
      prefetch_buffer_pool_bytes: 4294967296
      prefetch_inflight_budget_chunks: 2048
    duckdb_scan:
      num_threads: 12               # shared scan/IO concurrency
      cache: none                   # result cache: none|parquet|table_host|table_gpu
    pipeline:
      num_threads: 4                # PER-GPU
  operator_params:
    scan_task_batch_size: 134217728 # 128 MiB; smaller = finer pipelining
  topology:
    num_gpus: 1                     # or gpu_ids: [0,1,...]
```

## DuckDB session (bind phase is separate from Sirius execution!)
- Launch: `duckdb -unsigned` (`allow_unsigned_extensions` is startup-only).
- The **bind phase** (reading parquet schema) uses DuckDB's own httpfs, NOT
  Sirius — needs its own GCS secret:
  ```sql
  INSTALL httpfs; LOAD httpfs;
  CREATE OR REPLACE SECRET gcs_secret (TYPE GCS, KEY_ID 'GOOG1E...', SECRET '...');
  -- bearer needs the community gcs extension; core httpfs GCS is HMAC-only,
  -- credential_chain is NOT supported for GCS.
  LOAD 'build/release/extension/sirius/sirius.duckdb_extension';
  ```
- Sirius execution uses its OWN gRPC+bearer auth — independent of the secret.

## Build gotchas (`cmake/gcs_grpc_proto.cmake`)
- **Protobuf must be found CONFIG-mode, before gRPC** (`set(protobuf_MODULE_COMPATIBLE ON)`
  + `find_package(Protobuf CONFIG REQUIRED)`). The legacy FindProtobuf module
  defines a PARTIAL `protobuf::*` target set; modern gRPCConfig then loads
  `protobuf-targets.cmake` which aborts with "Some (but not all) targets in
  this export set were already defined." Surfaced by the libgrpc ≥1.62 upgrade.
- FetchContent `googleapis` (`.git`!) → protoc generates `gcs_storage_protos`.
- **Proto file list must be the transitive closure** — add any `.proto` protoc
  reports missing (e.g. `google/type/expr.proto` via IAM policy).
- **Strip `[ctype = CORD]`** from `storage.proto` before protoc (4-pass regex,
  any list position) — otherwise `ChecksummedData::content()` is generated
  PRIVATE and the field is unreadable. After strip, `content()` is a public
  `const std::string&` (no `absl::Cord`).
- `gcs_storage_protos` is in the DuckDB **export set** AND its include dir is
  wrapped in `$<BUILD_INTERFACE:>` (can't export a raw build path; can't mix
  PRIVATE with the plain `target_link_libraries` signature used here).
- `s3_blocking_io_object` and `s3_blocking_ioctx` had `final` removed so the GCS
  subclasses can derive.
- After proto/cmake changes: `rm -rf build/ && pixi run make` (a plain
  `make clean` leaves `_deps/` + generated stubs stale).

## Verify / debug

Run env: `SIRIUS_LOG_DIR=/tmp SIRIUS_LOG_LEVEL=info` (logs are file-only). Every
transport feature announces itself at INFO — after a query run,
`grep gcs_grpc /tmp/sirius.log` and look for:

| Log line | Confirms |
|---|---|
| `gcs_grpc: reactor up \| channels=… max_streams=… bidi_reads=… directpath=…` | effective config (echoed at init) |
| `SiriusContext: GCS backend transport=grpc (…)` | backend selection + parsed knobs |
| `gcs_grpc: scatter-gather read path active (first request: …)` | Phase 3 SG reads in use |
| `gcs_grpc: BidiReadObject ACTIVE for bucket=…` | Phase 1 bidi/Rapid fast path serving data |
| `gcs_grpc: Rapid Storage redirect handshake active for bucket=…` | zonal-bucket redirect being followed (routing_token) |
| `gcs_grpc: following BidiReadObject redirect … routing_token=<set>` | each redirect hop (with resume range count) |
| `gcs_grpc: … not available from this location … ensure grpc_directpath=true` (ERROR) | redirect couldn't route — location/DirectPath problem |
| `gcs_grpc: BidiReadObject unavailable … falling back to unary` (WARN) | auto-mode fallback took over |
| `gcs_grpc: unary ReadObject path active …` | unary path in use (fallback or bidi off) |
| `gcs_grpc: retrying ReadObject … resume at +N` (WARN) | Phase 4 retry + resumption fired |
| `gcs_grpc: device read path active (pinned staging window …)` | Phase 5 device path in use |
| `gcs_grpc stats: +X MiB (Y MiB/s) \| totals: …` | 10s cadence throughput + per-path counters |
| `gcs_grpc: reactor shutdown \| totals: …` | end-of-session cumulative totals |

The stats/totals lines carry `unary_streams` / `bidi_sessions` / `bidi_ranges` /
`sg_reads` / `retries` / `device_chunks`, so the split between paths is
quantifiable per run. `gcs_grpc_reactor::stats()` exposes the same snapshot
programmatically.

- Confirm DirectPath: `GRPC_TRACE=xds_client,alts,pick_first GRPC_VERBOSITY=DEBUG`
  → ALTS handshake / c2p resolver = active; plain `pick_first` to GFE IP = not.
- Cold vs warm benchmarking: restart DuckDB between runs (no local disk cache
  for GCS); or `enable_prefetch_cache: false` + `cache: none`.

## Known limitations / honest caveats
- **IO-bound queries won't beat CPU DuckDB.** Sparse-projection simple
  aggregates (e.g. `COUNT(col) WHERE …`) are network-bound with trivial compute
  + an extra PCIe hop; CPU wins. GPU wins on compute-heavy queries (big joins,
  large GROUP BYs, wide scans) and co-located Rapid buckets over DirectPath.
- **BidiReadObject + redirect path: partially exercised.** A real Rapid bucket
  returned `ABORTED (10) "object is not available from this location"` — the
  redirect handshake, now handled (see redirect bullet above). Still verified
  against the proto surface only for the token/handle field access; watch the
  `following BidiReadObject redirect` INFO log to confirm it actually resolves.
  If it exhausts redirects, the ERROR line tells you it's a location/DirectPath
  problem — on a co-located VM set `grpc_directpath: true`; otherwise
  `grpc_bidi_reads: off` forces the (slower but portable) unary path.
- **routing_token is echoed in BOTH the spec field and the
  `x-goog-request-params` header, appended RAW (NOT percent-encoded)** — the
  bucket param is encoded, the token is not (google/java-storage test
  `redirectTokenMustNotBeUrlEncoded`). The header matters because the c2p/GFE
  routing layer reads it before the body. Encoding the token was the actual bug
  behind the endless redirect loop.
- **Bidi session restarts are immediate** (no backoff between session
  restarts; per-range attempts still capped) — fine for stream rotation, could
  hot-loop briefly during a real outage until attempts exhaust.
- **NUMA-aware staging is config-only** (`use_host_per_numa` exists in the
  cucascade configurator) — scan staging doesn't yet pin blocks to the
  GPU-local NUMA node explicitly.
- **Directory LIST is still re-fetched every query** (DuckDB bind phase, not
  Sirius) — use explicit file arrays in views to avoid it.
- **Multi-GPU**: set `topology.num_gpus`; the GCS backend is a single SHARED
  network reactor feeding all GPUs, so IO concurrency (`grpc_max_streams`,
  `grpc_channels`, `duckdb_scan.num_threads`) matters more than GPU count for
  IO-bound scans.

## What was done (session 2 — full transport rework, NOT yet compiled/run)
- **Channel-lane pool**: N channels × (CQ + worker), `grpc_channels` knob;
  removes single-worker memcpy ceiling + per-channel HTTP/2 stream cap.
- **BidiReadObject sessions** behind `grpc_bidi_reads` (auto probe + fallback).
- **Scatter-gather reads** end-to-end (types.hpp `host_read_sg_req` /
  `sg_write_cursor`, ioctx `host_read_segments_async` (cache-aware) + virtual
  `_io` w/ per-segment fallback, templated_ioctx native-SG detection via
  `requires`, datasource passthrough, parquet_scan_task one-read-per-merged-range,
  reactor `grpc_target_read_bytes` splitting).
- **Retries + resumption** (alarm-scheduled backoff, cursor resume) — the
  config fields existed but were previously unused.
- **Device path rewrite**: staged SG read + async H2D + host-func completion,
  bounded staging semaphore (replaced detached thread + per-chunk sync).
- **Observability**: `stats()` snapshot, init config echo, one-shot per-path
  "active" INFO logs, WARN/ERROR on retries/fallbacks, 10s throughput log,
  shutdown totals (see Verify / debug table).
- **NOT verified on hardware**: written on a non-CUDA machine; needs
  `rm -rf build/ && pixi run make` on the GCE box (proto list unchanged —
  bidi messages come from the same storage.proto; CORD strip already covers
  ChecksummedData). Watch for generated-accessor name drift in
  BidiReadObject{Request,Response} if the googleapis pin moved.

## What was done (session 1)
- Built the GCS XML async backend (`gcs_async_ioctx`) + metadata-server OAuth2.
- Built the native gRPC backend end-to-end (proto codegen, ioctx, reactor,
  routing header, CORD strip, export-set wiring).
- Replaced the gRPC thread-per-request + blocking-stream model with the
  async CompletionQueue reactor (true multiplexing). Added `grpc_max_streams`
  (default 128) — fixed a regression where concurrency was capped at 16.
- DirectPath support behind `grpc_directpath`.
- File-size caching in `create_io_object` (kills per-file `GetObject` on run 2+).
