# GCS Parquet Reading — Design Document

## Overview

This document describes the design for extending Sirius's parquet reading support to Google
Cloud Storage (GCS) using GCS's S3-compatible XML API (HMAC authentication). The change
adds a `gcs_ioctx` backend that registers for the `gs://` URI scheme and delegates all HTTP
I/O to the existing libcurl + SigV4 stack.

---

## Background: S3 parquet reading today

Sirius reads parquet files from S3 through a layered abstraction:

```
SQL parquet_scan("s3://bucket/key.parquet")
        │
        ▼
datasource_factory::create()        ← URI scheme dispatch
        │
        ▼
s3_blocking_ioctx / s3_ioctx        ← HTTP GET via libcurl
        │  (presigned SigV4 URL or Authorization header)
        ▼
s3_blocking_io_object               ← holds bucket + key + size
        │
        ▼
sirius_datasource                   ← cudf::io::datasource adapter
        │
        ▼
cuDF parquet reader / GPU decode
```

Key architectural properties:
- **`sirius_ioctx`** is the abstract backend trait (`io/io_context.hpp`).
- **`datasource_registry`** maps URI schemes → `sirius_ioctx` instances; dispatch is by
  `supports(path)` and scheme lookup.
- **Auth is fully pluggable** via `s3_request_authorizer`. Sirius ships `sirius_sigv4_presigned_authorizer`
  (query-string SigV4) and `sirius_sigv4_header_authorizer` (Authorization header SigV4).
- The **SigV4 engine** (`sigv4.hpp`) is generic: `sigv4_signer_config` has a `service` field
  (default `"s3"`) and is reusable for any SigV4-compatible endpoint.

---

## GCS HMAC approach

GCS exposes an [S3-compatible XML API](https://cloud.google.com/storage/docs/interoperability) via
HMAC keys. Requests are signed with AWS SigV4 against:

| Field    | Value                              |
|----------|------------------------------------|
| endpoint | `https://storage.googleapis.com`   |
| region   | `auto` (GCS accepts any value)     |
| service  | `s3`                               |
| signing  | SigV4 — identical to the S3 path   |

Because the signing algorithm is identical to S3, **no new signing code is needed**. The existing
`sirius_sigv4_presigned_authorizer` works as-is when pointed at the GCS endpoint with HMAC
credentials.

---

## Design

### New files

```
src/include/io/gcs/gcs_io_object.hpp   — io_object for gs:// paths (gs:// cache id)
src/include/io/gcs/gcs_ioctx.hpp       — gcs_ioctx class declaration
src/io/gcs/gcs_ioctx.cpp               — supports() + create_io_object() overrides
```

### Modified files

```
src/include/io/object_store_config.hpp  — add gcs_object_store_config struct
src/include/sirius_config.hpp           — add gcs_config field to sirius_config
src/sirius_config.cpp                   — add YAML parsing for gcs_config section
src/include/sirius_context.hpp          — add gcs_ioctx_ member
src/sirius_context.cpp                  — init + cache + borrowed_io_ctxs + teardown
CMakeLists.txt                          — add src/io/gcs/gcs_ioctx.cpp
```

### Class hierarchy

```
sirius_ioctx  (abstract)
    └── s3_blocking_ioctx          ← existing S3 implementation
            └── gcs_ioctx          ← NEW: gs:// adapter over the same HTTP stack

sirius_io_object  (abstract)
    └── s3_blocking_io_object      ← existing: stores bucket/key, cache_id = "s3://..."
            └── gcs_io_object      ← NEW: cache_id = "gs://...", prevents S3/GCS cache collision
```

`gcs_ioctx` inherits everything from `s3_blocking_ioctx` unchanged (libcurl handle pool,
range GETs, retry logic, device-read bounce, async paths, prefetch cache hookup) and overrides
only two methods:

| Method              | `s3_blocking_ioctx` behavior      | `gcs_ioctx` behavior          |
|---------------------|-----------------------------------|-------------------------------|
| `supports(path)`    | Accepts `s3://…`                  | Accepts `gs://…`              |
| `create_io_object()`| Parses `s3://`, creates `s3_blocking_io_object` | Parses `gs://`, creates `gcs_io_object` |

`gcs_io_object` inherits `bucket()`, `key()`, `size()`, `object_path()` from
`s3_blocking_io_object` and only overrides `raw_file_cache_id()` to return
`"gs://bucket/key"` instead of `"s3://bucket/key"`. This prevents cross-backend cache
key collisions when both S3 and GCS backends are active.

### Configuration

YAML stanza (new top-level section, parallel to `object_store_config`):

```yaml
gcs_config:
  hmac_access_key: "GOOG1E..."
  hmac_secret_key: "..."
  endpoint: "https://storage.googleapis.com"   # optional — this is the default
  ca_bundle_path: ""                            # optional — uses system CA bundle
  tls_verify: true                              # optional — default true
```

The GCS backend is activated only when `hmac_access_key` and `hmac_secret_key` are both
non-empty. An absent `gcs_config` section (the default) leaves GCS disabled.

### Context wiring

`SiriusContext::initialize()` builds the GCS backend immediately after the S3 block (both
follow the same pattern):

1. Construct `sirius_sigv4_presigned_authorizer` with HMAC credentials + GCS endpoint.
2. Wrap in `s3_ioctx_config` (reuses the same config struct as S3).
3. Construct `gcs_ioctx` and store in `gcs_ioctx_`.
4. Optionally call `initialize_cache()` if `enable_prefetch_cache` is set.
5. Push `gcs_ioctx_` onto `borrowed_io_ctxs` so `sirius_scan_manager` dispatches `gs://`
   paths to it.

Teardown mirrors S3: `gcs_ioctx_.reset()` inside `terminate()`, after
`scan_manager_.reset()` drops its borrowed alias.

---

## Credential setup (end-user guide)

1. Enable [GCS interoperability](https://cloud.google.com/storage/docs/interoperability) for your GCS project.
2. Generate an HMAC key pair in the Cloud Console (`Storage → Settings → Interoperability`).
3. Add to your Sirius YAML config:

   ```yaml
   gcs_config:
     hmac_access_key: "<HMAC access ID>"
     hmac_secret_key: "<HMAC secret>"
   ```

4. Query as usual:

   ```sql
   LOAD 'sirius.duckdb_extension';
   SELECT * FROM parquet_scan('gs://my-bucket/data/*.parquet');
   ```

---

## Phase 2: Native service account auth (future work)

The HMAC path requires explicit key management. A future `gcs_oauth2_authorizer` would:
1. Load a service account JSON key file.
2. Mint a short-lived OAuth2 bearer token via `POST https://oauth2.googleapis.com/token`
   (JWT-signed with the service account's RSA private key).
3. Inject `Authorization: Bearer <token>` on each GCS request.
4. Refresh the token before expiry (typically 1-hour lifetime).

This fits into the existing `s3_request_authorizer` pluggable interface without touching
any other code — the `gcs_ioctx` itself would not change.

---

## Non-goals

- GCS gRPC transport (the S3-compatible HTTP API covers all Sirius read patterns).
- Resumable upload support (Sirius is read-only).
- RDMA / GPUDirect to GCS (no equivalent to S3's GPUDirect acceleration path).
