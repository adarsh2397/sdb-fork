# GCS Parquet Reading — Task Status

## Summary

End-to-end implementation of GCS parquet reading via the S3-compatible XML API (HMAC
authentication). All tasks completed.

---

## Tasks

| # | Task | Status |
|---|------|--------|
| 1 | Write `GCS_DESIGN.md` | ✅ Done |
| 2 | Add `gcs_object_store_config` to `object_store_config.hpp` | ✅ Done |
| 3 | Add `gcs_config` field to `sirius_config.hpp` + YAML parsing in `sirius_config.cpp` | ✅ Done |
| 4 | Create `gcs_io_object.hpp`, `gcs_ioctx.hpp`, `gcs_ioctx.cpp` | ✅ Done |
| 5 | Wire `gcs_ioctx` into `sirius_context` (init + cache + routing + teardown) | ✅ Done |
| 6 | Register `src/io/gcs/gcs_ioctx.cpp` in `CMakeLists.txt` | ✅ Done |

---

## Files Changed

### New files

| File | Description |
|------|-------------|
| `GCS_DESIGN.md` | Architecture doc: design rationale, class hierarchy, config schema, credential setup guide, phase 2 roadmap |
| `src/include/io/gcs/gcs_io_object.hpp` | `gcs_io_object` — subclasses `s3_blocking_io_object`, overrides `raw_file_cache_id()` to emit `gs://` prefix and prevent S3/GCS cache key collisions |
| `src/include/io/gcs/gcs_ioctx.hpp` | `gcs_ioctx` declaration — subclass of `s3_blocking_ioctx` accepting `gs://` URIs |
| `src/io/gcs/gcs_ioctx.cpp` | `gcs_ioctx` implementation — `supports()` and `create_io_object()` overrides; all HTTP machinery inherited from `s3_blocking_ioctx` |

### Modified files

| File | Change |
|------|--------|
| `src/include/io/object_store_config.hpp` | Added `gcs_object_store_config` struct (`hmac_access_key`, `hmac_secret_key`, `endpoint`, `ca_bundle_path`, `tls_verify`) |
| `src/include/sirius_config.hpp` | Added `gcs_object_store_config gcs_config{}` field to `sirius_config` |
| `src/sirius_config.cpp` | Added `from_yaml` overload for `gcs_object_store_config`; wired `gcs_config:` YAML section into the config loader |
| `src/include/sirius_context.hpp` | Added `gcs_ioctx_` member (`shared_ptr<sirius_ioctx>`); updated teardown-order comment |
| `src/sirius_context.cpp` | Added `#include "io/gcs/gcs_ioctx.hpp"`; added GCS init block (constructs `sirius_sigv4_presigned_authorizer` + `gcs_ioctx` when HMAC creds present); added `gcs_ioctx_` to prefetch cache init, `borrowed_io_ctxs`, and `terminate()` teardown |
| `CMakeLists.txt` | Added `src/io/gcs/gcs_ioctx.cpp` to the sirius extension source list |

---

## Architecture

```
sirius_ioctx  (abstract)
    └── s3_blocking_ioctx          existing S3 implementation
            └── gcs_ioctx          NEW: overrides supports() + create_io_object() only

sirius_io_object  (abstract)
    └── s3_blocking_io_object      existing: cache_id = "s3://..."
            └── gcs_io_object      NEW: cache_id = "gs://..." (prevents cache collision)
```

All HTTP I/O (libcurl handle pool, range GETs, retry/backoff, device-read bounce, async
paths, prefetch cache) is inherited from `s3_blocking_ioctx` unchanged. The GCS backend
reuses `sirius_sigv4_presigned_authorizer` pointed at `storage.googleapis.com` with
`region = "auto"` — GCS accepts any region value for HMAC requests.

---

## Configuration

Add to the Sirius YAML config to enable GCS:

```yaml
gcs_config:
  hmac_access_key: "GOOG1E..."       # GCS HMAC access ID
  hmac_secret_key: "..."             # GCS HMAC secret
  # endpoint: "https://storage.googleapis.com"  # optional, this is the default
  # tls_verify: true                             # optional
```

Then query normally:

```sql
LOAD 'sirius.duckdb_extension';
SELECT * FROM parquet_scan('gs://my-bucket/data/*.parquet');
```

---

## Future Work (Phase 2)

Native service account / OAuth2 authentication — implement `gcs_oauth2_authorizer` that
loads a service account JSON key, mints short-lived bearer tokens, and injects
`Authorization: Bearer <token>` per request. Fits into the existing
`s3_request_authorizer` interface with no changes to `gcs_ioctx` itself.
