/*
 * Copyright 2025, Sirius Contributors.
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

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>

namespace sirius::io {

/// Inert configuration carrier for remote object-store backends.
/// PR1 only exposes the POD fields and enum/string helpers; runtime plumbing and
/// backend consumption live in the integration/backend PRs.
/// Empty strings are valid and mean "no value configured".
struct object_store_config {
  std::string endpoint;
  std::string region;
  std::string access_key;
  std::string secret_key;
  /// STS temporary-credential session token (empty for long-lived keys). When
  /// set, the SigV4 signer adds X-Amz-Security-Token to presigned URLs.
  std::string session_token;

  /// Requested S3 transport. AUTO leaves the concrete backend/integration code
  /// to choose based on URI scheme and endpoint capabilities.
  enum class transport { AUTO, HTTP, RDMA };
  transport s3_transport = transport::AUTO;

  /// SigV4 signing form for S3 requests. @c presigned puts auth in the URL query
  /// string (default; works everywhere AWS does). @c header puts auth in the
  /// @c Authorization header (sign_request) — for on-prem / S3-compatible stores
  /// whose gateways prefer header auth over long presigned query strings.
  enum class signing_mode { presigned, header };
  signing_mode s3_signing_mode = signing_mode::presigned;

  /// PEM CA bundle used to verify the S3 endpoint's TLS certificate
  /// (CURLOPT_CAINFO). Empty (default) uses libcurl's system CA bundle —
  /// correct for AWS. Point it at a private / self-signed CA for on-prem or
  /// S3-compatible gateways (and the local-HTTPS test).
  std::string ca_bundle_path;

  /// Verify the S3 endpoint's TLS certificate (peer + host). Default true;
  /// false disables verification — INSECURE, dev/test only.
  bool tls_verify = true;

  /// Select the async (libcurl-multi) S3 backend. Default true: the datasource
  /// factory builds @c s3_ioctx (concurrent GETs + pipelined
  /// device reads). Set false to fall back to the blocking @c s3_blocking_ioctx (a
  /// per-request, serial-staging path) — the escape hatch if the async backend
  /// misbehaves against a particular store.
  bool s3_use_async_backend = true;
};

inline bool string_to_enum(std::string_view sv, object_store_config::transport& t)
{
  static const std::unordered_map<std::string_view, object_store_config::transport> map = {
    {"auto", object_store_config::transport::AUTO},
    {"http", object_store_config::transport::HTTP},
    {"https", object_store_config::transport::HTTP},
    {"rdma", object_store_config::transport::RDMA},
  };
  auto it = map.find(sv);
  if (it != map.end()) {
    t = it->second;
    return true;
  }
  return false;
}

inline bool enum_to_string(object_store_config::transport t, std::string& s)
{
  switch (t) {
    case object_store_config::transport::AUTO: s = "auto"; return true;
    case object_store_config::transport::HTTP: s = "http"; return true;
    case object_store_config::transport::RDMA: s = "rdma"; return true;
  }
  return false;
}

inline bool string_to_enum(std::string_view sv, object_store_config::signing_mode& m)
{
  static const std::unordered_map<std::string_view, object_store_config::signing_mode> map = {
    {"presigned", object_store_config::signing_mode::presigned},
    {"header", object_store_config::signing_mode::header},
  };
  auto it = map.find(sv);
  if (it != map.end()) {
    m = it->second;
    return true;
  }
  return false;
}

inline bool enum_to_string(object_store_config::signing_mode m, std::string& s)
{
  switch (m) {
    case object_store_config::signing_mode::presigned: s = "presigned"; return true;
    case object_store_config::signing_mode::header: s = "header"; return true;
  }
  return false;
}

/// Configuration for GCS reads via the S3-compatible XML API.
///
/// Two authentication modes are supported (mutually exclusive):
///   - HMAC: set @c hmac_access_key and @c hmac_secret_key. Requires a key
///     pair generated in GCS Console → Storage → Settings → Interoperability.
///   - Metadata server: set @c use_metadata_server = true. Works on GCE VMs
///     where the attached service account already has GCS bucket access. No
///     key management required.
///
/// The GCS backend is disabled when neither mode is configured.
struct gcs_object_store_config {
  /// Read transport. @c xml (default) uses the S3-compatible XML/HTTP API
  /// (@c gcs_async_ioctx). @c grpc uses the native google.storage.v2 gRPC
  /// ReadObject backend (@c gcs_grpc_ioctx) — required to exploit DirectPath /
  /// Rapid (zonal) buckets on a co-located GCE VM. Both reuse the same auth
  /// (HMAC / metadata-server / static token); gRPC ignores HMAC and uses the
  /// OAuth2 bearer token as call credentials.
  enum class transport { xml, grpc };
  transport gcs_transport = transport::xml;
  /// gRPC target host for the native backend (no scheme; ":443" appended if no
  /// port given). Ignored unless @c gcs_transport == grpc. For DirectPath on a
  /// co-located VM, use "google-c2p:///storage.googleapis.com".
  std::string grpc_endpoint = "storage.googleapis.com";

  /// Max concurrent read streams the gRPC reactor keeps in flight in TOTAL,
  /// split evenly across @c grpc_channels. Ignored unless
  /// @c gcs_transport == grpc. HTTP/2 streams are cheap (unlike curl
  /// connections), so this defaults high — too low a value serializes
  /// many-small-read scans (e.g. single-column COUNT over many row groups) into
  /// latency-bound waves. Raise further if reads stay network-bound.
  std::size_t grpc_max_streams = 128;

  /// Number of gRPC channels ("lanes"), each with its own TCP/ALTS connection,
  /// CompletionQueue and worker thread. Removes the per-channel HTTP/2
  /// concurrent-stream cap and the single-thread response-memcpy ceiling.
  /// Ignored unless @c gcs_transport == grpc.
  std::size_t grpc_channels = 4;

  /// BidiReadObject policy (Rapid Storage multi-range fast path):
  /// "auto" (default) probes per bucket and falls back to unary ReadObject
  /// when unsupported; "on" always uses it (errors surface); "off" never.
  /// Ignored unless @c gcs_transport == grpc.
  std::string grpc_bidi_reads = "auto";

  /// Target size for one transport read. Larger scatter-gather scan reads are
  /// split into sub-reads of ~this size so they parallelize across
  /// streams/channels. Ignored unless @c gcs_transport == grpc.
  std::size_t grpc_target_read_bytes = 16UL << 20;  // 16 MiB

  /// When true, build the gRPC channel for DirectPath: c2p resolver
  /// ("google-c2p:///<grpc_endpoint>") + GoogleDefaultCredentials (ALTS + the
  /// VM's compute service-account auth), bypassing the GFE on a co-located GCE
  /// VM — the fast path for Rapid (zonal) buckets. In this mode auth comes from
  /// GoogleDefaultCredentials, not the OAuth2 authorizer. Off-GCE this must stay
  /// false; gRPC auto-falls back to CFE/TLS if DirectPath can't be negotiated.
  /// Ignored unless @c gcs_transport == grpc.
  bool grpc_directpath = false;

  /// GCS HMAC key ID (typically starts with "GOOG1E"). Ignored when
  /// @c use_metadata_server is true.
  std::string hmac_access_key;
  /// GCS HMAC secret corresponding to @c hmac_access_key. Ignored when
  /// @c use_metadata_server is true.
  std::string hmac_secret_key;
  /// When true, authenticate via the GCE instance metadata server instead of
  /// HMAC keys. The VM must have a service account attached with GCS read
  /// access. Takes precedence over HMAC credentials when both are set.
  bool use_metadata_server = false;
  /// Service account identifier used in the metadata server token URL.
  /// Defaults to "default" which resolves to the VM's primary service account.
  /// Ignored when @c use_metadata_server is false.
  std::string metadata_service_account;
  /// Static OAuth2 bearer token injected verbatim into every GCS request.
  /// Intended for debugging only — tokens expire (typically after 1 hour).
  /// Takes precedence over both @c use_metadata_server and HMAC when non-empty.
  std::string static_bearer_token;
  /// GCS XML API endpoint. Defaults to the global GCS endpoint; override only for
  /// testing (e.g. a local GCS emulator).
  std::string endpoint = "https://storage.googleapis.com";
  /// PEM CA bundle for TLS verification (CURLOPT_CAINFO). Empty uses the system
  /// bundle — correct for public GCS. Point at a private CA for local emulators.
  std::string ca_bundle_path;
  /// Verify TLS peer + host certificate. Default true; false is INSECURE (dev/test only).
  bool tls_verify = true;
};

inline bool string_to_enum(std::string_view sv, gcs_object_store_config::transport& t)
{
  if (sv == "xml" || sv == "http" || sv == "https") {
    t = gcs_object_store_config::transport::xml;
    return true;
  }
  if (sv == "grpc") {
    t = gcs_object_store_config::transport::grpc;
    return true;
  }
  return false;
}

inline bool enum_to_string(gcs_object_store_config::transport t, std::string& s)
{
  switch (t) {
    case gcs_object_store_config::transport::xml: s = "xml"; return true;
    case gcs_object_store_config::transport::grpc: s = "grpc"; return true;
  }
  return false;
}

}  // namespace sirius::io
