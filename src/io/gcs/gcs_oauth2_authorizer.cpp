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

#include "io/gcs/gcs_oauth2_authorizer.hpp"

#include "io/io_errors.hpp"
#include "io/uri_parser.hpp"

#include <curl/curl.h>

#include <cctype>
#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>

namespace sirius::io::gcs {

namespace {

/// Metadata server token URL for a given service account.
/// GCE metadata server requires the Metadata-Flavor: Google header.
std::string make_metadata_url(std::string_view service_account)
{
  return "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/" +
         std::string{service_account} + "/token";
}

/// libcurl write callback — appends received bytes to a std::string.
std::size_t write_to_string(void* ptr, std::size_t size, std::size_t nmemb, void* userdata)
{
  auto* out = static_cast<std::string*>(userdata);
  out->append(static_cast<char*>(ptr), size * nmemb);
  return size * nmemb;
}

/// Minimal JSON field extractor — pulls the string value for @p key from a
/// flat (non-nested) JSON object. Returns empty string if not found. This
/// avoids a JSON library dependency; the metadata server response is a
/// well-defined, non-nested object so a simple scan is safe.
std::string extract_json_string(std::string_view json, std::string_view key)
{
  // Look for "key" : "value" or "key":"value"
  std::string needle = "\"" + std::string{key} + "\"";
  auto pos           = json.find(needle);
  if (pos == std::string_view::npos) return {};
  pos += needle.size();
  // Skip whitespace and colon
  while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':'))
    ++pos;
  if (pos >= json.size() || json[pos] != '"') return {};
  ++pos;  // skip opening quote
  std::string value;
  while (pos < json.size() && json[pos] != '"') {
    if (json[pos] == '\\' && pos + 1 < json.size()) {
      ++pos;  // skip escape prefix; take literal char
    }
    value.push_back(json[pos++]);
  }
  return value;
}

/// Extract a JSON number field (e.g. expires_in). Returns -1 if not found.
long extract_json_number(std::string_view json, std::string_view key)
{
  std::string needle = "\"" + std::string{key} + "\"";
  auto pos           = json.find(needle);
  if (pos == std::string_view::npos) return -1;
  pos += needle.size();
  while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':'))
    ++pos;
  if (pos >= json.size()) return -1;
  long val    = 0;
  bool digits = false;
  while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
    val    = val * 10 + (json[pos++] - '0');
    digits = true;
  }
  return digits ? val : -1;
}

/// Parse and validate the GCS endpoint into (scheme, host).
std::pair<std::string, std::string> parse_gcs_endpoint(std::string_view endpoint)
{
  auto sep = endpoint.find("://");
  if (sep == std::string_view::npos) {
    throw credential_error(
      "gcs_metadata_server_authorizer: endpoint missing scheme (expected https://)");
  }
  std::string scheme{endpoint.substr(0, sep)};
  for (auto& c : scheme)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (scheme != "http" && scheme != "https") {
    throw credential_error(
      "gcs_metadata_server_authorizer: endpoint scheme must be http or https (got '" + scheme +
      "')");
  }
  std::string host{endpoint.substr(sep + 3)};
  auto bad = host.find_first_of("/?#");
  if (bad != std::string::npos) {
    throw credential_error(
      "gcs_metadata_server_authorizer: endpoint must be scheme://host[:port] only");
  }
  if (host.empty()) {
    throw credential_error("gcs_metadata_server_authorizer: endpoint missing host");
  }
  for (auto& c : host)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return {std::move(scheme), std::move(host)};
}

}  // namespace

gcs_metadata_server_authorizer::gcs_metadata_server_authorizer(std::string service_account,
                                                                std::string gcs_endpoint)
  : _service_account(std::move(service_account)), _gcs_endpoint(gcs_endpoint)
{
  if (_service_account.empty()) {
    throw credential_error(
      "gcs_metadata_server_authorizer: service_account must be non-empty");
  }
  auto [scheme, host] = parse_gcs_endpoint(gcs_endpoint);
  _scheme             = std::move(scheme);
  _host               = std::move(host);
}

void gcs_metadata_server_authorizer::refresh_token_locked()
{
  // One-shot libcurl handle — token fetches are rare (once per ~hour).
  CURL* h = curl_easy_init();
  if (!h) throw credential_error("gcs_metadata_server_authorizer: curl_easy_init failed");

  std::string body;
  std::string url = make_metadata_url(_service_account);

  // GCE metadata server requires the Metadata-Flavor: Google header.
  curl_slist* hdrs = nullptr;
  hdrs             = curl_slist_append(hdrs, "Metadata-Flavor: Google");

  curl_easy_setopt(h, CURLOPT_URL, url.c_str());
  curl_easy_setopt(h, CURLOPT_HTTPHEADER, hdrs);
  curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, write_to_string);
  curl_easy_setopt(h, CURLOPT_WRITEDATA, &body);
  // Short timeout — metadata server is local to the VM.
  curl_easy_setopt(h, CURLOPT_TIMEOUT, 10L);

  CURLcode rc = curl_easy_perform(h);
  curl_slist_free_all(hdrs);

  long http_code = 0;
  curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &http_code);
  curl_easy_cleanup(h);

  if (rc != CURLE_OK) {
    throw credential_error(
      std::string("gcs_metadata_server_authorizer: metadata server request failed: ") +
      curl_easy_strerror(rc));
  }
  if (http_code < 200 || http_code >= 300) {
    throw credential_error("gcs_metadata_server_authorizer: metadata server returned HTTP " +
                           std::to_string(http_code) + ": " + body);
  }

  // Response JSON: {"access_token":"...","expires_in":3599,"token_type":"Bearer"}
  std::string token      = extract_json_string(body, "access_token");
  long expires_in        = extract_json_number(body, "expires_in");

  if (token.empty()) {
    throw credential_error(
      "gcs_metadata_server_authorizer: could not parse access_token from metadata response: " +
      body);
  }
  if (expires_in <= 0) { expires_in = 3600; }  // safe fallback

  _token        = std::move(token);
  _token_expiry = std::chrono::steady_clock::now() +
                  std::chrono::seconds{expires_in - kRefreshLeadSeconds};
}

sirius::io::s3::s3_authorized_request gcs_metadata_server_authorizer::authorize(
  sirius::io::s3::s3_object_ref const& obj,
  sirius::io::s3::s3_request_method /*method*/,
  std::chrono::seconds /*timeout*/)
{
  if (obj.bucket.empty()) {
    throw credential_error("gcs_metadata_server_authorizer: empty bucket");
  }
  if (obj.key.empty()) {
    throw credential_error("gcs_metadata_server_authorizer: empty key");
  }

  std::string token;
  {
    std::lock_guard lk{_mtx};
    if (_token.empty() || std::chrono::steady_clock::now() >= _token_expiry) {
      refresh_token_locked();
    }
    token = _token;
  }

  // Build path-style URL: scheme://host/bucket/key
  // Key may contain '/' for nested objects — preserve as-is.
  std::string url = _scheme + "://" + _host + "/" + obj.bucket + "/" + obj.key;

  return sirius::io::s3::s3_authorized_request{
    std::move(url),
    {{"Authorization", "Bearer " + token}},
  };
}

}  // namespace sirius::io::gcs
