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

// gcs_bidi_probe — minimal standalone repro for the Rapid Storage (zonal
// bucket) BidiReadObject redirect handshake, isolated from all of Sirius.
//
// Uses the SAME generated stubs and the SAME pixi-provided gRPC as the Sirius
// extension, but the trivial synchronous API: open a BidiReadObject stream for
// one object, request the first KiB, follow redirects (routing_token echoed in
// BOTH x-goog-request-params and the spec, exactly like google-cloud-cpp's
// OpenObject::RequestParams), and print every hop.
//
// Outcomes:
//   "ZONAL BIDI READ OK after N redirect(s)"  -> environment + gRPC build are
//       fine; the bug is in Sirius's reactor and we diff against this tool.
//   redirect loop here too                    -> environment / gRPC-build
//       issue (c2p fallback, RLS not engaging, grpc < 1.56 needing
//       GRPC_EXPERIMENTAL_XDS_RLS_LB=true) — no client code can fix it;
//       compare `pixi list | grep grpc` and rerun with the GRPC_TRACE env.
//
// Build (on the GCE box, from the repo root, AFTER a normal Sirius build so
// the generated protos + static lib exist):
//
//   GEN=$(find build -type d -name gcs_grpc_gen | head -1)
//   LIB=$(find build -name "libgcs_storage_protos.a" | head -1)
//   pixi run bash -c "g++ -std=c++20 -I\"$GEN\" tools/gcs_bidi_probe.cc \"$LIB\" \
//     \$(pkg-config --cflags --libs grpc++ protobuf) -o /tmp/gcs_bidi_probe"
//
// Run:
//   /tmp/gcs_bidi_probe <bucket> <object>            # DirectPath (google-c2p)
//   /tmp/gcs_bidi_probe <bucket> <object> --cfe      # force CFE for comparison
//
//   # and with routing traces:
//   GRPC_TRACE=google_c2p_resolver,rls_lb,xds_client,pick_first \
//   GRPC_VERBOSITY=DEBUG /tmp/gcs_bidi_probe <bucket> <object> 2>trace.log

#include <grpcpp/grpcpp.h>

#include "google/rpc/status.pb.h"
#include "google/storage/v2/storage.grpc.pb.h"
#include "google/storage/v2/storage.pb.h"

#include <cstdio>
#include <memory>
#include <optional>
#include <string>

namespace v2 = google::storage::v2;

namespace {

std::optional<v2::BidiReadObjectRedirectedError> extract_redirect(grpc::Status const& status)
{
  if (status.error_details().empty()) return std::nullopt;
  google::rpc::Status rpc_status;
  if (!rpc_status.ParseFromString(status.error_details())) return std::nullopt;
  for (auto const& any : rpc_status.details()) {
    v2::BidiReadObjectRedirectedError redirect;
    if (any.UnpackTo(&redirect)) return redirect;
  }
  return std::nullopt;
}

}  // namespace

int main(int argc, char** argv)
{
  if (argc < 3) {
    std::fprintf(stderr, "usage: %s <bucket> <object> [--cfe]\n", argv[0]);
    return 2;
  }
  std::string const bucket = argv[1];
  std::string const object = argv[2];
  bool const use_cfe       = argc > 3 && std::string(argv[3]) == "--cfe";

  std::string const bucket_path = "projects/_/buckets/" + bucket;
  std::string const target =
    use_cfe ? "storage.googleapis.com:443" : "google-c2p:///storage.googleapis.com";

  std::printf("grpc version : %s\n", grpc::Version().c_str());
  std::printf("target       : %s\n", target.c_str());
  std::printf("object       : gs://%s/%s\n\n", bucket.c_str(), object.c_str());

  auto channel = grpc::CreateChannel(target, grpc::GoogleDefaultCredentials());
  auto stub    = v2::Storage::NewStub(channel);

  std::string routing_token;                 // from redirects
  std::optional<std::string> read_handle;    // from redirects/responses

  for (int attempt = 0; attempt <= 5; ++attempt) {
    // Header exactly like google-cloud-cpp OpenObject::RequestParams():
    // bucket RAW (literal slashes) + routing_token RAW. Nothing encoded.
    std::string params = "bucket=" + bucket_path;
    if (!routing_token.empty()) params += "&routing_token=" + routing_token;

    grpc::ClientContext ctx;
    ctx.AddMetadata("x-goog-request-params", params);
    std::printf("[attempt %d] x-goog-request-params=[%s]\n", attempt, params.c_str());

    auto stream = stub->BidiReadObject(&ctx);

    v2::BidiReadObjectRequest req;
    auto* spec = req.mutable_read_object_spec();
    spec->set_bucket(bucket_path);
    spec->set_object(object);
    if (!routing_token.empty()) spec->set_routing_token(routing_token);
    if (read_handle) spec->mutable_read_handle()->set_handle(*read_handle);
    auto* rr = req.add_read_ranges();
    rr->set_read_offset(0);
    rr->set_read_length(1024);
    rr->set_read_id(1);

    if (!stream->Write(req)) {
      std::printf("[attempt %d] Write failed (stream broken before send)\n", attempt);
    } else {
      v2::BidiReadObjectResponse resp;
      std::size_t got = 0;
      while (stream->Read(&resp)) {
        if (resp.has_read_handle()) read_handle = resp.read_handle().handle();
        for (auto const& rd : resp.object_data_ranges()) {
          if (rd.has_checksummed_data()) got += rd.checksummed_data().content().size();
          if (rd.range_end()) {
            std::printf("\nZONAL BIDI READ OK after %d redirect(s): %zu bytes of read_id=%lld\n",
                        attempt,
                        got,
                        static_cast<long long>(rd.read_range().read_id()));
            stream->WritesDone();
            (void)stream->Finish();
            return 0;
          }
        }
      }
    }

    auto status = stream->Finish();
    std::printf("[attempt %d] status=%d %s (details=%zu bytes)\n",
                attempt,
                static_cast<int>(status.error_code()),
                status.error_message().c_str(),
                status.error_details().size());
    auto redirect = extract_redirect(status);
    if (!redirect) {
      std::printf("[attempt %d] no BidiReadObjectRedirectedError in details -> giving up\n",
                  attempt);
      return 1;
    }
    if (!redirect->routing_token().empty()) routing_token = redirect->routing_token();
    if (redirect->has_read_handle()) read_handle = redirect->read_handle().handle();
    std::printf("[attempt %d] redirect: routing_token=%zu chars, read_handle=%s\n",
                attempt,
                routing_token.size(),
                read_handle ? "set" : "absent");
  }

  std::printf("\nREDIRECT LOOP: token echoed per google-cloud-cpp but never resolved.\n"
              "This means the client-side DirectPath routing (c2p/xDS/RLS) is not acting on\n"
              "the token in THIS gRPC build/process. Next steps:\n"
              "  1. pixi list | grep grpc   (RLS-in-xDS is default-on only >= 1.56)\n"
              "  2. retry with GRPC_EXPERIMENTAL_XDS_RLS_LB=true\n"
              "  3. rerun with GRPC_TRACE=google_c2p_resolver,rls_lb,xds_client to see whether\n"
              "     c2p engaged at all or silently fell back to DNS/CFE\n");
  return 1;
}
