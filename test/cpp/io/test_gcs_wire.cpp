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

// Cross-checks the hand-rolled protobuf-wire content extractor (gcs_wire)
// against the GENERATED google.storage.v2 protos: build a message with the real
// generated code, serialize it, split the bytes into slices (including
// byte-per-slice to stress cross-slice spans), run the extractor, and assert
// its output equals what a full parse would yield. Because the reference is the
// generated code itself, these tests pin the extractor to the true wire format
// and will fail loudly if a field number or the wire walk is wrong.

#include "catch.hpp"

#include "io/gcs/gcs_wire.hpp"

#include "google/storage/v2/storage.pb.h"

#include <cstdint>
#include <string>
#include <vector>

namespace wire = sirius::io::gcs::wire;
namespace v2   = google::storage::v2;

namespace {

// View `buf` as a sequence of slices of at most `chunk` bytes each. The views
// point into `buf`, which must outlive them. chunk==1 exercises the maximal
// cross-slice-spanning path (every varint and every content field straddles).
std::vector<wire::byte_view> chunk_views(std::string const& buf, std::size_t chunk)
{
  std::vector<wire::byte_view> views;
  auto const* p = reinterpret_cast<std::uint8_t const*>(buf.data());
  if (chunk == 0) chunk = buf.size() == 0 ? 1 : buf.size();
  for (std::size_t off = 0; off < buf.size(); off += chunk) {
    views.push_back(wire::byte_view{p + off, std::min(chunk, buf.size() - off)});
  }
  return views;
}

std::string materialize(wire::slice_seq slices, std::vector<wire::content_extent> const& exts)
{
  std::string out;
  for (auto const& e : exts) {
    wire::for_each_span(slices, e, [&](std::uint8_t const* ptr, std::size_t n) {
      out.append(reinterpret_cast<char const*>(ptr), n);
    });
  }
  return out;
}

// Content that deliberately contains bytes resembling protobuf tags/varints
// (0x0A = field-1 LEN tag, 0x80 continuation, 0x00, 0xFF) to prove the
// length-delimited content is skipped wholesale, never parsed into.
std::string tricky_payload(std::size_t n)
{
  std::string s;
  s.reserve(n);
  std::uint8_t x = 0;
  for (std::size_t i = 0; i < n; ++i) {
    // deterministic pseudo-random-ish byte stream incl. tag-like bytes
    x = static_cast<std::uint8_t>((x * 31 + 7 + i) & 0xFF);
    if ((i % 5) == 0) x = 0x0A;
    if ((i % 7) == 0) x = 0x80;
    s.push_back(static_cast<char>(x));
  }
  return s;
}

}  // namespace

TEST_CASE("gcs_wire: ReadObjectResponse content extraction", "[gcs_wire][readobject]")
{
  auto const chunk = GENERATE(as<std::size_t>{}, 0, 1, 3, 7, 64);

  SECTION("single content field round-trips at every slice granularity")
  {
    std::string const payload = tricky_payload(1000);
    v2::ReadObjectResponse msg;
    msg.mutable_checksummed_data()->set_content(payload);

    std::string buf;
    REQUIRE(msg.SerializeToString(&buf));
    auto slices = chunk_views(buf, chunk);

    auto r = wire::extract_read_object_response(slices);
    REQUIRE(r.ok);
    REQUIRE(materialize(slices, r.content) == payload);
  }

  SECTION("crc32c (unknown to the extractor) is skipped, content still exact")
  {
    std::string const payload = tricky_payload(300);
    v2::ReadObjectResponse msg;
    auto* cd = msg.mutable_checksummed_data();
    cd->set_content(payload);
    cd->set_crc32c(0xDEADBEEF);

    std::string buf;
    REQUIRE(msg.SerializeToString(&buf));
    auto slices = chunk_views(buf, chunk);

    auto r = wire::extract_read_object_response(slices);
    REQUIRE(r.ok);
    REQUIRE(materialize(slices, r.content) == payload);
  }

  SECTION("empty message yields ok with no content")
  {
    v2::ReadObjectResponse msg;
    std::string buf;
    REQUIRE(msg.SerializeToString(&buf));
    auto slices = chunk_views(buf, chunk);
    auto r      = wire::extract_read_object_response(slices);
    REQUIRE(r.ok);
    REQUIRE(r.content.empty());
  }
}

TEST_CASE("gcs_wire: ReadObjectResponse malformed input falls back", "[gcs_wire][readobject][error]")
{
  std::string const payload = tricky_payload(500);
  v2::ReadObjectResponse msg;
  msg.mutable_checksummed_data()->set_content(payload);
  std::string buf;
  REQUIRE(msg.SerializeToString(&buf));

  SECTION("truncated mid-content -> ok == false")
  {
    buf.resize(buf.size() - 100);  // chop into the content value
    auto slices = chunk_views(buf, 0);
    auto r      = wire::extract_read_object_response(slices);
    REQUIRE_FALSE(r.ok);
  }

  SECTION("truncated mid-length-prefix -> ok == false")
  {
    buf.resize(2);
    auto slices = chunk_views(buf, 0);
    auto r      = wire::extract_read_object_response(slices);
    REQUIRE_FALSE(r.ok);
  }
}

TEST_CASE("gcs_wire: BidiReadObjectResponse multi-range extraction", "[gcs_wire][bidi]")
{
  auto const chunk = GENERATE(as<std::size_t>{}, 0, 1, 5, 64);

  SECTION("ranges with content + read_id + range_end, in order")
  {
    struct expect {
      std::int64_t id;
      bool end;
      std::string data;
    };
    std::vector<expect> want = {
      {1, false, tricky_payload(200)},
      {2, false, tricky_payload(50)},
      {7, true, tricky_payload(777)},
    };

    v2::BidiReadObjectResponse msg;
    for (auto const& w : want) {
      auto* rd = msg.add_object_data_ranges();
      rd->mutable_checksummed_data()->set_content(w.data);
      rd->mutable_read_range()->set_read_id(w.id);
      rd->set_range_end(w.end);
    }
    // Top-level fields the extractor should skip:
    msg.mutable_read_handle()->set_handle("opaque-handle-bytes");

    std::string buf;
    REQUIRE(msg.SerializeToString(&buf));
    auto slices = chunk_views(buf, chunk);

    auto r = wire::extract_bidi_read_object_response(slices);
    REQUIRE(r.ok);
    REQUIRE(r.ranges.size() == want.size());
    for (std::size_t i = 0; i < want.size(); ++i) {
      INFO("range index " << i);
      REQUIRE(r.ranges[i].has_read_id);
      REQUIRE(r.ranges[i].read_id == want[i].id);
      REQUIRE(r.ranges[i].range_end == want[i].end);
      REQUIRE(r.ranges[i].has_checksummed_data);
      REQUIRE(materialize(slices, r.ranges[i].content) == want[i].data);
    }
  }

  SECTION("range with only read_range (no checksummed_data)")
  {
    v2::BidiReadObjectResponse msg;
    auto* rd = msg.add_object_data_ranges();
    rd->mutable_read_range()->set_read_id(42);
    rd->set_range_end(true);

    std::string buf;
    REQUIRE(msg.SerializeToString(&buf));
    auto slices = chunk_views(buf, chunk);

    auto r = wire::extract_bidi_read_object_response(slices);
    REQUIRE(r.ok);
    REQUIRE(r.ranges.size() == 1);
    REQUIRE(r.ranges[0].read_id == 42);
    REQUIRE(r.ranges[0].range_end);
    REQUIRE_FALSE(r.ranges[0].has_checksummed_data);
    REQUIRE(r.ranges[0].content.empty());
  }

  SECTION("empty message -> ok, no ranges")
  {
    v2::BidiReadObjectResponse msg;
    std::string buf;
    REQUIRE(msg.SerializeToString(&buf));
    auto slices = chunk_views(buf, chunk);
    auto r      = wire::extract_bidi_read_object_response(slices);
    REQUIRE(r.ok);
    REQUIRE(r.ranges.empty());
  }
}

TEST_CASE("gcs_wire: BidiReadObjectResponse property sweep vs generated parse",
          "[gcs_wire][bidi][property]")
{
  // Deterministic LCG so failures reproduce.
  std::uint64_t rng = 0x1234'5678'9abc'def0ULL;
  auto next         = [&rng]() {
    rng = rng * 6364136223846793005ULL + 1442695040888963407ULL;
    return rng >> 33;
  };

  for (int trial = 0; trial < 200; ++trial) {
    v2::BidiReadObjectResponse msg;
    int const n_ranges = static_cast<int>(next() % 6);
    std::vector<std::string> datas;
    std::vector<std::int64_t> ids;
    std::vector<bool> ends;
    for (int i = 0; i < n_ranges; ++i) {
      auto* rd            = msg.add_object_data_ranges();
      bool const has_data = (next() % 4) != 0;
      std::string d;
      if (has_data) {
        d = tricky_payload(next() % 400);
        rd->mutable_checksummed_data()->set_content(d);
      }
      auto id       = static_cast<std::int64_t>(next() % 100000);
      bool const en = (next() % 2) == 0;
      rd->mutable_read_range()->set_read_id(id);
      rd->set_range_end(en);
      datas.push_back(std::move(d));
      ids.push_back(id);
      ends.push_back(en);
    }

    std::string buf;
    REQUIRE(msg.SerializeToString(&buf));
    std::size_t const chunk = 1 + (next() % 17);  // vary slice granularity
    auto slices             = chunk_views(buf, chunk);

    auto r = wire::extract_bidi_read_object_response(slices);
    INFO("trial " << trial << " n_ranges " << n_ranges << " chunk " << chunk);
    REQUIRE(r.ok);
    REQUIRE(r.ranges.size() == static_cast<std::size_t>(n_ranges));
    for (int i = 0; i < n_ranges; ++i) {
      REQUIRE(r.ranges[i].read_id == ids[i]);
      REQUIRE(r.ranges[i].range_end == ends[i]);
      REQUIRE(materialize(slices, r.ranges[i].content) == datas[i]);
    }
  }
}
