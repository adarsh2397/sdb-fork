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

#pragma once

// -----------------------------------------------------------------------------
// gcs_wire — zero-extra-copy extraction of ChecksummedData.content spans from
// google.storage.v2 ReadObjectResponse / BidiReadObjectResponse messages.
//
// The GCS gRPC read hot path spends most of its CPU copying object bytes twice
// on the CQ worker: once when protobuf parses ChecksummedData.content into a
// std::string, and again when we memcpy that string into the pinned staging
// block. This module removes the FIRST copy: instead of letting protobuf parse
// the (huge) content field, we walk the protobuf wire format ourselves, locate
// the content byte ranges in the received gRPC slices, and hand those ranges
// straight to the destination cursor — the content bytes are never copied into
// an intermediate string. The tiny control fields (read_id, range_end) are
// extracted inline during the same single pass.
//
// This file is intentionally free of any protobuf or gRPC dependency: it
// operates purely on a sequence of byte spans (the gRPC ByteBuffer slices,
// converted to spans by the caller), so it is trivially unit-testable. The
// field numbers below are the wire contract of google.storage.v2 and are
// covered by protobuf's backwards-compatibility guarantee.
//
// Robustness: any malformed / unexpected input (truncated varint, length
// overrun, oversize nesting) makes the extractor return `ok == false`; the
// reactor then falls back to a normal typed protobuf parse for that message,
// so correctness never depends on this walk being exhaustive.
// -----------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace sirius::io::gcs::wire {

/// One contiguous received slice (no ownership; points into the gRPC ByteBuffer
/// that the caller keeps alive for the duration of extraction + copy-out).
using byte_view = std::span<const std::uint8_t>;

/// The gRPC ByteBuffer's slice sequence for one message, in order.
using slice_seq = std::span<const byte_view>;

/// A run of object bytes inside the logical concatenation of `slice_seq`,
/// expressed as a logical offset + length (a single `ChecksummedData.content`
/// value may span multiple slices; `for_each_span` walks the pieces).
struct content_extent {
  std::uint64_t offset{0};
  std::uint64_t length{0};
};

/// Result of walking a ReadObjectResponse (unary ReadObject stream message).
struct read_object_result {
  bool ok{false};
  /// content spans in wire order (normally exactly one per message).
  std::vector<content_extent> content;
};

/// Per `object_data_ranges` entry of a BidiReadObjectResponse.
struct bidi_range_result {
  std::int64_t read_id{0};
  bool has_read_id{false};
  bool range_end{false};
  bool has_checksummed_data{false};
  /// content spans for this range in wire order (normally one).
  std::vector<content_extent> content;
};

/// Result of walking a BidiReadObjectResponse. `ranges` is in wire order, which
/// matches the order the reactor pairs against its outstanding read_ids.
struct bidi_result {
  bool ok{false};
  std::vector<bidi_range_result> ranges;
};

/// Field-number contract (google.storage.v2). Stable by protobuf compat rules.
namespace fields {
inline constexpr std::uint32_t kReadObjectResponse_checksummed_data = 1;
inline constexpr std::uint32_t kChecksummedData_content             = 1;
inline constexpr std::uint32_t kBidiReadObjectResponse_ranges       = 1;
inline constexpr std::uint32_t kObjectRangeData_checksummed_data    = 1;
inline constexpr std::uint32_t kObjectRangeData_read_range          = 2;
inline constexpr std::uint32_t kObjectRangeData_range_end           = 3;
inline constexpr std::uint32_t kReadRange_read_id                   = 3;
}  // namespace fields

/// Walk a serialized ReadObjectResponse. `slices` must hold exactly one
/// message's bytes (gRPC delivers one de-framed, de-compressed message per
/// Read()).
[[nodiscard]] read_object_result extract_read_object_response(slice_seq slices);

/// Walk a serialized BidiReadObjectResponse (one message's bytes).
[[nodiscard]] bidi_result extract_bidi_read_object_response(slice_seq slices);

/// Invoke `sink(const std::uint8_t* ptr, std::size_t len)` for each contiguous
/// piece of `ext`, in logical order. Zero-copy: pointers refer into `slices`.
/// Feeds a scatter destination (e.g. range_state::append) directly. Returns the
/// total bytes visited (== ext.length unless the extent overruns the slices,
/// which cannot happen for an extent produced by the extractors above).
template <class Sink>
std::size_t for_each_span(slice_seq slices, content_extent ext, Sink&& sink)
{
  std::uint64_t remaining = ext.length;
  std::uint64_t skip      = ext.offset;
  std::size_t visited     = 0;
  for (auto const& s : slices) {
    if (remaining == 0) break;
    std::uint64_t slen = s.size();
    if (skip >= slen) {  // extent starts after this slice
      skip -= slen;
      continue;
    }
    std::uint64_t avail = slen - skip;
    std::uint64_t take  = avail < remaining ? avail : remaining;
    sink(s.data() + skip, static_cast<std::size_t>(take));
    visited += static_cast<std::size_t>(take);
    remaining -= take;
    skip = 0;
  }
  return visited;
}

}  // namespace sirius::io::gcs::wire
