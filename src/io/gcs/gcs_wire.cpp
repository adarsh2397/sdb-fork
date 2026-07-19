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

#include "io/gcs/gcs_wire.hpp"

namespace sirius::io::gcs::wire {

namespace {

// Protobuf wire types we handle. 3/4 (start/end group) are deprecated and, if
// seen, treated as malformed → fall back to typed parse.
constexpr std::uint32_t kWtVarint = 0;
constexpr std::uint32_t kWt64Bit  = 1;
constexpr std::uint32_t kWtLen    = 2;
constexpr std::uint32_t kWt32Bit  = 5;

/// Sequential forward reader over the slice sequence with a logical position.
/// Every read is bounds-checked against the total buffer; a read that would
/// run off the end returns false (the caller aborts and falls back).
class cursor {
 public:
  explicit cursor(slice_seq slices) : slices_(slices)
  {
    for (auto const& s : slices_) total_ += s.size();
  }

  [[nodiscard]] std::uint64_t pos() const noexcept { return pos_; }
  [[nodiscard]] std::uint64_t total() const noexcept { return total_; }

  [[nodiscard]] bool read_byte(std::uint8_t& out) noexcept
  {
    while (si_ < slices_.size() && off_ >= slices_[si_].size()) {
      ++si_;
      off_ = 0;
    }
    if (si_ >= slices_.size()) return false;
    out = slices_[si_][off_];
    ++off_;
    ++pos_;
    return true;
  }

  /// Base-128 varint, little-endian groups, max 10 bytes (64-bit).
  [[nodiscard]] bool read_varint(std::uint64_t& out) noexcept
  {
    std::uint64_t result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      std::uint8_t b = 0;
      if (!read_byte(b)) return false;
      result |= static_cast<std::uint64_t>(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        out = result;
        return true;
      }
    }
    return false;  // 10th byte still had the continuation bit → malformed
  }

  /// Advance `n` bytes without materializing them.
  [[nodiscard]] bool skip(std::uint64_t n) noexcept
  {
    if (n > total_ - pos_) return false;
    pos_ += n;
    while (n > 0) {
      if (si_ >= slices_.size()) return false;
      std::uint64_t avail = slices_[si_].size() - off_;
      if (avail == 0) {
        ++si_;
        off_ = 0;
        continue;
      }
      std::uint64_t take = avail < n ? avail : n;
      off_ += take;
      n -= take;
    }
    return true;
  }

  /// Skip a field's value given its wire type (LEN reads its own length).
  [[nodiscard]] bool skip_value(std::uint32_t wt) noexcept
  {
    switch (wt) {
      case kWtVarint: {
        std::uint64_t v = 0;
        return read_varint(v);
      }
      case kWt64Bit: return skip(8);
      case kWt32Bit: return skip(4);
      case kWtLen: {
        std::uint64_t len = 0;
        if (!read_varint(len)) return false;
        return skip(len);
      }
      default: return false;  // groups / unknown wire type → malformed
    }
  }

 private:
  slice_seq slices_;
  std::size_t si_{0};       // current slice index
  std::uint64_t off_{0};    // byte offset within slices_[si_]
  std::uint64_t pos_{0};    // logical position
  std::uint64_t total_{0};  // total bytes across all slices
};

/// Read one field header. Returns false at malformed input. On success sets
/// `field` and `wt`; the cursor sits at the field's value.
[[nodiscard]] bool read_tag(cursor& c, std::uint32_t& field, std::uint32_t& wt) noexcept
{
  std::uint64_t tag = 0;
  if (!c.read_varint(tag)) return false;
  field = static_cast<std::uint32_t>(tag >> 3);
  wt    = static_cast<std::uint32_t>(tag & 0x7);
  if (field == 0) return false;  // field 0 is illegal
  return true;
}

/// Walk a length-delimited ChecksummedData sub-message [current, end),
/// appending each `content` (field 1) span to `out`.
[[nodiscard]] bool walk_checksummed(cursor& c,
                                    std::uint64_t end,
                                    std::vector<content_extent>& out) noexcept
{
  while (c.pos() < end) {
    std::uint32_t field = 0, wt = 0;
    if (!read_tag(c, field, wt)) return false;
    if (field == fields::kChecksummedData_content && wt == kWtLen) {
      std::uint64_t len = 0;
      if (!c.read_varint(len)) return false;
      std::uint64_t start = c.pos();
      if (len > end - start) return false;  // content overruns the message
      out.push_back(content_extent{start, len});
      if (!c.skip(len)) return false;
    } else {
      if (!c.skip_value(wt)) return false;
    }
  }
  return c.pos() == end;
}

/// Walk a ReadRange sub-message [current, end); extract read_id (field 3).
[[nodiscard]] bool walk_read_range(cursor& c, std::uint64_t end, bidi_range_result& rr) noexcept
{
  while (c.pos() < end) {
    std::uint32_t field = 0, wt = 0;
    if (!read_tag(c, field, wt)) return false;
    if (field == fields::kReadRange_read_id && wt == kWtVarint) {
      std::uint64_t v = 0;
      if (!c.read_varint(v)) return false;
      rr.read_id     = static_cast<std::int64_t>(v);
      rr.has_read_id = true;
    } else {
      if (!c.skip_value(wt)) return false;
    }
  }
  return c.pos() == end;
}

/// Walk an ObjectRangeData sub-message [current, end).
[[nodiscard]] bool walk_object_range(cursor& c, std::uint64_t end, bidi_range_result& rr) noexcept
{
  while (c.pos() < end) {
    std::uint32_t field = 0, wt = 0;
    if (!read_tag(c, field, wt)) return false;
    if (field == fields::kObjectRangeData_checksummed_data && wt == kWtLen) {
      std::uint64_t len = 0;
      if (!c.read_varint(len)) return false;
      std::uint64_t sub_end = c.pos() + len;
      if (len > end - c.pos()) return false;
      rr.has_checksummed_data = true;
      if (!walk_checksummed(c, sub_end, rr.content)) return false;
    } else if (field == fields::kObjectRangeData_read_range && wt == kWtLen) {
      std::uint64_t len = 0;
      if (!c.read_varint(len)) return false;
      std::uint64_t sub_end = c.pos() + len;
      if (len > end - c.pos()) return false;
      if (!walk_read_range(c, sub_end, rr)) return false;
    } else if (field == fields::kObjectRangeData_range_end && wt == kWtVarint) {
      std::uint64_t v = 0;
      if (!c.read_varint(v)) return false;
      rr.range_end = (v != 0);
    } else {
      if (!c.skip_value(wt)) return false;
    }
  }
  return c.pos() == end;
}

}  // namespace

read_object_result extract_read_object_response(slice_seq slices)
{
  read_object_result r;
  cursor c(slices);
  std::uint64_t const end = c.total();
  while (c.pos() < end) {
    std::uint32_t field = 0, wt = 0;
    if (!read_tag(c, field, wt)) return r;  // ok stays false
    if (field == fields::kReadObjectResponse_checksummed_data && wt == kWtLen) {
      std::uint64_t len = 0;
      if (!c.read_varint(len)) return r;
      std::uint64_t sub_end = c.pos() + len;
      if (len > end - c.pos()) return r;
      if (!walk_checksummed(c, sub_end, r.content)) return r;
    } else {
      if (!c.skip_value(wt)) return r;
    }
  }
  r.ok = (c.pos() == end);
  return r;
}

bidi_result extract_bidi_read_object_response(slice_seq slices)
{
  bidi_result r;
  cursor c(slices);
  std::uint64_t const end = c.total();
  while (c.pos() < end) {
    std::uint32_t field = 0, wt = 0;
    if (!read_tag(c, field, wt)) return r;
    if (field == fields::kBidiReadObjectResponse_ranges && wt == kWtLen) {
      std::uint64_t len = 0;
      if (!c.read_varint(len)) return r;
      std::uint64_t sub_end = c.pos() + len;
      if (len > end - c.pos()) return r;
      bidi_range_result rr;
      if (!walk_object_range(c, sub_end, rr)) return r;
      r.ranges.push_back(std::move(rr));
    } else {
      if (!c.skip_value(wt)) return r;
    }
  }
  r.ok = (c.pos() == end);
  return r;
}

}  // namespace sirius::io::gcs::wire
