//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "frontend/converters/chunking.h"

#include <vector>

#include "absl/memory/memory.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/substitute.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

// UTF-8 is at most 4 bytes. The follow chart explains the format of each
// UTF-8 character.
// Char. number range  |        UTF-8 octet sequence
//    (hexadecimal)    |              (binary)
// --------------------+---------------------------------------------
// 0000 0000-0000 007F | 0xxxxxxx
// 0000 0080-0000 07FF | 110xxxxx 10xxxxxx
// 0000 0800-0000 FFFF | 1110xxxx 10xxxxxx 10xxxxxx
// 0001 0000-0010 FFFF | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
//
// More detail in the spec: https://tools.ietf.org/html/rfc3629#page-4
const uint8_t kPartialUTF8Bytes = 1 << 7;  // 0b10000000
const uint8_t kUTF8TwoBytes = 3 << 6;      // 0b11000000
const uint8_t kUTF8ThreeBytes = 7 << 5;    // 0b11100000
const uint8_t kUTF8FourBytes = 15 << 4;    // 0b11110000
const int64_t kMaxUTF8CharSize = 4;

bool IsPartialUTF8(char input) {
  return (static_cast<int>(input) & kPartialUTF8Bytes) != 0;
}

int64_t RemovePartialUTF8(absl::string_view string, int64_t available) {
  for (int64_t pos = available - 1;
       pos >= std::max<int64_t>(available - kMaxUTF8CharSize, 0); pos--) {
    char partial = string[pos];
    // Only remove partial UTF-8 character.
    if ((partial & kUTF8FourBytes) == kUTF8FourBytes) {
      if (pos != available - 4) available = pos;
      break;
    } else if ((partial & kUTF8ThreeBytes) == kUTF8ThreeBytes) {
      if (pos != available - 3) available = pos;
      break;
    } else if ((partial & kUTF8TwoBytes) == kUTF8TwoBytes) {
      if (pos != available - 2) available = pos;
      break;
    }
  }
  return available;
}

// Constructs a set of PartialResultSets. Data will be chunked as necessary to
// comply with the Cloud Spanner streaming chunk size limit. Only Strings and
// Lists need to be chunked (Structs are not a valid column type and will return
// an error if encountered).
class ResultSetBuilder {
 public:
  explicit ResultSetBuilder(
      int64_t max_chunk_size,
      std::vector<google::spanner::v1::PartialResultSet>* results)
      : max_chunk_size_(max_chunk_size), results_(results) {
    if (results_->empty()) {
      results_->emplace_back();
    }
    current_chunk_size_ = results_->back().ByteSizeLong();
    stack_.push_back(results_->back().mutable_values());
  }

  // Adds the incoming value to the set of PartialResultSets chunking as
  // necessary.
  absl::Status AddValue(const protobuf::Value& value) {
    // If the current size exceeds the limit, create a new chunk.
    if (HasExceededChunkLimit()) {
      StartNewResultSet();
    }

    // Adds the value to the current result set. It will be chunked into pieces
    // if the size of a result set would exceed max_chunk_size_. In that case,
    // partial values will be added to the end of this result set and beginning
    // of the next one. The partial results will be merged back together by the
    // receiving client.
    auto value_size = value.ByteSizeLong();
    switch (value.kind_case()) {
      case protobuf::Value::kListValue: {
        // Check if list can fit into current chunk.
        if (current_chunk_size_ + value_size <= max_chunk_size_) {
          AddUnchunkedValue(value);
        } else {
          StartList();
          for (const auto& list_value : value.list_value().values()) {
            ZETASQL_RETURN_IF_ERROR(AddValue(list_value));
          }
          FinishList();
        }
        CheckListBoundary();
        break;
      }
      case protobuf::Value::kStringValue: {
        // Check if string can fit into current chunk.
        if (current_chunk_size_ + value_size <= max_chunk_size_) {
          AddUnchunkedValue(value);
        } else {
          AddString(value.string_value());
        }
        CheckStringBoundary();
        break;
      }
      case protobuf::Value::kBoolValue:
      case protobuf::Value::kNumberValue:
      case protobuf::Value::kNullValue:
        AddUnchunkedValue(value);
        break;

      default:
        return error::Internal(absl::Substitute(
            "Cannot convert value of type ($0) to a potentially "
            "chunked PartialResultSet.",
            value.GetTypeName()));
    }
    return absl::OkStatus();
  }

 private:
  ResultSetBuilder(const ResultSetBuilder&) = delete;
  ResultSetBuilder& operator=(const ResultSetBuilder&) = delete;

  bool HasExceededChunkLimit() {
    return current_chunk_size_ >= max_chunk_size_;
  }

  bool IsListOpen() { return stack_.size() > 1; }

  // Adds a value as the next value without chunking. The value will be added to
  // a list if there are any nested lists otherwise it will be added as the next
  // value in results. Used for the fast path when it is known this will not
  // need to be chunked.
  void AddUnchunkedValue(const protobuf::Value& value) {
    *stack_.back()->Add() = value;
    current_chunk_size_ += value.ByteSizeLong();
  }

  // If a nested list ends at the boundary of the chunk, we need to make sure
  // that an empty list is added at the beginning of the next chunk so they will
  // be merged together. Otherwise it could end up being incorrectly merged with
  // a disjoint list in the next chunk. We explicitly check for this to catch
  // edge cases.
  void CheckListBoundary() {
    if (HasExceededChunkLimit() && IsListOpen()) {
      StartNewResultSet();
      // Add and empty list to merge with the last list from the previous
      // chunk.
      StartList();
      FinishList();
    }
  }

  // If a string nested inside a list ends at the boundary of the chunk, we
  // need to make sure that an empty string is added at the beginning of the
  // next chunk so they will be merged together. Otherwise it could end up being
  // incorrectly merged with another string in the next chunk. We explicitly
  // check for this to catch edge cases.
  void CheckStringBoundary() {
    if (HasExceededChunkLimit() && IsListOpen()) {
      StartNewResultSet();
      // The last string ended within the previous chunk, so we don't want to
      // concatenate it with the next string. Add an empty string to prevent
      // this.
      AddUnchunkedString("");
    }
  }

  // Adds a string as the next value. The value will be added to a list if there
  // are any nested lists otherwise it will be added as the next value in
  // results.
  void AddString(absl::string_view str) {
    if (str.empty()) {
      // Handle empty string case.
      AddUnchunkedString("");
      return;
    }

    while (!str.empty()) {
      int64_t available = std::max(max_chunk_size_ - current_chunk_size_,
                                 static_cast<int64_t>(0));
      if (str.size() > available) {
        // Strings are UTF-8 encoded. Not all client libraries support a split
        // UTF-8 character. Flush the entire and not partial UTF-8 character.
        if (available > 0 && IsPartialUTF8(str[available - 1])) {
          available = RemovePartialUTF8(str, available);
        }
        // Chunk the string into pieces.
        AddUnchunkedString(str.substr(0, available));
        results_->back().set_chunked_value(true);
        StartNewResultSet();
        str.remove_prefix(available);
      } else {
        // String can fit into remaing space of current chunk.
        AddUnchunkedString(str);
        break;
      }
    }
  }

  // Adds an unchunked string to the current result set or list.
  void AddUnchunkedString(absl::string_view str) {
    auto value = stack_.back()->Add();
    value->mutable_string_value()->assign(str.data(), str.size());
    current_chunk_size_ += value->ByteSizeLong();
  }

  // Adds a list as the next value. The list will be nested in another list if
  // there are any lists currently in the stack otherwise it will be added as
  // the next value in results.
  void StartList() {
    auto value = stack_.back()->Add();
    stack_.push_back(value->mutable_list_value()->mutable_values());
    current_chunk_size_ += value->ByteSizeLong();
  }

  // Removes a list from the stack.
  void FinishList() { stack_.pop_back(); }

  // Adds a new partial result set to results. If list(s) are currently being
  // processed it will create corresponding list(s) in the new chunk. The
  // current result set will have chunked_value set to true if a list was
  // currently being processed or if a string is split up.
  void StartNewResultSet() {
    if (IsListOpen()) {
      // Always mark as chunked if inside a list.
      results_->back().set_chunked_value(true);
    }
    size_t stack_depth = stack_.size() - 1;
    stack_.clear();

    results_->emplace_back();
    stack_.push_back(results_->back().mutable_values());
    for (int i = 0; i < stack_depth; ++i) {
      auto list = stack_.back()->Add()->mutable_list_value();
      stack_.push_back(list->mutable_values());
    }
    // Reset the size of the current result set.
    current_chunk_size_ = results_->back().ByteSizeLong();
  }

  // The size of the current chunk that is being appended to. This is an
  // estimate of the current chunk size. This estimate should work fine in
  // practice since the max chunk size is 1MB and the default message size limit
  // is 4MB for gRPC. Since we do not explicitly track the metadata, our size
  // estimate could be off by as much as a factor of 2. However, this shouldn't
  // be a problem since it will be well below the gRPC limit.
  int64_t current_chunk_size_;

  // The maximum allowed size of a chunk.
  int64_t max_chunk_size_;

  // The list of PartialResultSets that store the resulting chunks.
  std::vector<::google::spanner::v1::PartialResultSet>* results_;

  // The list stack is used to track nested lists. When a result set is chunked
  // all current lists need to be truncated and matching versions created in the
  // next chunk.
  std::vector<google::protobuf::RepeatedPtrField<protobuf::Value>*> stack_;
};

}  // namespace

zetasql_base::StatusOr<std::vector<google::spanner::v1::PartialResultSet>>
ChunkResultSet(const google::spanner::v1::ResultSet& set,
               int64_t max_chunk_size) {
  std::vector<google::spanner::v1::PartialResultSet> results;
  results.emplace_back();
  *results.front().mutable_metadata() = set.metadata();

  ResultSetBuilder builder(max_chunk_size, &results);
  for (const auto& row : set.rows()) {
    for (const auto& value : row.values()) {
      ZETASQL_RETURN_IF_ERROR(builder.AddValue(value));
    }
  }
  return results;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
