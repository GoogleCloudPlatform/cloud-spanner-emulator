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

#include "tests/common/chunking.h"

#include "google/protobuf/struct.pb.h"
#include "absl/memory/memory.h"
#include "absl/random/random.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/types/optional.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace {

void AppendToList(google::protobuf::ListValue* from, int start_index,
                  google::protobuf::ListValue* to) {
  for (int i = start_index; i < from->values_size(); ++i) {
    to->add_values()->Swap(from->mutable_values(i));
  }
}

void MergeList(google::protobuf::ListValue* merged_list,
               google::protobuf::ListValue* input_list) {
  if (input_list->values().empty()) return;

  if (merged_list->values().empty()) {
    merged_list->mutable_values()->Swap(input_list->mutable_values());
    return;
  }

  auto* last_value =
      merged_list->mutable_values(merged_list->values_size() - 1);
  auto* next_value = input_list->mutable_values(0);
  if (last_value->kind_case() != next_value->kind_case()) {
    AppendToList(input_list, 0, merged_list);
  } else if (next_value->kind_case() == google::protobuf::Value::kListValue) {
    MergeList(last_value->mutable_list_value(),
              next_value->mutable_list_value());
    AppendToList(input_list, 1, merged_list);

  } else if (next_value->kind_case() == google::protobuf::Value::kStringValue) {
    last_value->mutable_string_value()->append(next_value->string_value());
    AppendToList(input_list, 1, merged_list);
  } else {
    AppendToList(input_list, 0, merged_list);
  }
}

protobuf::Value::KindCase GetRandomValueType(absl::BitGen* gen,
                                             int string_weight, int list_weight,
                                             int number_weight,
                                             int bool_weight) {
  int weight_sum = string_weight + list_weight + number_weight + bool_weight;
  int string_threshold = string_weight;
  int list_threshold = string_threshold + list_weight;
  int number_threshold = list_threshold + number_weight;

  int rand =
      absl::Uniform<int>(absl::IntervalClosedClosed, *gen, 1, weight_sum);
  if (rand <= string_threshold) {
    return protobuf::Value::kStringValue;
  } else if (rand <= list_threshold) {
    return protobuf::Value::kListValue;
  } else if (rand <= number_threshold) {
    return protobuf::Value::kNumberValue;
  } else {
    return protobuf::Value::kBoolValue;
  }
}

constexpr int kStringWeight = 2;
constexpr int kListWeight = 2;
constexpr int kNumberWeight = 1;
constexpr int kBoolWeight = 1;

constexpr int kStringMinLength = 0;
constexpr int kStringMaxLength = 100;
constexpr int kValueMinLength = 0;
constexpr int kValueMaxLength = 100;

constexpr int kMaxStackDepth = 9;

}  // namespace

zetasql_base::StatusOr<google::spanner::v1::ResultSet> MergePartialResultSets(
    const std::vector<google::spanner::v1::PartialResultSet>& results,
    int columns_per_row) {
  google::spanner::v1::ResultSet out;
  google::protobuf::ListValue* current_row = nullptr;
  google::protobuf::Value prev_value;
  for (const auto& result : results) {
    if (result.has_metadata()) {
      *out.mutable_metadata() = result.metadata();
    }
    if (result.has_stats()) {
      *out.mutable_stats() = result.stats();
    }
    for (int i = 0; i < result.values_size(); ++i) {
      auto next_value = result.values(i);
      if (result.chunked_value() && i == result.values_size() - 1) {
        if (next_value.has_list_value()) {
          // List value
          if (next_value.list_value().values_size() > 0) {
            MergeList(prev_value.mutable_list_value(),
                      next_value.mutable_list_value());
          }
        } else {
          // String value
          prev_value.mutable_string_value()->append(next_value.string_value());
        }
      } else {
        if (current_row == nullptr ||
            current_row->values_size() == columns_per_row) {
          current_row = out.add_rows();
        }
        // Need to merge first value with the last value from the previous
        // chunk. We explicitly check the size of the previous value and ignore
        // empty values (size == 0), since they do not need to be merged.
        if (prev_value.ByteSizeLong() > 0) {
          if (i != 0) {
            return error::Internal(
                "Invalid index within chunk merging. Last value of previous "
                "chunk must be merged with first value of next chunk.");
          }
          if (prev_value.has_list_value()) {
            MergeList(prev_value.mutable_list_value(),
                      next_value.mutable_list_value());
          } else {
            prev_value.mutable_string_value()->append(
                next_value.string_value());
          }
          current_row->add_values()->Swap(&prev_value);
        } else {
          current_row->add_values()->Swap(&next_value);
        }
      }
    }
  }

  return out;
}

zetasql_base::StatusOr<google::spanner::v1::ResultSet> GenerateRandomResultSet(
    absl::BitGen* gen, int num_values) {
  google::spanner::v1::ResultSet result;
  result.mutable_metadata();
  auto row = result.add_rows();
  std::vector<google::protobuf::RepeatedPtrField<protobuf::Value>*> stack;
  stack.push_back(row->mutable_values());
  for (int i = 0; i < num_values; ++i) {
    protobuf::Value::KindCase type = GetRandomValueType(
        gen, kStringWeight, kListWeight, kNumberWeight, kBoolWeight);
    switch (type) {
      case protobuf::Value::kStringValue: {
        size_t length =
            absl::Uniform<size_t>(absl::IntervalClosedClosed, *gen,
                                  kStringMinLength, kStringMaxLength);
        std::string str(length, 'a');
        stack.back()->Add()->set_string_value(str);
        break;
      }
      case protobuf::Value::kListValue: {
        if (stack.size() == 1) {
          auto list = row->add_values()->mutable_list_value();
          stack.push_back(list->mutable_values());
        } else {
          int parity =
              absl::Uniform<int>(absl::IntervalClosedClosed, *gen, 0, 1);
          if (parity == 1 && stack.size() < kMaxStackDepth) {
            auto list = stack.back()->Add()->mutable_list_value();
            stack.push_back(list->mutable_values());
          } else {
            stack.pop_back();
          }
        }
        break;
      }
      case protobuf::Value::kNumberValue: {
        int value = absl::Uniform<int>(absl::IntervalClosedClosed, *gen,
                                       kValueMinLength, kValueMaxLength);
        stack.back()->Add()->set_number_value(value);
        break;
      }
      case protobuf::Value::kBoolValue: {
        int parity = absl::Uniform<int>(absl::IntervalClosedClosed, *gen, 0, 1);
        stack.back()->Add()->set_bool_value(parity);
        break;
      }
      default:
        break;
    }
  }
  return result;
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
