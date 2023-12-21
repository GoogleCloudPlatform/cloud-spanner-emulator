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

#include "backend/schema/catalog/sequence.h"

#include <cstdint>
#include <string>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/bit_reverse.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status Sequence::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}
absl::Status Sequence::ValidateUpdate(const SchemaNode* old,
                                      SchemaValidationContext* context) const {
  return validate_update_(this, old->template As<const Sequence>(), context);
}
std::string Sequence::DebugString() const {
  std::string debug_string = absl::Substitute("Sequence $0. Sequence kind: $1",
                                              name_, sequence_kind_name());
  if (start_with_counter().has_value()) {
    absl::StrAppend(&debug_string,
                    "\n  start_with_counter: ", start_with_counter().value());
  }
  if (skip_range_min().has_value() && skip_range_max().has_value()) {
    absl::StrAppend(&debug_string, "\n  skipped range: [",
                    skip_range_min().value(), ", ", skip_range_max().value(),
                    "]");
  }
  return debug_string;
}
absl::Status Sequence::DeepClone(SchemaGraphEditor* editor,
                                 const SchemaNode* orig) {
  return absl::OkStatus();
}

absl::StatusOr<zetasql::Value> Sequence::GetNextSequenceValue() const {
  if (!Sequence::SequenceLastValues.contains(name_)) {
    if (start_with_.has_value()) {
      Sequence::SequenceLastValues[name_] = start_with_.value();
    } else {
      Sequence::SequenceLastValues[name_] = kSequenceDefaultStartWith;
    }
  }
  if (Sequence::SequenceLastValues[name_] < 0) {
    return error::InvalidSequenceStartWithCounterValue();
  }

  // Retrieve the next value and make sure that it doesn't fall into the skipped
  // range. If it does, keep trying until we have one.
  int attempt_count = 1;
  int64_t value = -1;
  do {
    if (attempt_count > limits::kMaxGetSequenceValueAttempt) {
      ABSL_LOG(INFO) << "Attempted to get sequence values more than "
                << limits::kMaxGetSequenceValueAttempt
                << "times. Current skipped range is ["
                << skip_range_min_.value() << ", " << skip_range_max_.value()
                << "].";
      return error::SequenceExhausted(name_);
    }
    if (Sequence::SequenceLastValues[name_] == kInt64Max) {
      ABSL_LOG(INFO) << "No additional value can be obtained. The current sequence "
                << "counter is already at int64max.";
      return error::SequenceExhausted(name_);
    }
    // In a bit-reversed-positive sequence, we bit-reverse the counter and
    // preserve its sign.
    value = BitReverse(Sequence::SequenceLastValues[name_]++,
                       /*preserve_sign=*/true);

    ++attempt_count;
  } while (
      skip_range_min_.has_value() && skip_range_max_.has_value() &&
      (value <= skip_range_max_.value() && value >= skip_range_min_.value()));

  return zetasql::Value::Int64(value);
}

zetasql::Value Sequence::GetInternalSequenceState() const {
  // If no sequence value has been retrieved before, then the current state is
  // NULL.
  if (!Sequence::SequenceLastValues.contains(name_)) {
    return zetasql::Value::NullInt64();
  }
  return zetasql::Value::Int64(Sequence::SequenceLastValues[name_]);
}

void Sequence::ResetSequenceLastValue() const {
  if (!Sequence::SequenceLastValues.contains(name_)) {
    return;
  }
  if (start_with_.has_value()) {
    Sequence::SequenceLastValues[name_] = start_with_.value();
  } else {
    Sequence::SequenceLastValues[name_] = kSequenceDefaultStartWith;
  }
}

void Sequence::RemoveSequenceFromLastValuesMap() const {
  absl::flat_hash_map<std::string, int64_t>::iterator it =
      Sequence::SequenceLastValues.find(name_);
  if (it != Sequence::SequenceLastValues.end()) {
    Sequence::SequenceLastValues.erase(it);
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
