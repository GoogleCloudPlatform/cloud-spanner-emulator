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

#include "frontend/common/labels.h"

#include "common/errors.h"
#include "common/limits.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

// Returns true if the provided string is valid as a label key.
bool IsValidKeyLabel(const std::string& label) {
  static LazyRE2 regex = {R"([\p{Ll}\p{Lo}][\p{Ll}\p{Lo}\p{N}_-]{0,62})"};
  return RE2::FullMatch(label, *regex);
}

// Returns true if the provided string is valid as a label value.
bool IsValidValueLabel(const std::string& label) {
  static LazyRE2 regex = {R"([\p{Ll}\p{Lo}\p{N}_-]{0,63})"};
  return RE2::FullMatch(label, *regex);
}

}  // namespace

absl::Status ValidateLabels(
    const google::protobuf::Map<std::string, std::string>& labels) {
  if (labels.empty()) {
    return absl::OkStatus();
  }
  if (labels.size() > limits::kMaxNumCloudLabels) {
    return error::TooManyLabels(labels.size());
  }
  for (const auto& label : labels) {
    if (!IsValidKeyLabel(label.first)) {
      return error::BadLabelKey(label.first);
    }
    if (!IsValidValueLabel(label.second)) {
      return error::BadLabelValue(label.first, label.second);
    }
  }
  return absl::OkStatus();
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
