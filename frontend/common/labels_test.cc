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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "common/limits.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

TEST(ValidationTest, ValidateLabels) {
  google::protobuf::Map<std::string, std::string> labels;
  labels["a"] = "b";     // lowercase letters
  labels["a1"] = "1";    // key has digit, value starts with digit
  labels["a-1"] = "-1";  // key has hyphen, value starts with hyphen
  labels["a_"] = "_";    // key has underscore, value starts with underscore
  labels["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"] =
      "";  // key is 63 characters, value is empty
  ZETASQL_EXPECT_OK(ValidateLabels(labels));
}

TEST(ValidationTest, ValidateZeroLabels) {
  google::protobuf::Map<std::string, std::string> labels;
  ZETASQL_EXPECT_OK(ValidateLabels(labels));
}

TEST(ValidationTest, ValidateTooManyLabelsReturnsError) {
  google::protobuf::Map<std::string, std::string> labels;
  for (int i = 0; i < limits::kMaxNumCloudLabels + 1; i++) {
    labels[absl::StrCat("key-", i)] = absl::StrCat("value-", i);
  }
  EXPECT_THAT(ValidateLabels(labels),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ValidationTest, ValidateLabelInvalidKeyReturnsError) {
  google::protobuf::Map<std::string, std::string> labels;
  std::vector<std::string> invalid_keys = {
      "key%",          // invalid character
      "",              // empty
      "100",           // starts with digit
      "-",             // starts with hyphen
      "_",             // starts with underscore
      "invalid\\key",  // invalid character
      "iKey",          // upercase letter
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      "aaaaaaaaa"};  // 65 characters, too long
  for (const std::string& key : invalid_keys) {
    labels.clear();
    labels[key] = "value";
    EXPECT_THAT(ValidateLabels(labels),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST(ValidationTest, ValidateLabelInvalidValueReturnsError) {
  google::protobuf::Map<std::string, std::string> labels;
  std::vector<std::string> invalid_values = {
      "value%",        // invalid character
      "invalid\\key",  // invalid character
      "iKey",          // upercase letter
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      "aaaaaaaaa"};  // 65 characters, too long
  for (const std::string& value : invalid_values) {
    labels.clear();
    labels["key"] = value;
    EXPECT_THAT(ValidateLabels(labels),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
