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

#include "absl/status/status.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "frontend/common/status.h"
#include "zetasql/base/ret_check.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

TEST(GrpcStatusConversion, MessageLength) {
  std::string error_message(2048, 'a');
  EXPECT_THAT(ToGRPCStatus(absl::Status(absl::StatusCode::kInvalidArgument,
                                        error_message))
                  .error_message(),
              testing::EndsWith("..."));
}

}  // namespace
}  // namespace emulator
}  // namespace spanner
}  // namespace google
