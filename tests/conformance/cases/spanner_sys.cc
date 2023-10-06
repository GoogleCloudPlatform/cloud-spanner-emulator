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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

class SpannerSysTest : public DatabaseTest {
  absl::Status SetUpDatabase() override { return absl::OkStatus(); }
};

TEST_F(SpannerSysTest, SupportedOptimizerVersionsTableIsValid) {

  auto results = Query(R"(
      SELECT is_default, FORMAT_DATE('%F', release_date), version
      FROM spanner_sys.SUPPORTED_OPTIMIZER_VERSIONS
    )");

  EXPECT_THAT(results, IsOkAndHoldsRow({true, "2023-09-19", 42}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
