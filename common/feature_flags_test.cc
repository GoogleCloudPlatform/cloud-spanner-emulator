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

#include "common/feature_flags.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

TEST(EmulatorFeatureFlags, Basic) {
  const EmulatorFeatureFlags& features = EmulatorFeatureFlags::instance();

  EXPECT_TRUE(features.flags().enable_stored_generated_columns);
  EXPECT_TRUE(features.flags().enable_check_constraint);

  {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_stored_generated_columns = false;
    flags.enable_check_constraint = false;
    test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    EXPECT_FALSE(features.flags().enable_stored_generated_columns);
    EXPECT_FALSE(features.flags().enable_check_constraint);
  }
  EXPECT_TRUE(features.flags().enable_stored_generated_columns);
  EXPECT_TRUE(features.flags().enable_check_constraint);
}

}  // namespace
}  // namespace emulator
}  // namespace spanner
}  // namespace google
