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

#include "backend/schema/catalog/foreign_key.h"

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

TEST(ForeignKeyTest, ActionNameUnspecified) {
  EXPECT_EQ(ForeignKey::ActionName(ForeignKey::Action::kActionUnspecified), "");
}

TEST(ForeignKeyTest, ActionNameNoAction) {
  EXPECT_EQ(ForeignKey::ActionName(ForeignKey::Action::kNoAction), "NO ACTION");
}

TEST(ForeignKeyTest, ActionNameCascade) {
  EXPECT_EQ(ForeignKey::ActionName(ForeignKey::Action::kCascade), "CASCADE");
}

TEST(ForeignKeyTest, ActionNameIncorrect) {
  EXPECT_EQ(ForeignKey::ActionName(static_cast<ForeignKey::Action>(10)), "");
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
