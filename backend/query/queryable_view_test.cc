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

#include "backend/query/queryable_view.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/query/catalog.h"
#include "backend/query/queryable_column.h"
#include "common/feature_flags.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

class QueryableViewTest : public testing::Test {
 public:
  void SetUp() override {
    auto flag_setter = std::make_unique<test::ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{.enable_views = true});
    auto schema = test::CreateSchemaFromDDL(
        {R"(CREATE VIEW test_view SQL SECURITY INVOKER AS
              SELECT TRUE AS bool_col)"},
        &type_factory_);
    ZETASQL_EXPECT_OK(schema.status());
    schema_ = std::move(schema.value());
  }

 protected:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<test::ScopedEmulatorFeatureFlagsSetter> flag_setter_;
  std::unique_ptr<const Schema> schema_;
};

TEST_F(QueryableViewTest, FindColumnByName) {
  const auto* schema_view = schema_->FindView("test_view");
  QueryableView view{schema_view};
  EXPECT_NE(view.FindColumnByName("bool_col"), nullptr);
  EXPECT_NE(view.FindColumnByName("BOOL_COL"), nullptr);
  EXPECT_EQ(view.NumColumns(), 1);
  EXPECT_EQ(view.Name(), "test_view");
  EXPECT_EQ(view.wrapped_view(), schema_view);
  EXPECT_EQ(view.FindColumnByName("int_col"), nullptr);
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
