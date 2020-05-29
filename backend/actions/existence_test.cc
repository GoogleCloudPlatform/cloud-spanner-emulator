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

#include "backend/actions/existence.h"

#include <memory>
#include <queue>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "absl/types/variant.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::String;
using zetasql_base::testing::StatusIs;

// This unit test works with the following DDL statement:
//
// CREATE TABLE test_table (
//   int64_col INT64 NOT NULL,
//   string_col STRING(MAX)
// ) PRIMARY_KEY(int64_col);
class RowExistenceTest : public test::ActionsTest {
 public:
  RowExistenceTest()
      : schema_(emulator::test::CreateSchemaWithOneTable(&type_factory_)),
        table_(schema_->FindTable("test_table")),
        validator_(absl::make_unique<RowExistenceValidator>()) {}

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Table* table_;
  std::unique_ptr<Validator> validator_;
};

TEST_F(RowExistenceTest, InsertWithNoExistingRowSucceeds) {
  ZETASQL_EXPECT_OK(validator_->Validate(ctx(), Insert(table_, Key({Int64(1)}))));
}

TEST_F(RowExistenceTest, InsertWithExistingRowFails) {
  // Add row with same key.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), {}, {}));

  EXPECT_THAT(validator_->Validate(ctx(), Insert(table_, Key({Int64(1)}))),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(RowExistenceTest, UpdateWithExistingRowSucceeds) {
  // Add row with same key.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), {}, {}));

  ZETASQL_EXPECT_OK(validator_->Validate(
      ctx(), Update(table_, Key({Int64(1)}), {table_->FindColumn("string_col")},
                    {String("new")})));
}

TEST_F(RowExistenceTest, UpdateWithNonExistingRowFails) {
  EXPECT_THAT(validator_->Validate(ctx(), Update(table_, Key({Int64(1)}))),
              StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
