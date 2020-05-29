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

#include "backend/actions/interleave.h"

#include <memory>
#include <queue>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "absl/types/variant.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql_base::testing::StatusIs;

// This unit test works with the following DDL statements:
//
// CREATE TABLE Parent (
//   k1 INT64 NOT NULL,
//   c1 STRING(MAX)
// ) PRIMARY_KEY(k1);
//
// CREATE TABLE CascadeDeleteChild (
//   k1 INT64 NOT NULL,
//   k2 INT64 NOT NULL,
//   c1 STRING(MAX)
// ) PRIMARY KEY (k1, k2)
//   INTERLEAVE IN PARENT Parent ON DELETE CASCADE;
//
// CREATE TABLE NoActionDeleteChild (
//   k1 INT64 NOT NULL,
//   k2 INT64 NOT NULL,
//   c1 STRING(MAX)
// ) PRIMARY KEY (k1, k2)
//   INTERLEAVE IN PARENT Parent ON DELETE NO ACTION;
class InterleaveTest : public test::ActionsTest {
 public:
  InterleaveTest()
      : schema_(emulator::test::CreateSchemaWithInterleaving(&type_factory_)),
        parent_table_(schema_->FindTable("Parent")),
        cascade_delete_child_(schema_->FindTable("CascadeDeleteChild")),
        no_action_delete_child_(schema_->FindTable("NoActionDeleteChild")) {}

  void SetUp() override {
    ASSERT_EQ(no_action_delete_child_->on_delete_action(),
              Table::OnDeleteAction::kNoAction);
    ASSERT_EQ(cascade_delete_child_->on_delete_action(),
              Table::OnDeleteAction::kCascade);
  }

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Table* parent_table_;
  const Table* cascade_delete_child_;
  const Table* no_action_delete_child_;
};

TEST_F(InterleaveTest,
       ParentRowDeleteWithNoActionCascadeSucceedsWithNoChildRows) {
  std::unique_ptr<Validator> validator =
      absl::make_unique<InterleaveParentValidator>(parent_table_,
                                                   no_action_delete_child_);

  // Action should succeed for no child rows.
  ZETASQL_EXPECT_OK(validator->Validate(ctx(), Delete(parent_table_, Key({Int64(1)}))));
}

TEST_F(InterleaveTest, ParentRowDeleteWithNoActionCascadeFailsWithChildRows) {
  std::unique_ptr<Validator> validator =
      absl::make_unique<InterleaveParentValidator>(parent_table_,
                                                   no_action_delete_child_);

  // Action should fail if child rows exist.
  ZETASQL_EXPECT_OK(store()->Insert(no_action_delete_child_, Key({Int64(1), Int64(1)}),
                            {}, {}));
  EXPECT_THAT(
      validator->Validate(ctx(), Delete(parent_table_, Key({Int64(1)}))),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(InterleaveTest,
       ParentRowDeleteWithDeleteCascadeSucceedsWithAndWithoutChildRows) {
  std::unique_ptr<Validator> validator =
      absl::make_unique<InterleaveParentValidator>(parent_table_,
                                                   cascade_delete_child_);

  // Action should succeed if child row does not exist.
  ZETASQL_EXPECT_OK(validator->Validate(ctx(), Delete(parent_table_, Key({Int64(1)}))));

  // Action should succeed if child rows exist.
  ZETASQL_EXPECT_OK(store()->Insert(no_action_delete_child_, Key({Int64(1), Int64(1)}),
                            {}, {}));
  ZETASQL_EXPECT_OK(validator->Validate(ctx(), Delete(parent_table_, Key({Int64(1)}))));
}

TEST_F(InterleaveTest, ParentRowDeleteWithNoActionDeleteCascadeHasNoEffects) {
  std::unique_ptr<Effector> effector =
      absl::make_unique<InterleaveParentEffector>(parent_table_,
                                                  no_action_delete_child_);

  // Action should not take any effects even if child row exist.
  ZETASQL_EXPECT_OK(effector->Effect(ctx(), Delete(parent_table_, Key({Int64(1)}))));
  EXPECT_EQ(effects_buffer()->ops_queue()->size(), 0);
}

TEST_F(InterleaveTest, ParentRowDeleteWithOnDeleteCascadeAddsEffects) {
  std::unique_ptr<Effector> effector =
      absl::make_unique<InterleaveParentEffector>(parent_table_,
                                                  cascade_delete_child_);

  // Effector should add delete ops for child rows.
  ZETASQL_EXPECT_OK(store()->Insert(cascade_delete_child_, Key({Int64(1), Int64(1)}),
                            {}, {}));
  ZETASQL_EXPECT_OK(effector->Effect(ctx(), Delete(parent_table_, Key({Int64(1)}))));
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{cascade_delete_child_, Key({Int64(1), Int64(1)})}));
}

TEST_F(InterleaveTest, ChildRowInsertFailsWithoutParentRow) {
  std::unique_ptr<Validator> validator =
      absl::make_unique<InterleaveChildValidator>(parent_table_,
                                                  cascade_delete_child_);

  // Action should fail since there is no parent row.
  EXPECT_THAT(validator->Validate(ctx(), Insert(cascade_delete_child_,
                                                Key({Int64(1), Int64(1)}))),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InterleaveTest, ChildRowInsertSucceedsWithParentRow) {
  std::unique_ptr<Validator> validator =
      absl::make_unique<InterleaveChildValidator>(parent_table_,
                                                  cascade_delete_child_);

  // Add parent row.
  ZETASQL_EXPECT_OK(store()->Insert(parent_table_, Key({Int64(1)}), {}, {}));

  // Action succeeds with parent row.
  ZETASQL_EXPECT_OK(validator->Validate(
      ctx(), Insert(cascade_delete_child_, Key({Int64(1), Int64(1)}))));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
