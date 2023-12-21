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

#include "backend/transaction/foreign_key_restrictions.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using ::zetasql_base::testing::StatusIs;

class ForeignKeyRestrictionsTest : public testing::Test {
 public:
  ForeignKeyRestrictionsTest()
      : type_factory_(std::make_unique<zetasql::TypeFactory>()),
        feature_flags_({.enable_fk_delete_cascade_action = true}) {
    schema_ = test::CreateSchemaFromDDL(
                  {
                      R"(
                          CREATE TABLE Referenced (
                            A INT64,
                            B INT64,
                            C INT64,
                          ) PRIMARY KEY(A))",
                      R"(
                          CREATE TABLE Referencing (
                            X INT64,
                            Y INT64,
                            Z INT64,
                            CONSTRAINT C FOREIGN KEY(Y, Z)
                              REFERENCES Referenced(A, B) ON DELETE CASCADE,
                          ) PRIMARY KEY(X))"},
                  type_factory_.get())
                  .value();
    referenced_table_ = schema_->FindTable("Referenced");
    referenced_columns_.push_back(referenced_table_->FindColumn("A"));
  }

 protected:
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;
  const Table* referenced_table_;
  std::vector<const Column*> referenced_columns_;
  ForeignKeyRestrictions fk_restrictions_;
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(ForeignKeyRestrictionsTest, ValidateDeleteConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<WriteOp> write_ops = {
      DeleteOp{.table = referenced_table_, .key = Key(v1)},
      InsertOp{.table = referenced_table_,
               .key = Key(v2),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)}};
  const std::string table_name = referenced_table_->Name();
  EXPECT_THAT(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                      schema_.get()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateDeleteNoConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      DeleteOp{.table = referenced_table_, .key = Key(v1)},
      InsertOp{.table = referenced_table_,
               .key = Key(v2),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v3)}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateDeleteTwiceNoConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      DeleteOp{.table = referenced_table_, .key = Key(v1)},
      InsertOp{.table = referenced_table_,
               .key = Key(v2),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v3)},
      DeleteOp{.table = referenced_table_, .key = Key(v3)}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateInsertConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<WriteOp> write_ops = {
      InsertOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      InsertOp{.table = referenced_table_,
               .key = Key(v2),
               .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  EXPECT_THAT(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                      schema_.get()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateInsertNoConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      InsertOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      InsertOp{.table = referenced_table_,
               .key = Key(v3),
               .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateWhenReferencedColumnNotInserted) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      InsertOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      InsertOp{.table = referenced_table_, .key = Key(v2)}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateInsertTwiceNoConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      InsertOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      InsertOp{.table = referenced_table_,
               .key = Key(v3),
               .columns = referenced_columns_},
      InsertOp{.table = referenced_table_,
               .key = Key(v3),
               .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateUpdateConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<WriteOp> write_ops = {
      UpdateOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      UpdateOp{.table = referenced_table_,
               .key = Key(v2),
               .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  EXPECT_THAT(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                      schema_.get()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateUpdateNoConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      UpdateOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      UpdateOp{.table = referenced_table_,
               .key = Key(v3),
               .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateWhenReferencedColumnNotUpdated) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      UpdateOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      UpdateOp{.table = referenced_table_, .key = Key(v2)}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateInsertUpdateNoConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {
      UpdateOp{.table = referenced_table_,
               .key = Key(v1),
               .columns = referenced_columns_},
      DeleteOp{.table = referenced_table_, .key = Key(v2)},
      InsertOp{.table = referenced_table_,
               .key = Key(v3),
               .columns = referenced_columns_},
      UpdateOp{.table = referenced_table_,
               .key = Key(v3),
               .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateRangeDeleteConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<WriteOp> write_ops = {InsertOp{.table = referenced_table_,
                                             .key = Key(v1),
                                             .columns = referenced_columns_},
                                    InsertOp{.table = referenced_table_,
                                             .key = Key(v2),
                                             .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
  std::vector<KeyRange> delete_ranges = {
      KeyRange::ClosedOpen(Key(v1), Key(v2))};
  EXPECT_THAT(
      fk_restrictions_.ValidateReferencedDeleteMods(table_name, delete_ranges),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateRangeDeleteNoConflict) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  std::vector<zetasql::Value> v3 = {zetasql::Value::Int64(3)};
  std::vector<WriteOp> write_ops = {InsertOp{.table = referenced_table_,
                                             .key = Key(v2),
                                             .columns = referenced_columns_},
                                    InsertOp{.table = referenced_table_,
                                             .key = Key(v3),
                                             .columns = referenced_columns_}};
  const std::string table_name = referenced_table_->Name();
  ZETASQL_EXPECT_OK(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                    schema_.get()));
  std::vector<KeyRange> delete_ranges = {
      KeyRange::ClosedOpen(Key(v1), Key(v2))};
  ZETASQL_EXPECT_OK(
      fk_restrictions_.ValidateReferencedDeleteMods(table_name, delete_ranges));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateInsertConflictWithRangeDelete) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  const std::string table_name = referenced_table_->Name();
  std::vector<KeyRange> delete_ranges = {
      KeyRange::ClosedOpen(Key(v1), Key(v2))};

  ZETASQL_EXPECT_OK(
      fk_restrictions_.ValidateReferencedDeleteMods(table_name, delete_ranges));

  std::vector<WriteOp> write_ops = {InsertOp{.table = referenced_table_,
                                             .key = Key(v1),
                                             .columns = referenced_columns_},
                                    InsertOp{.table = referenced_table_,
                                             .key = Key(v2),
                                             .columns = referenced_columns_}};
  EXPECT_THAT(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                      schema_.get()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyRestrictionsTest, ValidateUpdateConflictWithRangeDelete) {
  std::vector<zetasql::Value> v1 = {zetasql::Value::Int64(1)};
  std::vector<zetasql::Value> v2 = {zetasql::Value::Int64(2)};
  const std::string table_name = referenced_table_->Name();
  std::vector<KeyRange> delete_ranges = {
      KeyRange::ClosedOpen(Key(v1), Key(v2))};

  ZETASQL_EXPECT_OK(
      fk_restrictions_.ValidateReferencedDeleteMods(table_name, delete_ranges));

  std::vector<WriteOp> write_ops = {UpdateOp{.table = referenced_table_,
                                             .key = Key(v1),
                                             .columns = referenced_columns_},
                                    InsertOp{.table = referenced_table_,
                                             .key = Key(v2),
                                             .columns = referenced_columns_}};
  EXPECT_THAT(fk_restrictions_.ValidateReferencedMods(write_ops, table_name,
                                                      schema_.get()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
