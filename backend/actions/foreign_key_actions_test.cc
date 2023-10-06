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

#include "backend/actions/foreign_key_actions.h"

#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/types/span.h"
#include "backend/actions/action.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using ::google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using ::zetasql::values::Int64;

// How to read the acronyms in this file:
// FK - Foreign Key.
// PK - FK is in the same order as the Primary Key.
// PKP - FK is in the same order as the PK, but is a prefix.
// PKPOutOfOrder - FK is in a different order than the PK, and it's a PK prefix.
// None - None of the above, FK can be defined on PK or non-PK column.

class ForeignKeyActionTest : public test::ActionsTest {
 public:
  ForeignKeyActionTest()
      : flag_setter_({
            .enable_fk_delete_cascade_action = true,
        }) {}

 protected:
  void Init(const absl::Span<const std::string> schema) {
    zetasql::TypeFactory type_factory_;
    schema_ =
        emulator::test::CreateSchemaFromDDL(schema, &type_factory_).value();
    referenced_table_ = schema_->FindTable("Referenced");
    referenced_columns_ = referenced_table_->columns();
    referencing_table_ = schema_->FindTable("Referencing");
    referencing_columns_ = referencing_table_->columns();
    foreign_key_ = schema_->FindTable("Referencing")->FindForeignKey("C");
    effector_ = std::make_unique<ForeignKeyActionEffector>(foreign_key_);
  }

  const ScopedEmulatorFeatureFlagsSetter flag_setter_;
  std::unique_ptr<const Schema> schema_;
  const Table* referenced_table_;
  absl::Span<const Column* const> referenced_columns_;
  const Table* referencing_table_;
  absl::Span<const Column* const> referencing_columns_;
  const ForeignKey* foreign_key_;
  std::unique_ptr<Effector> effector_;
};

TEST_F(ForeignKeyActionTest, ReferencedPK_ReferencingPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk)
              REFERENCES Referenced (referenced_pk)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk)
        )"};
  Init(schema);
  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1)}),
                            referenced_columns_, {Int64(1), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(1)}),
                            referencing_columns_, {Int64(1), Int64(20)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Delete(referenced_table_, Key({Int64(1)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(1)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPKP_ReferencingPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk)
              REFERENCES Referenced (referenced_pk1)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk)
        )"};
  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1), Int64(2)}),
                            referenced_columns_,
                            {Int64(1), Int64(2), Int64(30)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(1)}),
                            referencing_columns_, {Int64(1), Int64(40)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Delete(referenced_table_, Key({Int64(1), Int64(2)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(1)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPKOutOfOrder_ReferencingPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2)
        )",
      R"(
          CREATE TABLE Referencing (
          referencing_pk1 INT64 NOT NULL,
          referencing_pk2 INT64 NOT NULL,
          referencing_val INT64,
          CONSTRAINT C FOREIGN KEY (referencing_pk1, referencing_pk2)
            REFERENCES Referenced (referenced_pk2, referenced_pk1)
            ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk1, referencing_pk2)
        )"};
  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1), Int64(2)}),
                            referenced_columns_,
                            {Int64(1), Int64(2), Int64(30)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(2), Int64(1)}),
                            referencing_columns_,
                            {Int64(2), Int64(1), Int64(40)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Delete(referenced_table_, Key({Int64(1), Int64(2)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(2), Int64(1)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedNonPK_ReferencingPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk)
              REFERENCES Referenced (referenced_val)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk)
        )"};
  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1)}),
                            referenced_columns_, {Int64(1), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(10)}),
                            referencing_columns_, {Int64(10), Int64(20)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Delete(referenced_table_, Key({Int64(1)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(10)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPK_ReferencingPKP) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk1 INT64 NOT NULL,
            referencing_pk2 INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk1)
              REFERENCES Referenced (referenced_pk)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk1, referencing_pk2)
        )"};

  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1)}),
                            referenced_columns_, {Int64(1), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(1), Int64(20)}),
                            referencing_columns_,
                            {Int64(1), Int64(20), Int64(100)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Delete(referenced_table_, Key({Int64(1)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(1), Int64(20)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPK_ReferencingPKOutOfOrder) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk1 INT64 NOT NULL,
            referencing_pk2 INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk1, referencing_pk2)
              REFERENCES Referenced (referenced_pk2, referenced_pk1)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk1, referencing_pk2)
        )"};

  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1), Int64(20)}),
                            referenced_columns_,
                            {Int64(1), Int64(20), Int64(30)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(20), Int64(1)}),
                            referencing_columns_,
                            {Int64(20), Int64(1), Int64(400)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Delete(referenced_table_, Key({Int64(1), Int64(20)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(20), Int64(1)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPK_ReferencingNonPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
          referenced_pk INT64 NOT NULL,
          referenced_val INT64,
        ) PRIMARY KEY (referenced_pk)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_val)
              REFERENCES Referenced (referenced_pk)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk)
        )"};

  Init(schema);

  // Add row in base table & index.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1)}),
                            referenced_columns_, {Int64(1), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(50)}),
                            referencing_columns_, {Int64(50), Int64(1)}));
  ZETASQL_ASSERT_OK(store()->Insert(foreign_key_->referencing_data_table(),
                            Key({Int64(1), Int64(50)}),
                            foreign_key_->referencing_data_table()->columns(),
                            {Int64(1), Int64(50)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Delete(referenced_table_, Key({Int64(1)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(50)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedNonPK_ReferencingNonPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_val)
              REFERENCES Referenced (referenced_val)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk)
        )"};
  Init(schema);

  // Add row in base table & index.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1)}),
                            referenced_columns_, {Int64(1), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(20)}),
                            referencing_columns_, {Int64(20), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(foreign_key_->referencing_data_table(),
                            Key({Int64(10), Int64(20)}),
                            foreign_key_->referencing_data_table()->columns(),
                            {Int64(10), Int64(20)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Delete(referenced_table_, Key({Int64(1)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(20)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPKNonPK_ReferencingPKNonPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk)
        )",
      R"(
          CREATE TABLE Referencing (
           referencing_pk INT64 NOT NULL,
           referencing_val INT64,
           CONSTRAINT C
             FOREIGN KEY (referencing_pk,referencing_val)
             REFERENCES Referenced
               (referenced_pk, referenced_val)
             ON DELETE CASCADE
         ) PRIMARY KEY (referencing_pk)
        )"};
  Init(schema);

  // Add row in base table & index.
  ZETASQL_ASSERT_OK(store()->Insert(referenced_table_, Key({Int64(1)}),
                            referenced_columns_, {Int64(1), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(1)}),
                            referencing_columns_, {Int64(1), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(foreign_key_->referencing_data_table(),
                            Key({Int64(1), Int64(10)}),
                            foreign_key_->referencing_data_table()->columns(),
                            {Int64(1), Int64(10)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Delete(referenced_table_, Key({Int64(1)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(1)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPKPOutOfOrder_ReferencingPK) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            referenced_pk3 INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2, referenced_pk3)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk1 INT64 NOT NULL,
            referencing_pk2 INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk1, referencing_pk2)
              REFERENCES Referenced (referenced_pk2, referenced_pk1)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk1, referencing_pk2)
        )"};
  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(
      referenced_table_, Key({Int64(1), Int64(2), Int64(3)}),
      referenced_columns_, {Int64(1), Int64(2), Int64(3), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(2), Int64(1)}),
                            referencing_columns_,
                            {Int64(2), Int64(1), Int64(10)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Delete(referenced_table_, Key({Int64(1), Int64(2), Int64(3)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(2), Int64(1)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPKPOutOfOrder_ReferencingPKOutOfOrder) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            referenced_pk3 INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2, referenced_pk3)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk1 INT64 NOT NULL,
            referencing_pk2 INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk2, referencing_pk1)
              REFERENCES Referenced (referenced_pk2, referenced_pk1)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk1, referencing_pk2)
        )"};
  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(
      referenced_table_, Key({Int64(1), Int64(2), Int64(3)}),
      referenced_columns_, {Int64(1), Int64(2), Int64(3), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(referencing_table_, Key({Int64(1), Int64(2)}),
                            referencing_columns_,
                            {Int64(1), Int64(2), Int64(50)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Delete(referenced_table_, Key({Int64(1), Int64(2), Int64(3)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(
                  DeleteOp{referencing_table_, Key({Int64(1), Int64(2)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPKPOutOfOrder_ReferencingPKPOutOfOrder) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            referenced_pk3 INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2, referenced_pk3)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk1 INT64 NOT NULL,
            referencing_pk2 INT64 NOT NULL,
            referencing_pk3 INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk2, referencing_pk1)
              REFERENCES Referenced (referenced_pk2, referenced_pk1)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk1, referencing_pk2, referencing_pk3)
        )"};
  Init(schema);

  // Add row in base table.
  ZETASQL_ASSERT_OK(store()->Insert(
      referenced_table_, Key({Int64(1), Int64(2), Int64(3)}),
      referenced_columns_, {Int64(1), Int64(2), Int64(3), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(
      referencing_table_, Key({Int64(1), Int64(2), Int64(4)}),
      referencing_columns_, {Int64(1), Int64(2), Int64(4), Int64(50)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Delete(referenced_table_, Key({Int64(1), Int64(2), Int64(3)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(DeleteOp{
                  referencing_table_, Key({Int64(1), Int64(2), Int64(4)})}));
}

TEST_F(ForeignKeyActionTest, ReferencedPKPOutOfOrder_ReferencingNone) {
  const std::vector<std::string> schema = {
      R"(
          CREATE TABLE Referenced (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            referenced_pk3 INT64 NOT NULL,
            referenced_val INT64,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2, referenced_pk3)
        )",
      R"(
          CREATE TABLE Referencing (
            referencing_pk1 INT64 NOT NULL,
            referencing_pk2 INT64 NOT NULL,
            referencing_pk3 INT64 NOT NULL,
            referencing_val INT64,
            CONSTRAINT C FOREIGN KEY (referencing_pk3, referencing_pk2)
              REFERENCES Referenced (referenced_pk2, referenced_pk1)
              ON DELETE CASCADE
          ) PRIMARY KEY (referencing_pk1, referencing_pk2, referencing_pk3)
        )"};
  Init(schema);

  // Add row in base table & index.
  ZETASQL_ASSERT_OK(store()->Insert(
      referenced_table_, Key({Int64(1), Int64(2), Int64(3)}),
      referenced_columns_, {Int64(1), Int64(2), Int64(3), Int64(10)}));
  ZETASQL_ASSERT_OK(store()->Insert(
      referencing_table_, Key({Int64(10), Int64(1), Int64(2)}),
      referencing_columns_, {Int64(10), Int64(1), Int64(2), Int64(50)}));
  ZETASQL_ASSERT_OK(store()->Insert(foreign_key_->referencing_data_table(),
                            Key({Int64(1), Int64(2), Int64(10)}),
                            foreign_key_->referencing_data_table()->columns(),
                            {Int64(1), Int64(2), Int64(10)}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Delete(referenced_table_, Key({Int64(1), Int64(2), Int64(3)}))));
  // Verify foreign key delete cascade to referencing table is added to the
  // transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<DeleteOp>(DeleteOp{
                  referencing_table_, Key({Int64(10), Int64(1), Int64(2)})}));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
