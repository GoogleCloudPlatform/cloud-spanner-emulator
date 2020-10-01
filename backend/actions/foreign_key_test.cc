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

#include "backend/actions/foreign_key.h"

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

class ForeignKeyTest : public test::ActionsTest {
 public:
  ForeignKeyTest()
      : schema_(emulator::test::CreateSchemaFromDDL({R"(
            CREATE TABLE T (
              A INT64,
              B INT64,
              C INT64,
            ) PRIMARY KEY(A)
          )",
                                                     R"(
             CREATE TABLE U (
               X INT64,
               Y INT64,
               Z INT64,
               CONSTRAINT C FOREIGN KEY (Y, Z) REFERENCES T (B, C),
             ) PRIMARY KEY(X)
           )"},
                                                    &type_factory_)
                    .value()),
        foreign_key_(schema_->FindTable("U")->FindForeignKey("C")),
        referencing_data_(
            foreign_key_->referencing_index()->index_data_table()),
        referencing_columns_(referencing_data_->columns()),
        referenced_data_(foreign_key_->referenced_index()->index_data_table()),
        referenced_columns_(referenced_data_->columns()),
        referencing_verifier_(
            absl::make_unique<ForeignKeyReferencingVerifier>(foreign_key_)),
        referenced_verifier_(
            absl::make_unique<ForeignKeyReferencedVerifier>(foreign_key_)) {}

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const ForeignKey* foreign_key_;
  const Table* referencing_data_;
  absl::Span<const Column* const> referencing_columns_;
  const Table* referenced_data_;
  absl::Span<const Column* const> referenced_columns_;
  std::unique_ptr<Verifier> referencing_verifier_;
  std::unique_ptr<Verifier> referenced_verifier_;
};

TEST_F(ForeignKeyTest, InsertReferencingRowWithReferencedRow) {
  // Insert the referenced row.
  ZETASQL_ASSERT_OK(
      store()->Insert(referenced_data_, Key({Int64(1), Int64(2), Int64(3)}),
                      referenced_columns_, {Int64(1), Int64(2), Int64(3)}));

  // Insertion of the matching referencing row should succeed. The extra key
  // value from the unused primary key column should be excluded from the
  // verification lookup.
  ZETASQL_EXPECT_OK(referencing_verifier_->Verify(
      ctx(), Insert(referencing_data_, Key({Int64(1), Int64(2), Int64(4)}),
                    referencing_columns_, {Int64(1), Int64(2), Int64(4)})));
}

TEST_F(ForeignKeyTest, InsertReferencingRowWithoutReferencedRow) {
  // Insertion of a referencing row without a matching referenced row should
  // fail.
  EXPECT_THAT(
      referencing_verifier_->Verify(
          ctx(), Insert(referencing_data_, Key({Int64(1), Int64(2), Int64(3)}),
                        referencing_columns_, {Int64(1), Int64(2), Int64(3)})),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTest, DeleteReferencedRowWithReferencingRow) {
  // Insert referencing and referenced rows.
  ZETASQL_ASSERT_OK(
      store()->Insert(referencing_data_, Key({Int64(1), Int64(2), Int64(4)}),
                      referencing_columns_, {Int64(1), Int64(2), Int64(4)}));

  // Deletion of the referenced row with a corresponding referencing row should
  // fail. The extra key value from the unused primary key column should be
  // excluded from the verification lookup.
  EXPECT_THAT(
      referenced_verifier_->Verify(
          ctx(), Delete(referenced_data_, Key({Int64(1), Int64(2), Int64(3)}))),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTest, DeleteReferencedRowWithoutReferencingRow) {
  // Insert two referenced rows and one referencing row.
  ZETASQL_ASSERT_OK(
      store()->Insert(referenced_data_, Key({Int64(1), Int64(2), Int64(3)}),
                      referenced_columns_, {Int64(1), Int64(2), Int64(3)}));
  ZETASQL_ASSERT_OK(
      store()->Insert(referenced_data_, Key({Int64(4), Int64(5), Int64(6)}),
                      referenced_columns_, {Int64(4), Int64(5), Int64(6)}));
  ZETASQL_ASSERT_OK(
      store()->Insert(referencing_data_, Key({Int64(1), Int64(2), Int64(4)}),
                      referencing_columns_, {Int64(1), Int64(2), Int64(4)}));

  // Deletion of the referenced row without a corresponding referencing row
  // should succeed.
  ZETASQL_EXPECT_OK(referenced_verifier_->Verify(
      ctx(), Delete(referenced_data_, Key({Int64(4), Int64(5), Int64(6)}))));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
