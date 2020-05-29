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

#include "backend/actions/unique_index.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
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

class UniqueIndexTest : public test::ActionsTest {
 public:
  UniqueIndexTest()
      : schema_(emulator::test::CreateSchemaFromDDL(
                    {
                        R"(
                            CREATE TABLE TestTable (
                              int64_col INT64 NOT NULL,
                              string_col STRING(MAX)
                            ) PRIMARY KEY (int64_col)
                          )",
                        R"(
                            CREATE UNIQUE INDEX TestIndex ON TestTable(string_col DESC)
                    )"},
                    &type_factory_)
                    .value()),
        index_(schema_->FindIndex("TestIndex")),
        verifier_(absl::make_unique<UniqueIndexVerifier>(index_)) {}

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Index* index_;
  std::unique_ptr<Verifier> verifier_;
};

TEST_F(UniqueIndexTest, DuplicateIndexKeysReturnsAlreadyExistsError) {
  // Add row with same key.
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(3)}), {}, {}));
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(4)}), {}, {}));

  EXPECT_THAT(verifier_->Verify(
                  ctx(), Insert(index_->index_data_table(),
                                Key({String("value"), Int64(3)}), {}, {})),
              StatusIs(absl::StatusCode::kAlreadyExists));
  EXPECT_THAT(verifier_->Verify(
                  ctx(), Insert(index_->index_data_table(),
                                Key({String("value"), Int64(4)}), {}, {})),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(UniqueIndexTest, IndexKeysNotInTransactionFailsWithInternalError) {
  EXPECT_THAT(verifier_->Verify(
                  ctx(), Insert(index_->index_data_table(),
                                Key({String("value"), Int64(3)}), {}, {})),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(UniqueIndexTest, UniqueIndexKeysReturnsOk) {
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(), Key({String("value")}),
                            {}, {}));

  ZETASQL_EXPECT_OK(verifier_->Verify(ctx(), Insert(index_->index_data_table(),
                                            Key({String("value")}), {}, {})));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
