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

#include <cstdint>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {

using zetasql_base::testing::StatusIs;

class ForeignKeyVerifiersTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("foreign_key_verifiers.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectForeignKeyVerifiersTest, ForeignKeyVerifiersTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ForeignKeyVerifiersTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ForeignKeyVerifiersTest, ValidKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {1, 2}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(x,y) REFERENCES t(a,b)"}));
}

// Indexes include all of the indexed table's primary key columns in its
// primary key columns. This can make the index primary key wider than the
// indexed table's primary key. Data validation must therefore do prefix lookups
// instead of point lookups. Add some tests for a mixture of wide keys.
TEST_P(ForeignKeyVerifiersTest, ValidWideKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {3, 2}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(y) REFERENCES t(b)"}));
}

TEST_P(ForeignKeyVerifiersTest, ValidWideReferencingKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {3, 1}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(y) REFERENCES t(a)"}));
}

TEST_P(ForeignKeyVerifiersTest, ValidWideReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {2, 4}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(x) REFERENCES t(b)"}));
}

TEST_P(ForeignKeyVerifiersTest, InvalidKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {1, 3}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(x,y) REFERENCES t(a,b)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ForeignKeyVerifiersTest, ValidReversedKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {2, 1}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(y,x) REFERENCES t(a,b)"}));
}

TEST_P(ForeignKeyVerifiersTest, InvalidReversedKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {1, 2}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(y,x) REFERENCES t(a,b)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ForeignKeyVerifiersTest, InvalidWideReferencingKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {3, 4}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(y) REFERENCES t(a)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ForeignKeyVerifiersTest, InvalidWideReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {3, 4}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(x) REFERENCES t(b)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ForeignKeyVerifiersTest, EmptyReferencingTable) {
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(y) REFERENCES t(b)"}));
}

TEST_P(ForeignKeyVerifiersTest, ReferencingNullValues) {
  ZETASQL_ASSERT_OK(Insert("u", {"x", "y"}, {1, Null<std::int64_t>()}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(x,y) REFERENCES t(a,b)"}));
}

TEST_P(ForeignKeyVerifiersTest, NonMatchingReferencedRow) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(y) REFERENCES t(b)"}));
}

TEST_P(ForeignKeyVerifiersTest, DuplicateReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {3, 2}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE u ADD FOREIGN KEY(x) REFERENCES t(b)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ForeignKeyVerifiersTest, CreateValidTable) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {3, 4}));
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_EXPECT_OK(UpdateSchema({R"(
      CREATE TABLE v (
        x INT64,
        y INT64,
        FOREIGN KEY(x) REFERENCES t(b)
      ) PRIMARY KEY(x))"}));
  } else {
    ZETASQL_EXPECT_OK(UpdateSchema({R"(
      CREATE TABLE v (
        x BIGINT PRIMARY KEY,
        y BIGINT,
        FOREIGN KEY(x) REFERENCES t(b)
      ))"}));
  }
}

TEST_P(ForeignKeyVerifiersTest, CreateTableWithDuplidateReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("t", {"a", "b"}, {3, 2}));
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(UpdateSchema({R"(
      CREATE TABLE v (
        x INT64,
        y INT64,
        FOREIGN KEY(x) REFERENCES t(b)
      ) PRIMARY KEY(x))"}),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  } else {
    EXPECT_THAT(UpdateSchema({R"(
      CREATE TABLE v (
        x BIGINT PRIMARY KEY,
        y BIGINT,
        FOREIGN KEY(x) REFERENCES t(b)
      ))"}),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
