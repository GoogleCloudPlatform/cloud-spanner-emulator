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

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class IndexBackfillTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("index_backfill.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectIndexBackfillTest, IndexBackfillTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<IndexBackfillTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(IndexBackfillTest, BackfillForUniqueIndexSucceeds) {
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {2, "user1", 25}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {3, "user2", 20}));

  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE UNIQUE INDEX users_by_name_age_unique ON users(name, age)"}));

  EXPECT_THAT(ReadWithIndex("users", "users_by_name_age_unique",
                            {"name", "age"}, KeySet::All()),
              IsOkAndHoldsRows({{"user1", 20}, {"user1", 25}, {"user2", 20}}));
}

TEST_P(IndexBackfillTest,
       BackfillForUniqueIndexFailsWithUniquenessViolationFails) {
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, "user", 20}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {2, "user", 20}));

  EXPECT_THAT(
      UpdateSchema(
          {"CREATE UNIQUE INDEX users_by_name_age_unique ON users(name, age)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(IndexBackfillTest, ValidateCreationOfDuplicateIndexFails) {
  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE INDEX users_by_name_age_unique ON users(name, age)"}));
  EXPECT_THAT(
      UpdateSchema(
          {"CREATE INDEX users_by_name_age_unique ON users(name, age)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(IndexBackfillTest, BackfillOfIndexOnInterleavedTableSucceeds) {
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_EXPECT_OK(UpdateSchema({R"(
    CREATE TABLE photos(
      user_id    INT64 NOT NULL,
      photo_id   INT64 NOT NULL,
      name       STRING(MAX),
    ) PRIMARY KEY (user_id, photo_id),
    INTERLEAVE IN PARENT users ON DELETE CASCADE
  )"}));
  } else {
    ZETASQL_EXPECT_OK(UpdateSchema({R"(
      CREATE TABLE photos(
        user_id    BIGINT NOT NULL,
        photo_id   BIGINT NOT NULL,
        name       TEXT,
        PRIMARY KEY (user_id, photo_id)
      )
      INTERLEAVE IN PARENT users ON DELETE CASCADE
    )"}));
  }

  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(
      Insert("photos", {"user_id", "photo_id", "name"}, {1, 2, "entry1"}));
  ZETASQL_EXPECT_OK(
      Insert("photos", {"user_id", "photo_id", "name"}, {1, 3, "entry2"}));

  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE INDEX photos_by_name ON photos(name, photo_id, user_id)"}));

  EXPECT_THAT(ReadWithIndex("photos", "photos_by_name",
                            {"name", "photo_id", "user_id"}, KeySet::All()),
              IsOkAndHoldsRows({{"entry1", 2, 1}, {"entry2", 3, 1}}));
}

TEST_P(IndexBackfillTest,
       BackfillOfIndexOnInterleavedTableWithCascadingDeletesSucceeds) {
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_EXPECT_OK(UpdateSchema({R"(
    CREATE TABLE photos(
      user_id    INT64 NOT NULL,
      photo_id   INT64 NOT NULL,
      name       STRING(MAX),
    ) PRIMARY KEY (user_id, photo_id),
    INTERLEAVE IN PARENT users ON DELETE CASCADE
  )"}));
  } else {
    ZETASQL_EXPECT_OK(UpdateSchema({R"(
      CREATE TABLE photos(
        user_id    BIGINT NOT NULL,
        photo_id   BIGINT NOT NULL,
        name       TEXT,
        PRIMARY KEY (user_id, photo_id)
      )
      INTERLEAVE IN PARENT users ON DELETE CASCADE
    )"}));
  }

  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(
      Insert("photos", {"user_id", "photo_id", "name"}, {1, 2, "entry1"}));
  ZETASQL_EXPECT_OK(
      Insert("photos", {"user_id", "photo_id", "name"}, {1, 3, "entry2"}));

  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE INDEX photos_by_name ON photos(name, photo_id)"}));

  ZETASQL_EXPECT_OK(Delete("users", KeySet::All()));

  EXPECT_THAT(ReadWithIndex("photos", "photos_by_name", {"name", "photo_id"},
                            KeySet::All()),
              IsOkAndHoldsRows({}));
}

TEST_P(IndexBackfillTest, ValidateBackfillOfNullFilteredIndexSucceeds) {
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"},
                   {2, Null<std::string>(), 20}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"},
                   {3, "user2", Null<int64_t>()}));

  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_EXPECT_OK(UpdateSchema(
        {"CREATE NULL_FILTERED INDEX users_by_name_null_filtered ON "
         "users(name, age)"}));
  } else {
    ZETASQL_EXPECT_OK(UpdateSchema(
        {"CREATE INDEX users_by_name_null_filtered ON "
         "users(name, age) WHERE name IS NOT NULL AND age IS NOT NULL"}));
  }

  EXPECT_THAT(ReadWithIndex("users", "users_by_name_null_filtered",
                            {"name", "age"}, KeySet::All()),
              IsOkAndHoldsRows({{"user1", 20}}));
}

TEST_P(IndexBackfillTest, ValidateBackfillOfIndexWithStoredColumns) {
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {2, "user2", 30}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"},
                   {3, "user2", Null<int64_t>()}));

  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_EXPECT_OK(UpdateSchema(
        {"CREATE INDEX users_by_name ON users(name) STORING (age)"}));
  } else {
    ZETASQL_EXPECT_OK(UpdateSchema(
        {"CREATE INDEX users_by_name ON users(name) INCLUDE (age)"}));
  }

  EXPECT_THAT(
      ReadWithIndex("users", "users_by_name", {"name", "age"}, KeySet::All()),
      IsOkAndHoldsRows(
          {{"user1", 20}, {"user2", 30}, {"user2", Null<int64_t>()}}));
}

TEST_P(IndexBackfillTest, ValidateNullFilteringDoesNotApplyToStoredColumns) {
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {2, "user2", 30}));
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"},
                   {3, "user2", Null<int64_t>()}));

  // Ensure that null filtering does not apply to stored columns.
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_EXPECT_OK(
        UpdateSchema({"CREATE NULL_FILTERED INDEX users_by_name ON users(name) "
                      "STORING (age)"}));
  } else {
    ZETASQL_EXPECT_OK(
        UpdateSchema({"CREATE INDEX users_by_name ON users(name) INCLUDE (age) "
                      "WHERE name IS NOT NULL"}));
  }

  EXPECT_THAT(
      ReadWithIndex("users", "users_by_name", {"name", "age"}, KeySet::All()),
      IsOkAndHoldsRows(
          {{"user1", 20}, {"user2", 30}, {"user2", Null<int64_t>()}}));
}

TEST_P(IndexBackfillTest, CannotBackfillIndexWithLargeKey) {
  std::string long_name(8192, 'a');
  ZETASQL_EXPECT_OK(Insert("users", {"user_id", "name", "age"}, {1, long_name, 20}));

  EXPECT_THAT(UpdateSchema({"CREATE INDEX users_by_name ON users(name)"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(IndexBackfillTest, BackfillForUniqueNumericIndex) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP()
        << "PG dialect does not support index defined on NUMERIC type columns.";
  }
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  Numeric val1 = cloud::spanner::MakeNumeric("123.456789").value();
  Numeric val2 = cloud::spanner::MakeNumeric("0").value();
  Numeric val3 = cloud::spanner::MakeNumeric("999999999.456789").value();

  ZETASQL_EXPECT_OK(Insert("numeric_table", {"key", "val"}, {1, val1}));
  ZETASQL_EXPECT_OK(Insert("numeric_table", {"key", "val"}, {2, val2}));
  ZETASQL_EXPECT_OK(Insert("numeric_table", {"key", "val"}, {3, val3}));

  ZETASQL_EXPECT_OK(UpdateSchema({"CREATE UNIQUE INDEX idx ON numeric_table(val)"}));

  EXPECT_THAT(
      ReadWithIndex("numeric_table", "idx", {"key", "val"}, KeySet::All()),
      IsOkAndHoldsRows({{2, val2}, {1, val1}, {3, val3}}));
}

TEST_P(IndexBackfillTest,
       BackfillForUniqueNumericIndexFailsWithUniquenessViolationFails) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP()
        << "PG dialect does not support index defined on NUMERIC type columns.";
  }
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  Numeric val = cloud::spanner::MakeNumeric("123.456789").value();
  ZETASQL_EXPECT_OK(Insert("numeric_table", {"key", "val"}, {1, val}));
  ZETASQL_EXPECT_OK(Insert("numeric_table", {"key", "val"}, {2, val}));

  EXPECT_THAT(UpdateSchema({"CREATE UNIQUE INDEX idx ON numeric_table(val)"}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Found uniqueness violation")));
}

TEST_P(IndexBackfillTest, BackfillNullFilteredNumericIndex) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP()
        << "PG dialect does not support index defined on NUMERIC type columns.";
  }
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  Numeric val = cloud::spanner::MakeNumeric("123.456789").value();
  ZETASQL_EXPECT_OK(Insert("numeric_table", {"key", "val"}, {1, Null<Numeric>()}));
  ZETASQL_EXPECT_OK(Insert("numeric_table", {"key", "val"}, {2, val}));

  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE NULL_FILTERED INDEX idx ON numeric_table(val)"}));

  EXPECT_THAT(
      ReadWithIndex("numeric_table", "idx", {"key", "val"}, KeySet::All()),
      IsOkAndHoldsRows({{2, val}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
