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

#include "frontend/collections/database_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/match.h"
#include "frontend/entities/database.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

class DatabaseManagerTest : public testing::Test {
 protected:
  DatabaseManagerTest()
      : database_manager_(&clock_),
        database_uri_(
            "projects/test-p/instances/test-instance/databases/test-database") {
  }

  Clock clock_;
  DatabaseManager database_manager_;
  const std::string database_uri_;
  const std::vector<std::string> empty_schema_;
};

TEST_F(DatabaseManagerTest, CreateNewDatabase) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Database> database,
      database_manager_.CreateDatabase(database_uri_, empty_schema_));
  EXPECT_EQ(database->database_uri(), database_uri_);
}

TEST_F(DatabaseManagerTest, CreateExistingDatabaseUriFailsWithAlreadyExists) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Database> database,
      database_manager_.CreateDatabase(database_uri_, empty_schema_));
  EXPECT_THAT(database_manager_.CreateDatabase(database_uri_, empty_schema_),
              zetasql_base::testing::StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(DatabaseManagerTest, GetExistingDatabase) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Database> database,
      database_manager_.CreateDatabase(database_uri_, empty_schema_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Database> actual_database,
                       database_manager_.GetDatabase(database_uri_));
  EXPECT_EQ(actual_database->database_uri(), database_uri_);
}

TEST_F(DatabaseManagerTest, GetNonExistingDatabaseReturnsNotFound) {
  EXPECT_THAT(database_manager_.GetDatabase("not-exists"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(DatabaseManagerTest, DeleteExistingDatabase) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Database> database,
      database_manager_.CreateDatabase(database_uri_, empty_schema_));
  ZETASQL_EXPECT_OK(database_manager_.DeleteDatabase(database_uri_));
  EXPECT_THAT(database_manager_.GetDatabase(database_uri_),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(DatabaseManagerTest, ListDatabase) {
  std::string instance_uri = "projects/test-p/instances/test-i";
  int num_databases = 5;

  for (int i = 0; i < num_databases; i++) {
    std::string database_uri =
        absl::StrCat(instance_uri, "/databases/database-", i);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Database> database,
        database_manager_.CreateDatabase(database_uri, empty_schema_));
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Database>> databases,
                       database_manager_.ListDatabases(instance_uri));
  EXPECT_EQ(databases.size(), num_databases);
  for (int i = 0; i < num_databases; i++) {
    EXPECT_EQ(databases[i]->database_uri(),
              absl::StrCat(instance_uri, "/databases/database-", i));
  }
}

TEST_F(DatabaseManagerTest, ListDatabaseWithSimilarInstanceUri) {
  std::string similar_database_uri = absl::StrCat(
      "projects/test-p/instances/test-instances/databases/database");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Database> database,
      database_manager_.CreateDatabase(database_uri_, empty_schema_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(database, database_manager_.CreateDatabase(
                                     similar_database_uri, empty_schema_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Database>> databases,
                       database_manager_.ListDatabases(
                           "projects/test-p/instances/test-instance"));
  EXPECT_EQ(databases.size(), 1);
  EXPECT_EQ(databases[0]->database_uri(), database_uri_);
}

TEST_F(DatabaseManagerTest, DatabaseQuotaIsEnforced) {
  std::string database_uri_prefix =
      "projects/test-project/instances/test-instance/databases/test-database-";

  // Create 100 databases.
  for (int i = 1; i <= 100; ++i) {
    std::string database_uri = absl::StrCat(database_uri_prefix, i);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Database> database,
        database_manager_.CreateDatabase(database_uri, empty_schema_));
  }

  // The next database creation should fail.
  EXPECT_THAT(database_manager_.CreateDatabase(
                  absl::StrCat(database_uri_prefix, 101), empty_schema_),
              zetasql_base::testing::StatusIs(absl::StatusCode::kResourceExhausted));

  // But creating a database in another instance should not fail.
  ZETASQL_EXPECT_OK(database_manager_.CreateDatabase(
      absl::StrCat("projects/test-project/instances/test-instance-2/databases/"
                   "test-database-",
                   101),
      empty_schema_));

  // If we clear some quota, we can create a database again.
  ZETASQL_EXPECT_OK(
      database_manager_.DeleteDatabase(absl::StrCat(database_uri_prefix, 100)));
  ZETASQL_EXPECT_OK(database_manager_.CreateDatabase(
      absl::StrCat(database_uri_prefix, 101), empty_schema_));
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
