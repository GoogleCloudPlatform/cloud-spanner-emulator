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

#include <memory>

#include "google/protobuf/any.pb.h"
#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "backend/database/database.h"
#include "backend/schema/printer/print_ddl.h"
#include "common/limits.h"
#include "frontend/common/uris.h"
#include "tests/common/test_env.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using zetasql_base::testing::StatusIs;

namespace database_api = ::google::spanner::admin::database::v1;
namespace operations_api = ::google::longrunning;
namespace protobuf_api = ::google::protobuf;
namespace spanner_api = ::google::spanner::v1;

class DatabaseApiTest : public test::ServerTest {
 protected:
  void SetUp() override { ZETASQL_EXPECT_OK(CreateTestInstance()); }
  void TearDown() override { ZETASQL_EXPECT_OK(CleanupTestInstance()); }

  absl::Status ListDatabases(const std::string& instance_uri, int32_t page_size,
                             const std::string& page_token,
                             database_api::ListDatabasesResponse* response) {
    grpc::ClientContext context;
    database_api::ListDatabasesRequest request;
    request.set_parent(instance_uri);
    request.set_page_size(page_size);
    request.set_page_token(page_token);
    return test_env()->database_admin_client()->ListDatabases(&context, request,
                                                              response);
  }

  absl::Status CreateDatabase(
      const std::string& instance_uri, const std::string& database_name,
      const std::vector<std::string>& extra_statements = {}) {
    grpc::ClientContext context;
    database_api::CreateDatabaseRequest request;
    request.set_parent(instance_uri);
    request.set_create_statement(
        absl::StrCat("CREATE DATABASE `", database_name, "`"));
    for (auto extra_statement : extra_statements) {
      request.add_extra_statements(extra_statement);
    }
    operations_api::Operation operation;
    ZETASQL_RETURN_IF_ERROR(test_env()->database_admin_client()->CreateDatabase(
        &context, request, &operation));
    ZETASQL_RETURN_IF_ERROR(WaitForOperation(operation.name(), &operation));
    database_api::CreateDatabaseMetadata metadata;
    ZETASQL_RET_CHECK(operation.metadata().UnpackTo(&metadata));
    ZETASQL_RET_CHECK_EQ(metadata.database(),
                 MakeDatabaseUri(instance_uri, database_name));
    return absl::OkStatus();
  }

  absl::Status GetDatabase(const std::string& database_uri,
                           database_api::Database* database) {
    grpc::ClientContext context;
    database_api::GetDatabaseRequest request;
    request.set_name(database_uri);
    return test_env()->database_admin_client()->GetDatabase(&context, request,
                                                            database);
  }

  absl::Status UpdateDatabaseDdl(
      const std::string& database_uri,
      const std::vector<std::string>& update_statements,
      database_api::UpdateDatabaseDdlMetadata* metadata = nullptr) {
    grpc::ClientContext context;
    database_api::UpdateDatabaseDdlRequest request;
    request.set_database(database_uri);
    for (auto statement : update_statements) {
      request.add_statements(statement);
    }
    operations_api::Operation operation;
    ZETASQL_RETURN_IF_ERROR(test_env()->database_admin_client()->UpdateDatabaseDdl(
        &context, request, &operation));
    ZETASQL_RETURN_IF_ERROR(WaitForOperation(operation.name(), &operation));
    if (metadata) {
      ZETASQL_RET_CHECK(operation.metadata().UnpackTo(metadata));
    }
    google::rpc::Status status = operation.error();
    return absl::Status(static_cast<absl::StatusCode>(status.code()),
                        status.message());
  }

  absl::Status GetDatabaseDdl(const std::string& database_uri,
                              database_api::GetDatabaseDdlResponse* response) {
    grpc::ClientContext context;
    database_api::GetDatabaseDdlRequest request;
    request.set_database(database_uri);
    ZETASQL_RETURN_IF_ERROR(test_env()->database_admin_client()->GetDatabaseDdl(
        &context, request, response));
    return absl::OkStatus();
  }

  absl::Status DropDatabase(const std::string& database_uri) {
    grpc::ClientContext context;
    database_api::DropDatabaseRequest request;
    request.set_database(database_uri);
    protobuf_api::Empty response;
    return test_env()->database_admin_client()->DropDatabase(&context, request,
                                                             &response);
  }

 private:
  absl::Status CleanupTestInstance() {
    database_api::ListDatabasesResponse response;
    ZETASQL_RETURN_IF_ERROR(ListDatabases(test_instance_uri_, 0 /*page_size*/,
                                  "" /*page_token*/, &response));
    while (!response.databases().empty()) {
      for (const auto& database : response.databases()) {
        ZETASQL_RETURN_IF_ERROR(DropDatabase(database.name()));
      }
      response.clear_databases();
      if (!response.next_page_token().empty()) {
        std::string next_page_token = response.next_page_token();
        response.clear_next_page_token();
        ZETASQL_RETURN_IF_ERROR(ListDatabases(test_instance_uri_, 0 /*page_size*/,
                                      next_page_token, &response));
      }
    }
    return absl::OkStatus();
  }
};

// Tests for CreateDatabase.

TEST_F(DatabaseApiTest, CreateDatabaseWithInvalidDatabaseName) {
  // Name less than 2 characters.
  EXPECT_THAT(CreateDatabase(test_instance_uri_, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Name greater than 30 characters.
  EXPECT_THAT(CreateDatabase(test_instance_uri_,
                             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Only lowercase allowed.
  EXPECT_THAT(CreateDatabase(test_instance_uri_, "AAAAA"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Non-alphanumeric characters are not allowed.
  EXPECT_THAT(CreateDatabase(test_instance_uri_, "aaaa!@#$aaa"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Cannot end with hypen.
  EXPECT_THAT(CreateDatabase(test_instance_uri_, "aaaa-aaaa-"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(DatabaseApiTest, LimitDatabasePerInstance) {
  for (int i = 0; i < limits::kMaxDatabasesPerInstance; ++i) {
    ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_,
                             absl::StrCat(test_database_name_, i)));
  }
  // Creating
  EXPECT_THAT(CreateDatabase(test_instance_uri_, test_database_name_),
              StatusIs(absl::StatusCode::kResourceExhausted));
}

TEST_F(DatabaseApiTest, CreateDatabaseEmptyInitialSchema) {
  ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_, test_database_name_));
}

TEST_F(DatabaseApiTest, CreateDatabaseWithInitialSchema) {
  ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_, test_database_name_,
                           {
                               R"(
                                 CREATE TABLE test_table (
                                   int64_col INT64 NOT NULL,
                                   string_col STRING(MAX)
                                 ) PRIMARY KEY(int64_col)
                               )",
                           }));
}

TEST_F(DatabaseApiTest, CreateDatabaseWithInvalidInitialSchema) {
  EXPECT_THAT(
      CreateDatabase(test_instance_uri_, test_database_name_, {"INVALID DDL"}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(DatabaseApiTest, CreateDuplicateDatabaseReturnsAlreadyExists) {
  ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_, test_database_name_));

  // Request to create same database.
  EXPECT_THAT(CreateDatabase(test_instance_uri_, test_database_name_),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(DatabaseApiTest, CreateDatabaseWithoutInstanceReturnsNotFound) {
  EXPECT_THAT(CreateDatabase(
                  MakeInstanceUri(test_project_name_, "non-existent-instance"),
                  test_database_name_),
              StatusIs(absl::StatusCode::kNotFound));
}

// Tests for ListDatabases.
static const int32_t kMaxPageSize = 1000;

TEST_F(DatabaseApiTest, DoesNotListDatabasesForUnknownInstance) {
  database_api::ListDatabasesResponse response;
  EXPECT_THAT(ListDatabases(
                  MakeInstanceUri(test_project_name_, "non-existent-instance"),
                  0 /*page_size*/, "" /*page_token*/, &response),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(DatabaseApiTest, ListsEmptyDatabasesForNewInstance) {
  database_api::ListDatabasesResponse response;
  ZETASQL_EXPECT_OK(ListDatabases(test_instance_uri_, 0 /*page_size*/,
                          "" /*page_token*/, &response));
  EXPECT_EQ(response.databases_size(), 0);
}

TEST_F(DatabaseApiTest, ListsCustomPageSizeDatabases) {
  // Create 5+3 = 8 databases test-database0, test-database1,...,
  // test-database7 in test-instance.
  int32_t page_size = 5;
  int32_t last_page_size = 3;
  for (int i = 0; i < page_size + last_page_size; i++) {
    ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_,
                             absl::StrCat(test_database_name_, i)));
  }

  // List databases from test-instance with page_size being 5, i.e., only 5
  // databases test-database0, test-database1,..., test-database4 should be
  // returned with next_page_token pointing to test-database5.
  database_api::ListDatabasesResponse response;
  ZETASQL_EXPECT_OK(ListDatabases(test_instance_uri_, page_size, "" /*page_token*/,
                          &response));
  EXPECT_EQ(response.databases_size(), page_size);
  for (int i = 0; i < page_size; i++) {
    EXPECT_EQ(response.databases(i).name(),
              absl::StrCat(test_database_uri_, i));
  }
  EXPECT_EQ(response.next_page_token(),
            absl::StrCat(test_database_uri_, page_size));

  // Using the next_page_token pointing to test-database5, list next at most 5
  // databases test-database5, test-database6 and test-database7.
  database_api::ListDatabasesResponse response2;
  ZETASQL_EXPECT_OK(ListDatabases(test_instance_uri_, page_size,
                          response.next_page_token(), &response2));
  EXPECT_EQ(response2.databases_size(), last_page_size);
  for (int i = 0; i < last_page_size; i++) {
    EXPECT_EQ(response2.databases(i).name(),
              absl::StrCat(test_database_uri_, i + page_size));
  }
  // No more databases left to be returned and thus next_page_token is not set.
  EXPECT_EQ(response2.next_page_token(), "");
}

// Tests for UpdateDatabaseDdl.

TEST_F(DatabaseApiTest, UpdateDatabaseDdlInvalidDatabaseUri) {
  ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_, test_database_name_));

  // Invalid project name should return an error.
  auto instance_uri =
      MakeInstanceUri("non-existent-project", test_instance_name_);
  EXPECT_THAT(
      UpdateDatabaseDdl(MakeDatabaseUri(instance_uri, test_database_name_), {}),
      StatusIs(absl::StatusCode::kNotFound));

  // Invalid instance name should return an error.
  instance_uri = MakeInstanceUri(test_project_name_, "non-existent-instance");
  EXPECT_THAT(
      UpdateDatabaseDdl(MakeDatabaseUri(instance_uri, test_database_name_), {}),
      StatusIs(absl::StatusCode::kNotFound));

  // Invalid database name should return an error.
  EXPECT_THAT(
      UpdateDatabaseDdl(
          MakeDatabaseUri(test_instance_uri_, "non-existent-database"), {}),
      StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(DatabaseApiTest, UpdateDatabaseDdlPartialSuccess) {
  ZETASQL_EXPECT_OK(CreateTestDatabase());
  ZETASQL_ASSERT_OK_AND_ASSIGN(const std::string session, CreateTestSession());

  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
    single_use_transaction { read_write {} }
    mutations {
      insert {
        table: "test_table"
        columns: "int64_col"
        columns: "string_col"
        values {
          values { string_value: "1" }
          values { string_value: "a" }
        }
        values {
          values { string_value: "2" }
          values { string_value: "a" }
        }
      }
    }
  )");
  *commit_request.mutable_session() = session;
  spanner_api::CommitResponse commit_response;
  ZETASQL_ASSERT_OK(Commit(commit_request, &commit_response));

  database_api::UpdateDatabaseDdlMetadata metadata;
  std::vector<std::string> statements = {R"(
     CREATE TABLE another_table (
       int64_col INT64 NOT NULL,
     ) PRIMARY KEY (int64_col)
  )",
                                         R"(
     CREATE UNIQUE INDEX test_index ON test_table(string_col)
  )"};

  EXPECT_THAT(UpdateDatabaseDdl(test_database_uri_, statements, &metadata),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr(
                           "Found uniqueness violation on index test_index")));

  EXPECT_EQ(metadata.commit_timestamps_size(), 1);
  EXPECT_EQ(metadata.statements_size(), 2);
  for (int i = 0; i < metadata.statements_size(); ++i) {
    EXPECT_EQ(metadata.statements(i), statements[i]);
  }
}

TEST_F(DatabaseApiTest, GetDatabaseNonExistentDatabase) {
  database_api::Database database;
  EXPECT_THAT(GetDatabase(test_database_uri_, &database),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(DatabaseApiTest, GetDatabase) {
  ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_, test_database_name_));

  database_api::Database database;
  ZETASQL_EXPECT_OK(GetDatabase(test_database_uri_, &database));
  EXPECT_EQ(database.name(), test_database_uri_);
}

TEST_F(DatabaseApiTest, DropDatabaseInvalidInstance) {
  auto instance_uri =
      MakeInstanceUri(test_project_name_, "invalid-instance-name");
  EXPECT_THAT(DropDatabase(MakeDatabaseUri(instance_uri, test_database_name_)),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(DatabaseApiTest, DropDatabase) {
  ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_, test_database_name_));

  database_api::Database database;
  ZETASQL_EXPECT_OK(GetDatabase(test_database_uri_, &database));

  ZETASQL_EXPECT_OK(DropDatabase(test_database_uri_));

  EXPECT_THAT(GetDatabase(test_database_uri_, &database),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(DatabaseApiTest, DropDatabaseIdempotent) {
  database_api::Database database;
  EXPECT_THAT(GetDatabase(test_database_uri_, &database),
              StatusIs(absl::StatusCode::kNotFound));

  ZETASQL_EXPECT_OK(DropDatabase(test_database_uri_));
}

TEST_F(DatabaseApiTest, UpdateAndGetDatabaseDDL) {
  std::vector<std::vector<std::string>> test_schemas = {
      {
          R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL,
  string_col STRING(MAX),
  ts TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(int64_col))"},
      {
          R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL,
  string_col STRING(MAX),
) PRIMARY KEY(int64_col))",
          R"(CREATE UNIQUE NULL_FILTERED INDEX test_index ON test_table(string_col))",
      },
      {
          R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL,
  string_col STRING(MAX),
) PRIMARY KEY(int64_col))"},
      {
          R"(CREATE TABLE test_table (
) PRIMARY KEY())"}};

  for (auto schema : test_schemas) {
    ZETASQL_EXPECT_OK(CreateDatabase(test_instance_uri_, test_database_name_, schema));

    database_api::GetDatabaseDdlResponse response;
    ZETASQL_EXPECT_OK(GetDatabaseDdl(test_database_uri_, &response));

    for (int i = 0; i < schema.size(); ++i) {
      EXPECT_THAT(response.statements(i), schema[i]);
    }

    ZETASQL_EXPECT_OK(
        DropDatabase(MakeDatabaseUri(test_instance_uri_, test_database_name_)));
  }
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
