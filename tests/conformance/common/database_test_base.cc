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

#include "tests/conformance/common/database_test_base.h"

#include <chrono>  // NOLINT(build/c++11)
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "google/cloud/spanner/backoff_policy.h"
#include "google/cloud/spanner/batch_dml_result.h"
#include "google/cloud/spanner/commit_result.h"
#include "google/cloud/spanner/connection_options.h"
#include "google/cloud/spanner/create_instance_request_builder.h"
#include "google/cloud/spanner/database_admin_connection.h"
#include "google/cloud/spanner/internal/database_admin_stub.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/polling_policy.h"
#include "google/cloud/spanner/retry_policy.h"
#include "google/cloud/spanner/transaction.h"
#include "google/cloud/status_or.h"
#include "tests/conformance/common/environment.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

void DatabaseTest::SetUp() {
  // Get the global environment in which the test runs.
  const ConformanceTestGlobals& globals = GetConformanceTestGlobals();

  // Specify custom retry, backoff, and polling policies. The default policies
  // are on the order of 10's of seconds. We want the tests to complete much
  // faster than that, so we set the policies on the order of milliseconds. By
  // default TestEnv conformance tests wait quite a while for admin operations
  // like create instance/database/schema.
  auto retry_policy = absl::make_unique<cloud::spanner::LimitedTimeRetryPolicy>(
      std::chrono::seconds(120));
  auto backoff_policy =
      absl::make_unique<cloud::spanner::ExponentialBackoffPolicy>(
          std::chrono::milliseconds(1), std::chrono::milliseconds(2), 1.01);
  auto polling_policy =
      absl::make_unique<cloud::spanner::GenericPollingPolicy<>>(
          *retry_policy, *backoff_policy);

  // Pick a unique database name for every test (reuse the instance).
  database_ = absl::make_unique<google::cloud::spanner::Database>(
      google::cloud::spanner::Instance(globals.project_id, globals.instance_id),
      absl::StrCat("test-database-", absl::ToUnixMicros(absl::Now())));

  // Setup the database client.
  database_client_ = absl::make_unique<cloud::spanner::DatabaseAdminClient>(
      cloud::spanner::MakeDatabaseAdminConnection(
          *globals.connection_options, retry_policy->clone(),
          backoff_policy->clone(), polling_policy->clone()));
  ZETASQL_ASSERT_OK(ToUtilStatusOr(database_client_->CreateDatabase(*database_).get()));

  // Setup a client to interact with the database.
  client_ = absl::make_unique<cloud::spanner::Client>(
      google::cloud::spanner::MakeConnection(*database_,
                                             *globals.connection_options));

  // Setup stubs to access low-level API features not exposed by the C++ client.
  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateChannel(globals.connection_options->endpoint(),
                          globals.connection_options->credentials());
  spanner_stub_ = v1::Spanner::NewStub(channel);
  database_admin_stub_ =
      admin::database::v1::DatabaseAdmin::NewStub(channel);
  operations_stub_ = longrunning::Operations::NewStub(channel);

  // Allow test suites to customize the database at SetUp time.
  ZETASQL_ASSERT_OK(SetUpDatabase());
}

void DatabaseTest::TearDown() { database_client_->DropDatabase(*database_); }

absl::Status DatabaseTest::ResetDatabase() {
  const ConformanceTestGlobals& globals = GetConformanceTestGlobals();
  ZETASQL_RETURN_IF_ERROR(ToUtilStatus(database_client_->DropDatabase(*database_)));
  database_ = absl::make_unique<google::cloud::spanner::Database>(
      google::cloud::spanner::Instance(globals.project_id, globals.instance_id),
      absl::StrCat("test-database-", absl::ToUnixMicros(absl::Now())));
  return ToUtilStatus(
      database_client_->CreateDatabase(*database_).get().status());
}

absl::Status DatabaseTest::SetSchema(const std::vector<std::string>& schema) {
  auto status_or = database_client_->UpdateDatabase(*database_, schema).get();
  if (!status_or.ok()) {
    return ToUtilStatus(status_or.status());
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<DatabaseTest::UpdateDatabaseDdlMetadata>
DatabaseTest::UpdateSchema(const std::vector<std::string>& schema) {
  auto status_or = database_client_->UpdateDatabase(*database_, schema).get();
  if (!status_or.ok()) {
    return ToUtilStatus(status_or.status());
  }
  return status_or.value();
}

zetasql_base::StatusOr<std::vector<std::string>> DatabaseTest::GetDatabaseDdl() const {
  auto status_or = database_client_->GetDatabaseDdl(*database_);
  if (!status_or.ok()) {
    return ToUtilStatus(status_or.status());
  }
  GetDatabaseDdlResponse response = status_or.value();
  std::vector<std::string> ddl_statements;
  ddl_statements.reserve(response.statements_size());
  for (int i = 0; i < response.statements_size(); ++i) {
    ddl_statements.push_back(response.statements(i));
  }
  return ddl_statements;
}

// Helper typedefs to make code below a bit more readable.
using cloud::spanner::CommitResult;
using InsertMutationBuilder = cloud::spanner::InsertMutationBuilder;
using UpdateMutationBuilder = cloud::spanner::UpdateMutationBuilder;
using ReplaceMutationBuilder = cloud::spanner::ReplaceMutationBuilder;
using InsertOrUpdateMutationBuilder =
    cloud::spanner::InsertOrUpdateMutationBuilder;
using DeleteMutationBuilder = cloud::spanner::DeleteMutationBuilder;
using Transaction = cloud::spanner::Transaction;
using BatchDmlResult = cloud::spanner::BatchDmlResult;
using PartitionedDmlResult = cloud::spanner::PartitionedDmlResult;

zetasql_base::StatusOr<PartitionedDmlResult> DatabaseTest::ExecutePartitionedDml(
    const SqlStatement& sql_statement) {
  return ToUtilStatusOr(client().ExecutePartitionedDml(sql_statement));
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::Commit(Mutations mutations) {
  return ToUtilStatusOr(client().Commit(
      [&](Transaction) -> cloud::StatusOr<Mutations> { return mutations; }));
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::CommitTransaction(
    Transaction txn, Mutations mutations) {
  return ToUtilStatusOr(client().Commit(txn, mutations));
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::CommitDml(
    const std::vector<SqlStatement>& sql_statements) {
  return ToUtilStatusOr(client().Commit(
      [&](Transaction const& txn) -> cloud::StatusOr<Mutations> {
        for (const auto& sql_statement : sql_statements) {
          auto result = client().ExecuteDml(txn, sql_statement);
          if (!result) return result.status();
        }
        return Mutations{};
      }));
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::CommitDmlTransaction(
    Transaction txn, const std::vector<SqlStatement>& sql_statements) {
  for (const auto& sql_statement : sql_statements) {
    auto result = client().ExecuteDml(txn, sql_statement);
    if (!result) return ToUtilStatus(result.status());
  }
  auto result = client().Commit(txn, Mutations{});
  if (!result) return ToUtilStatus(result.status());
  return result.value();
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::CommitBatchDml(
    const std::vector<SqlStatement>& sql_statements) {
  return ToUtilStatusOr(client().Commit(
      [&](Transaction const& txn) -> cloud::StatusOr<Mutations> {
        auto result = client().ExecuteBatchDml(txn, sql_statements);
        if (!result) return result.status();
        if (!result.value().status.ok()) return result.value().status;
        return Mutations{};
      }));
}

zetasql_base::StatusOr<BatchDmlResult> DatabaseTest::BatchDmlTransaction(
    Transaction txn, const std::vector<SqlStatement>& sql_statements) {
  auto result = client().ExecuteBatchDml(txn, sql_statements);
  if (!result) return ToUtilStatus(result.status());
  return result.value();
}

absl::Status DatabaseTest::Rollback(Transaction txn) {
  auto status = client().Rollback(txn);
  return absl::Status(absl::StatusCode(status.code()), status.message());
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::Insert(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit({InsertMutationBuilder(table, columns).AddRow(row).Build()});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::MultiInsert(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = InsertMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::Update(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit({UpdateMutationBuilder(table, columns).AddRow(row).Build()});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::MultiUpdate(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = UpdateMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::Delete(std::string table,
                                                  cloud::spanner::Key key) {
  KeySet key_set;
  key_set.AddKey(key);
  return Commit({cloud::spanner::MakeDeleteMutation(table, key_set)});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::Delete(
    std::string table, std::vector<cloud::spanner::Key> keys) {
  KeySet key_set;
  for (const auto& key : keys) {
    key_set.AddKey(key);
  }
  return Commit({cloud::spanner::MakeDeleteMutation(table, key_set)});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::Delete(
    std::string table, cloud::spanner::KeySet key_set) {
  return Commit({cloud::spanner::MakeDeleteMutation(table, key_set)});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::InsertOrUpdate(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit(
      {InsertOrUpdateMutationBuilder(table, columns).AddRow(row).Build()});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::MultiInsertOrUpdate(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = InsertOrUpdateMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::Replace(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit({ReplaceMutationBuilder(table, columns).AddRow(row).Build()});
}

zetasql_base::StatusOr<CommitResult> DatabaseTest::MultiReplace(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = ReplaceMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
