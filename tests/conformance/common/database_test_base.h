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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_DATABASE_TEST_BASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_DATABASE_TEST_BASE_H_

#include <string>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "zetasql/base/status.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "frontend/server/server.h"
#include "tests/conformance/common/environment.h"
#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/database_admin_client.h"
#include "google/cloud/spanner/instance_admin_client.h"
#include "google/cloud/spanner/keys.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/read_options.h"
#include "google/cloud/spanner/results.h"
#include "google/cloud/spanner/row.h"
#include "google/cloud/spanner/sql_statement.h"
#include "google/cloud/spanner/timestamp.h"
#include "google/cloud/spanner/transaction.h"
#include "google/cloud/spanner/value.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// Base fixture for conformance testing of Cloud Spanner.
//
// This fixture will create a new database for every test case. Several helpers
// are defined within this fixture to reduce the boilerplate required to write
// the conformance tests.
//
// This fixture is intended to work with all Cloud Spanner environments -
// emulator, test env, and production.
//
// This fixture (and accompanying tests) are intended to be completely separate
// from the Cloud Spanner Emulator codebase so that they are reusable even when
// the emulator is completely refactored. Hence, no code from the emulator
// should be included in this fixture or its accompanying tests.
class DatabaseTest : public ::testing::Test {
 public:
  // Standard test fixture callbacks.
  void SetUp() override;
  void TearDown() override;

 protected:
  // Bring several symbols into the namespace of this class for readability.
  using Client = cloud::spanner::Client;
  using CommitResult = cloud::spanner::CommitResult;
  using Mutation = cloud::spanner::Mutation;
  using Mutations = cloud::spanner::Mutations;
  using Transaction = cloud::spanner::Transaction;
  using SqlStatement = cloud::spanner::SqlStatement;
  using DmlResult = cloud::spanner::DmlResult;
  using UpdateDatabaseDdlMetadata =
      admin::database::v1::UpdateDatabaseDdlMetadata;
  using GetDatabaseDdlResponse = admin::database::v1::GetDatabaseDdlResponse;

  using KeyBound = cloud::spanner::KeyBound;
  using KeySet = cloud::spanner::KeySet;

  using Row = cloud::spanner::Row;
  template <typename T>
  using optional = cloud::optional<T>;
  using ReadOptions = cloud::spanner::ReadOptions;
  using Value = cloud::spanner::Value;
  using Timestamp = cloud::spanner::Timestamp;

  static Timestamp MakeMinTimestamp() {
    // 0001-01-01T00:00:00Z
    auto tp = std::chrono::time_point_cast<std::chrono::seconds>(
                  std::chrono::system_clock::from_time_t(0)) -
              std::chrono::seconds(62135596800);
    return google::cloud::spanner::MakeTimestamp(tp).value();
  }

  static Timestamp MakePastTimestamp(std::chrono::seconds past_duration) {
    return google::cloud::spanner::MakeTimestamp(
               std::chrono::system_clock::now() - past_duration)
        .value();
  }

  static Timestamp MakeNowTimestamp() {
    return google::cloud::spanner::MakeTimestamp(
               std::chrono::system_clock::now())
        .value();
  }

  static Timestamp MakeFutureTimestamp(std::chrono::seconds future_duration) {
    return google::cloud::spanner::MakeTimestamp(
               std::chrono::system_clock::now() + future_duration)
        .value();
  }

  static Timestamp MakeMaxTimestamp() {
    int64_t kTimestampMax = 253402300800LL * 1000000 - 1;
    // 9999-12-31T23:59:59.999999Z
    auto tp = std::chrono::time_point_cast<std::chrono::seconds>(
                  std::chrono::system_clock::from_time_t(0)) +
              std::chrono::microseconds(kTimestampMax);
    return google::cloud::spanner::MakeTimestamp(tp).value();
  }

  static zetasql_base::StatusOr<Timestamp> ParseRFC3339TimeSeconds(std::string input) {
    absl::Time result;
    if (!absl::ParseTime(absl::RFC3339_sec, input, &result, nullptr)) {
      return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                          absl::StrCat("Failed to parse input time: ", input));
    }
    auto tp = std::chrono::time_point_cast<std::chrono::seconds>(
                  std::chrono::system_clock::from_time_t(0)) +
              std::chrono::seconds(absl::ToUnixSeconds(result));
    return google::cloud::spanner::MakeTimestamp(tp).value();
  }

  // Callback intended to be implemented by subclasses to setup the database.
  virtual zetasql_base::Status SetUpDatabase() = 0;

  // Resets the database used by the test case (a new database will be created).
  // This is intended for file based tests which run inside a single gunit test
  // case and need to reset the database for every file-based test case.
  zetasql_base::Status ResetDatabase();

  // Sets the schema on the database created for this test.
  zetasql_base::Status SetSchema(const std::vector<std::string>& schema);

  // Updates the schema of the database created for this test. Returning the
  // result in an `UpdateDatabaseDdlMetadata` message.
  zetasql_base::StatusOr<UpdateDatabaseDdlMetadata> UpdateSchema(
      const std::vector<std::string>& schema);

  // Returns the DDL for the database.
  zetasql_base::StatusOr<std::vector<std::string>> GetDatabaseDdl() const;

  // Returns the client used to communicate with the emulator.
  Client& client() { return *client_; }

  // The client library requires explicit construction of Value objects which
  // is cumbersome for unit tests. This class acts as a proxy for implicitly
  // converting a list of C++ objects into client library Value objects.
  class ValueRow {
   public:
    // Creates a vector of Value objects from an argument list.
    template <typename... Ts>
    ValueRow(Ts... values)  // NOLINT
        : row_({cloud::spanner::Value(std::forward<Ts>(values))...}) {}

    // Creates a vector of Value objects from a typed client library Row object.
    ValueRow(const cloud::spanner::Row& row) {  // NOLINT
      for (const google::cloud::spanner::Value& value : row.values()) {
        row_.push_back(value);
      }
    }

    // Casts this proxy object into a vector of Value objects.
    operator std::vector<google::cloud::spanner::Value>() const {  // NOLINT
      return row_;
    }

    // Comparison operator for use in gUnit expectations.
    bool operator==(const ValueRow& other) const { return row_ == other.row_; }

    // Serialization operator for use in gUnit expectations.
    friend std::ostream& operator<<(std::ostream& os, const ValueRow& r) {
      char const* sep = "{";
      for (auto const& v : r.row_) {
        os << sep;
        os << ::testing::PrintToString(v);
        sep = ", ";
      }
      os << "}";
      return os;
    }

   private:
    // Underlying vector of Value objects.
    std::vector<Value> row_;
  };

  struct ReadResult {
    std::vector<ValueRow> values;
    bool has_read_timestamp = false;
    absl::Time read_timestamp;
  };

  // Returns a matcher for a set of rows returned by the client library.
  auto IsOkAndHoldsRows(const std::vector<ValueRow>& rows) {
    return zetasql_base::testing::IsOkAndHolds(testing::ElementsAreArray(rows));
  }

  // Same as above, but used when there is only one row.
  auto IsOkAndHoldsRow(const ValueRow& row) {
    return zetasql_base::testing::IsOkAndHolds(testing::ElementsAre(row));
  }

  // Converts a list of literal key parts into a singleton KeySet.
  template <typename... Ts>
  cloud::spanner::Key Key(Ts... ts) {
    return cloud::spanner::MakeKey(ts...);
  }

  // Convenience functions to create key_set with a single key_range.
  KeySet ClosedOpen(cloud::spanner::Key start, cloud::spanner::Key end) {
    KeySet key_set;
    key_set.AddRange(KeyBound{std::move(start), KeyBound::Bound::kClosed},
                     KeyBound{std::move(end), KeyBound::Bound::kOpen});
    return key_set;
  }

  KeySet ClosedClosed(cloud::spanner::Key start, cloud::spanner::Key end) {
    KeySet key_set;
    key_set.AddRange(KeyBound{std::move(start), KeyBound::Bound::kClosed},
                     KeyBound{std::move(end), KeyBound::Bound::kClosed});
    return key_set;
  }

  KeySet OpenOpen(cloud::spanner::Key start, cloud::spanner::Key end) {
    KeySet key_set;
    key_set.AddRange(KeyBound{std::move(start), KeyBound::Bound::kOpen},
                     KeyBound{std::move(end), KeyBound::Bound::kOpen});
    return key_set;
  }

  KeySet OpenClosed(cloud::spanner::Key start, cloud::spanner::Key end) {
    KeySet key_set;
    key_set.AddRange(KeyBound{std::move(start), KeyBound::Bound::kOpen},
                     KeyBound{std::move(end), KeyBound::Bound::kClosed});
    return key_set;
  }

  // Read using a given transaction.
  zetasql_base::StatusOr<ReadResult> Read(Transaction txn, std::string table,
                                  std::vector<std::string> columns,
                                  KeySet key_set) {
    auto result = client().Read(std::move(txn), std::move(table),
                                std::move(key_set), std::move(columns));
    ReadResult read_result;
    auto time = result.ReadTimestamp();
    if (time.has_value()) {
      auto tp = time->get<std::chrono::system_clock::time_point>();
      read_result.read_timestamp = absl::FromChrono(tp.value());
      read_result.has_read_timestamp = true;
    }
    std::vector<ValueRow> values;
    for (const auto& row : result) {
      if (!row.ok()) {
        return ToUtilStatus(row.status());
      }
      values.push_back(row.value());
    }
    read_result.values = values;
    return read_result;
  }

  // Read using a single-use transaction read options.
  zetasql_base::StatusOr<std::vector<ValueRow>> Read(
      Transaction::SingleUseOptions transaction_options, std::string table,
      std::vector<std::string> columns, KeySet key_set) {
    auto result =
        client().Read(std::move(transaction_options), std::move(table),
                      std::move(key_set), std::move(columns));
    std::vector<ValueRow> values;
    for (const auto& row : result) {
      if (!row.ok()) {
        return ToUtilStatus(row.status());
      }
      values.push_back(row.value());
    }
    return values;
  }

  // Collects all rows from a read operation into a single vector.
  zetasql_base::StatusOr<std::vector<ValueRow>> Read(std::string table,
                                             std::vector<std::string> columns,
                                             KeySet key_set) {
    return ReadWithIndex(std::move(table), "", std::move(columns),
                         std::move(key_set));
  }

  zetasql_base::StatusOr<std::vector<ValueRow>> Read(std::string table,
                                             std::vector<std::string> columns,
                                             cloud::spanner::Key key) {
    KeySet key_set;
    key_set.AddKey(std::move(key));
    return Read(std::move(table), std::move(columns), key_set);
  }

  // Same as version above except reads with a specified index.
  zetasql_base::StatusOr<std::vector<ValueRow>> ReadWithIndex(
      std::string table, std::string index, std::vector<std::string> columns,
      KeySet key_set) {
    ReadOptions options;
    options.index_name = std::move(index);
    auto result = client().Read(std::move(table), std::move(key_set),
                                std::move(columns), options);
    std::vector<ValueRow> retval;
    for (const auto& row : result) {
      if (!row.ok()) {
        return ToUtilStatus(row.status());
      }
      retval.push_back(row.value());
    }
    return retval;
  }

  // Collects all rows from a table into a single vector.
  zetasql_base::StatusOr<std::vector<ValueRow>> ReadAll(
      std::string table, std::vector<std::string> columns) {
    return ReadAllWithIndex(std::move(table), "", std::move(columns));
  }

  // Same as version above except reads with a specified index.
  zetasql_base::StatusOr<std::vector<ValueRow>> ReadAllWithIndex(
      std::string table, std::string index, std::vector<std::string> columns) {
    return ReadWithIndex(std::move(table), std::move(index), std::move(columns),
                         KeySet::All());
  }

  zetasql_base::StatusOr<std::vector<ValueRow>> Query(const std::string& query) {
    auto result =
        client().ExecuteQuery(google::cloud::spanner::SqlStatement(query));
    std::vector<ValueRow> retval;
    for (const auto& row : result) {
      if (!row.ok()) {
        return ToUtilStatus(row.status());
      }
      retval.push_back(row.value());
    }
    return retval;
  }

  // Execute query using an existing transaction.
  zetasql_base::StatusOr<std::vector<ValueRow>> QueryTransaction(
      Transaction txn, const std::string& query) {
    auto result = client().ExecuteQuery(txn, SqlStatement(query));
    std::vector<ValueRow> retval;
    for (const auto& row : result) {
      if (!row.ok()) {
        return ToUtilStatus(row.status());
      }
      retval.push_back(row.value());
    }
    return retval;
  }

  zetasql_base::StatusOr<DmlResult> ExecuteDml(const std::string& statement) {
    return ExecuteDml(SqlStatement(statement));
  }

  zetasql_base::StatusOr<DmlResult> ExecuteDml(const SqlStatement sql_statement) {
    return ExecuteDmlTransaction(Transaction(Transaction::ReadWriteOptions()),
                                 sql_statement);
  }

  zetasql_base::StatusOr<DmlResult> ExecuteDmlTransaction(
      Transaction txn, const SqlStatement sql_statement) {
    auto dml_result = client().ExecuteDml(txn, sql_statement);
    if (!dml_result.ok()) {
      return ToUtilStatus(std::move(dml_result).status());
    }
    return std::move(dml_result).value();
  }

  // Returns a client library NULL value of the specified type.
  template <typename T>
  cloud::spanner::Value Null() {
    return cloud::spanner::MakeNullValue<T>();
  }

  // Helper methods to create Mutation objects.
  template <typename... Ts>
  Mutation MakeInsert(std::string table_name, std::vector<std::string> columns,
                      Ts&&... values) {
    return cloud::spanner::MakeInsertMutation(
        std::move(table_name), std::move(columns), std::forward<Ts>(values)...);
  }

  template <typename... Ts>
  Mutation MakeUpdate(std::string table_name, std::vector<std::string> columns,
                      Ts&&... values) {
    return cloud::spanner::MakeUpdateMutation(
        std::move(table_name), std::move(columns), std::forward<Ts>(values)...);
  }

  template <typename... Ts>
  Mutation MakeInsertOrUpdate(std::string table_name,
                              std::vector<std::string> columns,
                              Ts&&... values) {
    return cloud::spanner::MakeInsertOrUpdateMutation(
        std::move(table_name), std::move(columns), std::forward<Ts>(values)...);
  }

  template <typename... Ts>
  Mutation MakeReplace(std::string table_name, std::vector<std::string> columns,
                       Ts&&... values) {
    return cloud::spanner::MakeReplaceMutation(
        std::move(table_name), std::move(columns), std::forward<Ts>(values)...);
  }

  template <typename... Ts>
  Mutation MakeDelete(const std::string& table_name, Ts&&... values) {
    return cloud::spanner::MakeDeleteMutation(table_name,
                                              std::forward<Ts>(values)...);
  }

  // Commits the given mutations inside a transaction runner.
  zetasql_base::StatusOr<CommitResult> Commit(Mutations mutations);

  // Commits the given transaction with the set of mutations provided.
  zetasql_base::StatusOr<CommitResult> CommitTransaction(Transaction txn,
                                                 Mutations mutations);

  // Commits the given transaction with the set of sql statements provided.
  zetasql_base::StatusOr<CommitResult> CommitDml(
      const std::vector<std::string>& statements) {
    std::vector<SqlStatement> sql_statements;
    for (auto statement : statements) {
      sql_statements.push_back(SqlStatement(statement));
    }
    return CommitDml(sql_statements);
  }

  // Commits a set of sql statements.
  zetasql_base::StatusOr<CommitResult> CommitDml(
      const std::vector<SqlStatement>& sql_statements);

  // Rollback the given transaction.
  zetasql_base::Status Rollback(Transaction txn);

  // Group of helpers for committing single-row operations of various kinds.
  zetasql_base::StatusOr<CommitResult> Insert(std::string table,
                                      std::vector<std::string> columns,
                                      ValueRow row);
  zetasql_base::StatusOr<CommitResult> MultiInsert(std::string table,
                                           std::vector<std::string> columns,
                                           std::vector<ValueRow> rows);
  zetasql_base::StatusOr<CommitResult> Update(std::string table,
                                      std::vector<std::string> columns,
                                      ValueRow row);
  zetasql_base::StatusOr<CommitResult> MultiUpdate(std::string table,
                                           std::vector<std::string> columns,
                                           std::vector<ValueRow> rows);
  zetasql_base::StatusOr<CommitResult> Delete(std::string table,
                                      cloud::spanner::Key key);
  zetasql_base::StatusOr<CommitResult> Delete(std::string table,
                                      std::vector<cloud::spanner::Key> keys);
  zetasql_base::StatusOr<CommitResult> Delete(std::string table, KeySet key_set);
  zetasql_base::StatusOr<CommitResult> InsertOrUpdate(std::string table,
                                              std::vector<std::string> columns,
                                              ValueRow row);
  zetasql_base::StatusOr<CommitResult> MultiInsertOrUpdate(
      std::string table, std::vector<std::string> columns,
      std::vector<ValueRow> rows);
  zetasql_base::StatusOr<CommitResult> Replace(std::string table,
                                       std::vector<std::string> columns,
                                       ValueRow row);
  zetasql_base::StatusOr<CommitResult> MultiReplace(std::string table,
                                            std::vector<std::string> columns,
                                            std::vector<ValueRow> rows);

  bool in_prod_env() const { return GetConformanceTestGlobals().in_prod_env; }

 private:
  // The database used in this test (a new one is created for each test case).
  std::unique_ptr<cloud::spanner::Database> database_;

  // The client used for the database admin api.
  std::unique_ptr<cloud::spanner::DatabaseAdminClient> database_client_;

  // The client used for the spanner client api.
  std::unique_ptr<cloud::spanner::Client> client_;
};

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_DATABASE_TEST_BASE_H_
