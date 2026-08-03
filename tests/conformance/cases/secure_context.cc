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
#include <string>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "tests/conformance/common/database_test_base.h"
#include "tests/conformance/common/environment.h"
#include "grpcpp/client_context.h"
#include "grpcpp/support/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using ::testing::ElementsAre;
using ::googlesql_base::testing::StatusIs;

google::protobuf::Value MakeStringValue(absl::string_view s) {
  google::protobuf::Value val;
  val.set_string_value(s);
  return val;
}

google::protobuf::Value MakeNullValue() {
  google::protobuf::Value val;
  val.set_null_value(google::protobuf::NULL_VALUE);
  return val;
}

google::protobuf::Value MakeBoolValue(bool b) {
  google::protobuf::Value val;
  val.set_bool_value(b);
  return val;
}

class SecureContextTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

  absl::StatusOr<std::vector<std::vector<std::string>>>
  ExecuteSqlWithSecureContext(
      absl::string_view sql,
      const absl::flat_hash_map<std::string, google::protobuf::Value>&
          secure_context) {
    grpc::ClientContext create_session_context;
    v1::CreateSessionRequest create_session_request;
    create_session_request.set_database(database()->FullName());
    v1::Session session;
    grpc::Status status = raw_client()->CreateSession(
        &create_session_context, create_session_request, &session);
    if (!status.ok()) {
      return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                          status.error_message());
    }

    grpc::ClientContext context;
    v1::ExecuteSqlRequest request;
    request.set_session(session.name());
    request.set_sql(sql);
    auto* sc_map = request.mutable_request_options()
                       ->mutable_client_context()
                       ->mutable_secure_context();
    for (const auto& [key, value] : secure_context) {
      (*sc_map)[key] = value;
    }

    v1::ResultSet response;
    status = raw_client()->ExecuteSql(&context, request, &response);
    if (!status.ok()) {
      return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                          status.error_message());
    }

    std::vector<std::vector<std::string>> results;
    results.reserve(response.rows().size());
    for (const auto& row : response.rows()) {
      std::vector<std::string> result_row;
      result_row.reserve(row.values().size());
      for (const auto& value : row.values()) {
        if (value.kind_case() == google::protobuf::Value::kStringValue) {
          result_row.push_back(value.string_value());
        } else if (value.kind_case() == google::protobuf::Value::kNumberValue) {
          result_row.push_back(absl::StrCat(value.number_value()));
        } else if (value.kind_case() == google::protobuf::Value::kBoolValue) {
          result_row.push_back(value.bool_value() ? "true" : "false");
        } else if (value.kind_case() == google::protobuf::Value::kNullValue) {
          result_row.push_back("NULL");
        } else {
          result_row.push_back("UNKNOWN");
        }
      }
      results.push_back(result_row);
    }
    return results;
  }
};

TEST_F(SecureContextTest, SelectSecureContext) {
  // Test reading a present key
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT('foo')",
                                  {{"foo", MakeStringValue("bar")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("bar")));

  // Test reading a missing key
  results = ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT('baz')",
                                        {{"foo", MakeStringValue("bar")}});
  EXPECT_THAT(results.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("Missing secure parameter: baz")));

  // Test reading with empty context
  results = ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT('foo')", {});
  EXPECT_THAT(results.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("Missing secure parameter: foo")));

  // Test passing NULL argument to SECURE_CONTEXT
  results =
      ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT(CAST(NULL AS STRING))",
                                  {{"foo", MakeStringValue("bar")}});
  EXPECT_THAT(results.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("The argument to SECURE_CONTEXT() "
                                            "cannot be NULL.")));

  // Test reading a key with null value in the map
  results = ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT('foo')",
                                        {{"foo", MakeNullValue()}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("NULL")));

  // Test reading a key with invalid type (bool) in the map
  results = ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT('foo')",
                                        {{"foo", MakeBoolValue(true)}});
  EXPECT_THAT(results.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("Secure parameters must be "
                                            "string or null values.")));
}

TEST_F(SecureContextTest, HelperParsesAllTypes) {
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext(
          "SELECT true, 3.14, 123456789, 'test', CAST(NULL AS STRING)", {});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("true", "3.14", "123456789",
                                                "test", "NULL")));
}

TEST_F(SecureContextTest, SelectSecureContextInView) {
  // Create a view that uses SECURE_CONTEXT
  GOOGLESQL_ASSERT_OK(
      UpdateSchema({"CREATE VIEW SecureView SQL SECURITY INVOKER AS SELECT "
                    "SECURE_CONTEXT('foo') AS val"})
          .status());

  // Test reading from the view with a present key
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext("SELECT val FROM SecureView",
                                  {{"foo", MakeStringValue("bar")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("bar")));
}

TEST_F(SecureContextTest, FilterWithSecureContextInView) {
  // Create a table with owner column
  GOOGLESQL_ASSERT_OK(
      UpdateSchema(
          {"CREATE TABLE Items (id INT64 NOT NULL, name STRING(MAX), owner "
           "STRING(MAX)) PRIMARY KEY(id)"})
          .status());

  // Populate data
  GOOGLESQL_EXPECT_OK(Insert("Items", {"id", "name", "owner"},
                   {Value(1), Value("item1"), Value("alice")})
                .status());
  GOOGLESQL_EXPECT_OK(Insert("Items", {"id", "name", "owner"},
                   {Value(2), Value("item2"), Value("bob")})
                .status());

  // Create a view that filters by SECURE_CONTEXT
  GOOGLESQL_ASSERT_OK(UpdateSchema({"CREATE VIEW SecureItems SQL SECURITY INVOKER AS "
                          "SELECT i.id, i.name, "
                          "i.owner FROM Items i "
                          "WHERE i.owner = SECURE_CONTEXT('user')"})
                .status());

  // Query the view with secure context for alice
  absl::flat_hash_map<std::string, google::protobuf::Value> secure_context = {
      {"user", MakeStringValue("alice")}};
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext("SELECT id, name FROM SecureItems",
                                  secure_context);
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("1", "item1")));

  // Query the view with secure context for bob
  secure_context = {{"user", MakeStringValue("bob")}};
  results = ExecuteSqlWithSecureContext("SELECT id, name FROM SecureItems",
                                        secure_context);
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("2", "item2")));
}

TEST_F(SecureContextTest, NestedSecureContextView) {
  GOOGLESQL_ASSERT_OK(
      UpdateSchema({"CREATE VIEW InnerView SQL SECURITY INVOKER AS SELECT "
                    "SECURE_CONTEXT('inner_key') AS inner_val",
                    "CREATE VIEW OuterView SQL SECURITY INVOKER AS SELECT "
                    "InnerView.inner_val FROM InnerView"})
          .status());

  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext(
          "SELECT * FROM OuterView",
          {{"inner_key", MakeStringValue("inner_value")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("inner_value")));
}

TEST_F(SecureContextTest, JoinViewsWithSharedSecureContext) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({"CREATE VIEW ViewA SQL SECURITY INVOKER AS SELECT "
                          "SECURE_CONTEXT('shared_key') AS val_a",
                          "CREATE VIEW ViewB SQL SECURITY INVOKER AS SELECT "
                          "SECURE_CONTEXT('shared_key') AS val_b"})
                .status());

  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext(
          "SELECT a.val_a, b.val_b FROM ViewA a CROSS JOIN ViewB b "
          "WHERE a.val_a = SECURE_CONTEXT('shared_key')",
          {{"shared_key", MakeStringValue("shared_value")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results,
              ElementsAre(ElementsAre("shared_value", "shared_value")));
}

TEST_F(SecureContextTest, JoinViewsWithDistinctSecureContext) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({"CREATE VIEW ViewC SQL SECURITY INVOKER AS SELECT "
                          "SECURE_CONTEXT('key_c') AS val_c",
                          "CREATE VIEW ViewD SQL SECURITY INVOKER AS SELECT "
                          "SECURE_CONTEXT('key_d') AS val_d"})
                .status());

  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext(
          "SELECT c.val_c, d.val_d FROM ViewC c CROSS JOIN ViewD d "
          "WHERE c.val_c != SECURE_CONTEXT('key_e')",
          {{"key_c", MakeStringValue("value_c")},
           {"key_d", MakeStringValue("value_d")},
           {"key_e", MakeStringValue("value_e")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("value_c", "value_d")));
}

TEST_F(SecureContextTest, MissingSecureContextInJoinedView) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({"CREATE VIEW ViewE SQL SECURITY INVOKER AS SELECT "
                          "SECURE_CONTEXT('key_e') AS val_e",
                          "CREATE VIEW ViewF SQL SECURITY INVOKER AS SELECT "
                          "SECURE_CONTEXT('key_f') AS val_f"})
                .status());

  // Omit 'key_f' from the context
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext(
          "SELECT e.val_e, f.val_f FROM ViewE e CROSS JOIN ViewF f",
          {{"key_e", MakeStringValue("value_e")}});
  EXPECT_THAT(
      results.status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("Missing secure parameter: key_f")));
}

TEST_F(SecureContextTest, NullAndInvalidValuesInView) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({"CREATE VIEW ViewG SQL SECURITY INVOKER AS SELECT "
                          "SECURE_CONTEXT('key_g') AS val_g"})
                .status());

  // Provide NULL value
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext("SELECT val_g FROM ViewG",
                                  {{"key_g", MakeNullValue()}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("NULL")));

  // Provide invalid boolean value
  results = ExecuteSqlWithSecureContext("SELECT val_g FROM ViewG",
                                        {{"key_g", MakeBoolValue(true)}});
  EXPECT_THAT(
      results.status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr(
                   "Secure parameters must be string or null values.")));
}

TEST_F(SecureContextTest, CaseInsensitiveKeys) {
  // Verify that 'Foo' matches 'foo' (case-insensitive fallback)
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT('Foo')",
                                  {{"foo", MakeStringValue("bar")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("bar")));

  // Verify that exact case matches
  results = ExecuteSqlWithSecureContext("SELECT SECURE_CONTEXT('Foo')",
                                        {{"Foo", MakeStringValue("baz")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("baz")));
}

TEST_F(SecureContextTest, CreateDefinerView) {
  // Create a view that uses SECURE_CONTEXT and SQL SECURITY DEFINER
  GOOGLESQL_ASSERT_OK(
      UpdateSchema({"CREATE VIEW DefinerView SQL SECURITY DEFINER AS SELECT "
                    "SECURE_CONTEXT('foo') AS val"})
          .status());

  // Test reading from the view with a present key
  absl::StatusOr<std::vector<std::vector<std::string>>> results =
      ExecuteSqlWithSecureContext("SELECT val FROM DefinerView",
                                  {{"foo", MakeStringValue("bar")}});
  GOOGLESQL_ASSERT_OK(results);
  EXPECT_THAT(*results, ElementsAre(ElementsAre("bar")));

  // Check INFORMATION_SCHEMA.VIEWS to verify it is DEFINER
  if (!GetConformanceTestGlobals().in_prod_env) {
    absl::StatusOr<std::vector<ValueRow>> view_results = Query(
        "SELECT security_type FROM INFORMATION_SCHEMA.VIEWS WHERE table_name = "
        "'DefinerView'");
    GOOGLESQL_ASSERT_OK(view_results);
    EXPECT_THAT(*view_results, ElementsAre(ValueRow(Value("DEFINER"))));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
