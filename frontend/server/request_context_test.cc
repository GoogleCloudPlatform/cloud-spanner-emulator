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

#include "frontend/server/request_context.h"

#include <memory>

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "frontend/common/uris.h"
#include "tests/common/test_env.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;

class SessionExistenceTest : public testing::Test {
 protected:
  void SetUp() override {
    env_ = absl::make_unique<ServerEnv>();
    request_context_ =
        absl::make_unique<RequestContext>(env_.get(), /*grpc=*/nullptr);

    const std::string instance_uri =
        "projects/test-project/instances/test-instance";
    const std::string database_uri =
        absl::StrCat(instance_uri, "/databases/test-database");

    // Create an instance.
    instance_api::Instance instance_pb = PARSE_TEXT_PROTO(R"(
      name: "projects/test-project/instances/test-instance"
      display_name: ""
      node_count: 3
    )");
    ZETASQL_ASSERT_OK(
        env_->instance_manager()->CreateInstance(instance_uri, instance_pb));
    // Create a database that belongs to the instance created above.
    std::vector<std::string> empty_schema;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Database> database,
        env_->database_manager()->CreateDatabase(database_uri, empty_schema));

    // Create a session that belongs to the database created above.
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Session> session,
                         env_->session_manager()->CreateSession({}, database));

    absl::string_view project_id, instance_id, database_id;
    ZETASQL_EXPECT_OK(ParseSessionUri(session->session_uri(), &project_id, &instance_id,
                              &database_id, &session_id_));
  }

  absl::string_view session_id_;
  std::unique_ptr<ServerEnv> env_;
  std::unique_ptr<RequestContext> request_context_;
};

TEST_F(SessionExistenceTest, CorrectSessionUri) {
  const std::string session_uri = absl::StrCat(
      "projects/test-project/instances/test-instance/databases/test-database/"
      "sessions/",
      session_id_);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Session> session,
                       GetSession(request_context_.get(), session_uri));
}

TEST_F(SessionExistenceTest, WrongSessionId) {
  std::string wrong_session_uri =
      "projects/test-project/instances/test-instance/databases/test-database/"
      "sessions/nonexist-session";
  EXPECT_THAT(GetSession(request_context_.get(), wrong_session_uri),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kNotFound,
                  testing::MatchesRegex(".*Session not found.*")));
}

TEST_F(SessionExistenceTest, WrongDatabaseId) {
  const std::string wrong_session_uri = absl::StrCat(
      "projects/test-project/instances/test-instance/databases/nonexist-db/"
      "sessions/",
      session_id_);
  EXPECT_THAT(GetSession(request_context_.get(), wrong_session_uri),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kNotFound,
                  testing::MatchesRegex(".*Database not found.*")));
}

TEST_F(SessionExistenceTest, WrongInstanceId) {
  const std::string wrong_session_uri = absl::StrCat(
      "projects/test-project/instances/nonexist-instance/databases/"
      "test-database/sessions/",
      session_id_);
  EXPECT_THAT(GetSession(request_context_.get(), wrong_session_uri),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kNotFound,
                  testing::MatchesRegex(".*Instance not found.*")));
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
