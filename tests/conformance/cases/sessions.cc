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

#include "google/protobuf/empty.pb.h"
#include "google/protobuf/timestamp.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::IsOk;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

static constexpr int kMaxBatchCreateSessionsCount = 100;

// Tests APIs for session management. Uses low-level API stubs for session
// creation/management because these APIs are not exposed by the
// google::spanner::Client used by the test base class.
class SessionsTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE TestTable()
      PRIMARY KEY()
    )"});
  }

  // Creates a new session with the given database URI.
  zetasql_base::StatusOr<spanner_api::Session> CreateSession(
      absl::string_view database_uri,
      const std::map<std::string, std::string>& labels = {}) {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    request.set_database(std::string(database_uri));  // NOLINT
    if (!labels.empty()) {
      auto label_map = request.mutable_session()->mutable_labels();
      for (const auto& key_type : labels) {
        (*label_map)[key_type.first] = key_type.second;
      }
    }
    spanner_api::Session response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response;
  }

  // Creates a new session that belongs to the database for the test.
  zetasql_base::StatusOr<spanner_api::Session> CreateSession(
      const std::map<std::string, std::string>& labels = {}) {
    return CreateSession(database()->FullName(), labels);
  }

  // Gets the last use timestamp for the specified `session_name`.
  zetasql_base::StatusOr<google::protobuf::Timestamp> GetSessionLastUseTimestamp(
      absl::string_view session_name) {
    grpc::ClientContext context;
    spanner_api::GetSessionRequest request;
    request.set_name(std::string(session_name));  // NOLINT
    spanner_api::Session response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->GetSession(&context, request, &response));
    return response.approximate_last_use_time();
  }

  // Lists the active sessions up to the specified `page_size` limit and sets
  // `page_token` to the page token for the next page of results.
  zetasql_base::StatusOr<std::vector<spanner_api::Session>> ListSessionsPage(
      const std::string& uri, int32_t page_size = 1,
      std::string* page_token = nullptr) {
    std::vector<spanner_api::Session> sessions;
    v1::ListSessionsRequest request;
    request.set_database(uri);
    request.set_page_size(page_size);
    v1::ListSessionsResponse response;
    if (page_token) {
      request.set_page_token(*page_token);
    }
    grpc::ClientContext context;
    ZETASQL_RETURN_IF_ERROR(raw_client()->ListSessions(&context, request, &response));
    for (const auto& session : response.sessions()) {
      sessions.push_back(session);
    }
    if (page_token) {
      *page_token = response.next_page_token();
    }
    return sessions;
  }

  // Gets a session.
  zetasql_base::StatusOr<std::string> GetSession(absl::string_view session_name) {
    grpc::ClientContext context;
    spanner_api::GetSessionRequest request;
    request.set_name(std::string(session_name));  // NOLINT
    spanner_api::Session response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->GetSession(&context, request, &response));
    return response.name();
  }

  // Deletes a session.
  absl::Status DeleteSession(absl::string_view session_name) {
    grpc::ClientContext context;
    spanner_api::DeleteSessionRequest request;
    request.set_name(std::string(session_name));  // NOLINT
    google::protobuf::Empty response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->DeleteSession(&context, request, &response));
    return absl::OkStatus();
  }

  // Batch-creates the specified number of sessions.
  zetasql_base::StatusOr<std::vector<std::string>> BatchCreateSessions(
      const std::string& uri, int32_t num_sessions) {
    std::vector<std::string> sessions;
    grpc::ClientContext context;
    v1::BatchCreateSessionsRequest request;
    request.set_database(uri);
    request.set_session_count(num_sessions);
    v1::BatchCreateSessionsResponse response;
    ZETASQL_RETURN_IF_ERROR(
        raw_client()->BatchCreateSessions(&context, request, &response));
    for (const auto& session : *response.mutable_session()) {
      sessions.push_back(session.name());
    }
    return sessions;
  }
};

TEST_F(SessionsTest, CreateSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
  EXPECT_THAT(session.name(), testing::HasSubstr(absl::StrCat(
                                  database()->FullName(), "/sessions/")));
  EXPECT_TRUE(session.has_create_time());
  EXPECT_TRUE(session.has_approximate_last_use_time());
}

TEST_F(SessionsTest, CreateSessionWithLabel) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session,
                       CreateSession({{"test_key", "test_value"}}));
  EXPECT_THAT(session.name(), testing::HasSubstr(absl::StrCat(
                                  database()->FullName(), "/sessions/")));
  // Creating session doesn't return labels in response.
  EXPECT_EQ(session.labels_size(), 1);
  EXPECT_TRUE(session.has_create_time());
  EXPECT_TRUE(session.has_approximate_last_use_time());
}

TEST_F(SessionsTest, ListSessionsWithNonExistentInstanceReturnsNotFound) {
  // This returns "Instance not found" instead of "Database not found".
  std::string page_token;
  auto status = ListSessionsPage(
      "projects/test-project/instances/fake-instance/databases/test-database",
      4, &page_token);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(SessionsTest,
       BatchCreatesSessionWithNonExistentInstanceReturnsNotFound) {
  // This returns "Instance not found" instead of "Database not found".
  auto status = BatchCreateSessions(
      "projects/test-project/instances/fake-instance/databases/test-database",
      1);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(SessionsTest, CreatesSessionWithNonExistentInstanceReturnsNotFound) {
  EXPECT_THAT(CreateSession("projects/test-project/instances/fake-instance/"
                            "databases/test-database"),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(SessionsTest, CreatesSessionWithNonExistentDatabaseReturnsNotFound) {
  EXPECT_THAT(CreateSession("projects/test-project/instances/test-instance/"
                            "databases/doesnotexist"),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(SessionsTest,
       CreatesSessionWithInvalidDatabaseUriReturnsInvalidArgument) {
  EXPECT_THAT(CreateSession("database/test-database"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SessionsTest, GetSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
  EXPECT_THAT(GetSession(session.name()), IsOkAndHolds(session.name()));
}

TEST_F(SessionsTest, GetSessionWithInvalidSessionUriReturnsInvalidArgument) {
  EXPECT_THAT(GetSession(/*session_name=*/""),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SessionsTest, DeleteSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
  ZETASQL_EXPECT_OK(DeleteSession(session.name()));
}

TEST_F(SessionsTest, DeleteSessionWithInvalidSessionUriReturnsInvalidArgument) {
  EXPECT_THAT(DeleteSession(/*session_name=*/""),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SessionsTest, DeleteSessionAndGetSessionReturnsNotFound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
  ZETASQL_EXPECT_OK(DeleteSession(session.name()));
  EXPECT_THAT(GetSession(session.name()),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(SessionsTest, CanDeleteDeletedSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
  ZETASQL_EXPECT_OK(DeleteSession(session.name()));
  ZETASQL_EXPECT_OK(DeleteSession(session.name()));
}

TEST_F(SessionsTest, LastUseTimeIncreases) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto t1, GetSessionLastUseTimestamp(session.name()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto t2, GetSessionLastUseTimestamp(session.name()));
  auto less_equal = [](google::protobuf::Timestamp& t1,
                       google::protobuf::Timestamp& t2) {
    if (t1.seconds() < t2.seconds()) {
      return true;
    }
    if (t1.seconds() == t2.seconds()) {
      return t1.nanos() <= t2.nanos();
    }
    return false;
  };
  EXPECT_TRUE(less_equal(t1, t2));
}

TEST_F(SessionsTest, ListSessionsWithPageSize) {
  std::vector<std::string> expected;
  expected.reserve(10);
  for (int i = 0; i < 10; ++i) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
    expected.emplace_back(session.name());
  }

  std::vector<std::string> actual;
  actual.reserve(10);
  std::string page_token;
  for (int page_size : {4, 3, 2, 1}) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto sessions_list,
        ListSessionsPage(database()->FullName(), page_size, &page_token));
    EXPECT_EQ(sessions_list.size(), page_size);
    for (auto& session : sessions_list) {
      actual.emplace_back(session.name());
    }
  }
  // After paginating through all the sessions, the page token should be empty.
  EXPECT_TRUE(page_token.empty());
  EXPECT_THAT(actual, testing::UnorderedElementsAreArray(expected));
}

TEST_F(SessionsTest, ListAllSessions) {
  std::vector<std::string> expected;
  expected.reserve(5);
  for (int i = 0; i < 5; ++i) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());
    expected.emplace_back(session.name());
  }

  // Setting page size to -1 returns all sessions.
  std::string page_token;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sessions_list,
      ListSessionsPage(database()->FullName(), /*page_size=*/-1, &page_token));
  std::vector<std::string> actual;
  actual.reserve(5);
  for (auto& session : sessions_list) {
    actual.emplace_back(session.name());
  }

  EXPECT_TRUE(page_token.empty());
  EXPECT_THAT(actual, testing::UnorderedElementsAreArray(expected));
}

TEST_F(SessionsTest, ListSessionsWithLabels) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession({{"abc", "def"}}));

  std::string page_token;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sessions_list,
      ListSessionsPage(database()->FullName(), /*page_size=*/-1, &page_token));
  EXPECT_EQ(sessions_list[0].labels_size(), 1);
  EXPECT_EQ(sessions_list[0].labels().find("abc")->second, "def");
}

TEST_F(SessionsTest, BatchCreateSessionReturnsMultipleSessions) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sessions,
      BatchCreateSessions(database()->FullName(), /*num_sessions=*/10));
  // Check that we have the right number.
  EXPECT_EQ(10, sessions.size());

  // Check that there are no duplicates.
  absl::flat_hash_set<std::string> session_uris;
  for (const auto& session : sessions) {
    session_uris.insert(session);
  }
  EXPECT_EQ(10, session_uris.size());
}

TEST_F(SessionsTest, BatchCreateSessionsReturnsErrorsOnInvalidArg) {
  EXPECT_THAT(BatchCreateSessions(database()->FullName(), /*num_sessions=*/-1),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SessionsTest, BatchCreateSessionSilentlyTruncatesRequestCount) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sessions,
      BatchCreateSessions(database()->FullName(),
                          /*num_sessions=*/kMaxBatchCreateSessionsCount * 3));
  EXPECT_EQ(sessions.size(), kMaxBatchCreateSessionsCount);
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
