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

#include "frontend/collections/session_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/match.h"
#include "absl/time/time.h"
#include "common/clock.h"
#include "frontend/collections/database_manager.h"
#include "frontend/entities/database.h"
#include "frontend/entities/session.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

class SessionManagerTest : public testing::Test {
 protected:
  SessionManagerTest()
      : session_manager_(&clock_), database_manager_(&clock_) {}

  void SetUp() override {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        database_,
        database_manager_.CreateDatabase(
            "projects/test-p/instances/test-i/databases/test-database", {}));
  }

  Clock clock_;
  Labels test_labels_;
  SessionManager session_manager_;
  DatabaseManager database_manager_;
  std::shared_ptr<Database> database_;
};

TEST_F(SessionManagerTest, CreateSession) {
  absl::Time start_time = absl::Now();
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Session> actual,
                       session_manager_.CreateSession(test_labels_, database_));
  EXPECT_TRUE(
      absl::StartsWith(actual->session_uri(), database_->database_uri()));
  EXPECT_GT(actual->create_time(), start_time);
  EXPECT_GT(actual->approximate_last_use_time(), start_time);
}

TEST_F(SessionManagerTest, GetSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Session> expected,
                       session_manager_.CreateSession(test_labels_, database_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Session> actual,
                       session_manager_.GetSession(expected->session_uri()));
  EXPECT_EQ(actual->session_uri(), expected->session_uri());
  EXPECT_EQ(actual->create_time(), expected->create_time());
  EXPECT_EQ(actual->approximate_last_use_time(),
            expected->approximate_last_use_time());
}

TEST_F(SessionManagerTest, GetSessionFailsAfterVersionGcDuration) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Session> expected,
                       session_manager_.CreateSession(test_labels_, database_));
  // Set the approximate_last_use_time to earlier than gc duration.
  expected->set_approximate_last_use_time(absl::Now() - absl::Hours(1.5));
  EXPECT_THAT(session_manager_.GetSession(expected->session_uri()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(SessionManagerTest, DeleteSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Session> actual,
                       session_manager_.CreateSession(test_labels_, database_));
  ZETASQL_EXPECT_OK(session_manager_.DeleteSession(actual->session_uri()));
  EXPECT_THAT(session_manager_.GetSession(actual->session_uri()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(SessionManagerTest, ListSessions) {
  int num = 5;
  std::shared_ptr<Session> expected;
  for (int i = 0; i < num; i++) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        expected, session_manager_.CreateSession(test_labels_, database_));
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::vector<std::shared_ptr<Session>> actual,
      session_manager_.ListSessions(database_->database_uri()));
  EXPECT_EQ(actual.size(), num);
  for (int i = 0; i < num; i++) {
    EXPECT_TRUE(
        absl::StartsWith(actual[i]->session_uri(), database_->database_uri()));
  }
}

TEST_F(SessionManagerTest, ListSessionsWithSimilarPrefix) {
  int num = 5;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Database> database1,
      database_manager_.CreateDatabase(
          "projects/test-p/instances/test-i/databases/test", {}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Database> database2,
      database_manager_.GetDatabase(
          "projects/test-p/instances/test-i/databases/test-database"));

  std::shared_ptr<Session> expected;
  for (int i = 0; i < num; i++) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        expected, session_manager_.CreateSession(test_labels_, database1));
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(expected,
                       session_manager_.CreateSession(test_labels_, database2));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::vector<std::shared_ptr<Session>> actual,
      session_manager_.ListSessions(database1->database_uri()));
  EXPECT_EQ(actual.size(), num);
  for (int i = 0; i < num; i++) {
    EXPECT_TRUE(absl::StartsWith(
        actual[i]->session_uri(),
        absl::StrCat(database1->database_uri(), "/sessions/")));
  }
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
