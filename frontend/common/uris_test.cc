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

#include "frontend/common/uris.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/string_view.h"

namespace google {
namespace spanner {
namespace emulator {
namespace {

TEST(UriUtilTest, ParseProjectUri) {
  absl::string_view project_uri = "projects/test-project-0";
  absl::string_view project_id;
  ZETASQL_EXPECT_OK(ParseProjectUri(project_uri, &project_id));
  EXPECT_EQ(project_id, "test-project-0");
}

TEST(UriUtilTest, InvalidProjectUriReturnsInvalidArgumentError) {
  absl::string_view invalid_uri = "project/test-project-0";
  absl::string_view project_id;
  EXPECT_THAT(ParseProjectUri(invalid_uri, &project_id),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  invalid_uri = "/projects/test-project-0";
  EXPECT_THAT(ParseProjectUri(invalid_uri, &project_id),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(UriUtilTest, MakeProjectUri) {
  absl::string_view project_id = "test-project-0";
  EXPECT_EQ(MakeProjectUri(project_id), "projects/test-project-0");
}

TEST(UriUtilTest, ParseInstanceConfigUri) {
  absl::string_view instance_config_uri =
      "projects/test-project-0/instanceConfigs/test-instance-config-0";
  absl::string_view project_id, instance_config_id;
  ZETASQL_EXPECT_OK(ParseInstanceConfigUri(instance_config_uri, &project_id,
                                   &instance_config_id));
  EXPECT_EQ(project_id, "test-project-0");
  EXPECT_EQ(instance_config_id, "test-instance-config-0");
}

TEST(UriUtilTest, ParseDatabaseUri) {
  absl::string_view database_uri =
      "projects/test-project/instances/test-instance/databases/"
      "test-database";
  absl::string_view project_id, instance_id, database_id;
  ZETASQL_EXPECT_OK(
      ParseDatabaseUri(database_uri, &project_id, &instance_id, &database_id));
  EXPECT_EQ(project_id, "test-project");
  EXPECT_EQ(instance_id, "test-instance");
  EXPECT_EQ(database_id, "test-database");
}

TEST(UriUtilTest, ParseInvalidDatabaseUriReturnsInvalidArgumentError) {
  absl::string_view project_id, instance_id, database_id;
  EXPECT_THAT(ParseDatabaseUri("invalid-projects/test-project/instanceConfigs/"
                               "test-instance/databases/"
                               "test-database",
                               &project_id, &instance_id, &database_id),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(
      ParseDatabaseUri(
          "projects/test-project/invalid-instances/test-instance/databases/"
          "test-database",
          &project_id, &instance_id, &database_id),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(
      ParseDatabaseUri(
          "projects/test-project/instances/test-instance/invalid-databases/"
          "test-database",
          &project_id, &instance_id, &database_id),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(ParseDatabaseUri(
                  "projects/test-project/instances/test-instance//databases/"
                  "test-database",
                  &project_id, &instance_id, &database_id),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(UriUtilTest, ParseSessionUri) {
  absl::string_view session_uri =
      "projects/test-project/instances/test-instance/databases/test-database/"
      "sessions/test-session-000";
  absl::string_view project_id, instance_id, database_id, session_id;
  ZETASQL_EXPECT_OK(ParseSessionUri(session_uri, &project_id, &instance_id,
                            &database_id, &session_id));
  EXPECT_EQ(project_id, "test-project");
  EXPECT_EQ(instance_id, "test-instance");
  EXPECT_EQ(database_id, "test-database");
  EXPECT_EQ(session_id, "test-session-000");
}

TEST(UriUtilTest, InvalidSessionUri) {
  absl::string_view session_uri =
      "projects/test-project/instances/test-instance/databases/test-database/"
      "session/test-session-000";
  absl::string_view project_id, instance_id, database_id, session_id;
  EXPECT_THAT(ParseSessionUri(session_uri, &project_id, &instance_id,
                              &database_id, &session_id),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(UriUtilTest, InvalidInstanceConfigUriReturnsInvalidArgumentError) {
  absl::string_view project_id, instance_config_id;
  absl::string_view invalid_uri =
      "projects/test-project-0/instances/test-instance-config-0";
  EXPECT_THAT(
      ParseInstanceConfigUri(invalid_uri, &project_id, &instance_config_id),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(UriUtilTest, MakeInstanceConfigUri) {
  absl::string_view project_id = "test-project-0";
  absl::string_view instance_config_id = "test-instance-config-0";
  EXPECT_EQ(MakeInstanceConfigUri(project_id, instance_config_id),
            "projects/test-project-0/instanceConfigs/test-instance-config-0");
}

TEST(UriUtilTest, ParseInstanceUri) {
  absl::string_view instance_uri =
      "projects/test-project-0/instances/test-instance-0";
  absl::string_view project_id, instance_id;
  ZETASQL_EXPECT_OK(ParseInstanceUri(instance_uri, &project_id, &instance_id));
  EXPECT_EQ(project_id, "test-project-0");
  EXPECT_EQ(instance_id, "test-instance-0");
}

TEST(UriUtilTest, InvalidInstanceUriReturnsInvalidArgumentError) {
  absl::string_view project_id, instance_id;
  absl::string_view invalid_uri =
      "projects/test-project-0/instanceConfigs/test-instance-0";
  EXPECT_THAT(ParseInstanceUri(invalid_uri, &project_id, &instance_id),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(UriUtilTest, MakeInstanceUri) {
  absl::string_view project_id = "test-project-0";
  absl::string_view instance_id = "test-instance-0";
  EXPECT_EQ(MakeInstanceUri(project_id, instance_id),
            "projects/test-project-0/instances/test-instance-0");
}

TEST(UriUtilTest, MakeDatabaseUri) {
  absl::string_view instance_uri =
      "projects/test-project-0/instances/test-instance-0";
  absl::string_view database_id = "test-database-0";
  EXPECT_EQ(MakeDatabaseUri(instance_uri, database_id),
            "projects/test-project-0/instances/test-instance-0/databases/"
            "test-database-0");
}

TEST(UriUtilTest, MakeSessionUri) {
  absl::string_view database_uri =
      "projects/test-project-0/instances/test-instance-0/databases/"
      "test-database-0";
  absl::string_view session_id = "test-session-0";
  EXPECT_EQ(MakeSessionUri(database_uri, session_id),
            "projects/test-project-0/instances/test-instance-0/databases/"
            "test-database-0/sessions/test-session-0");
}

}  // namespace
}  // namespace emulator
}  // namespace spanner
}  // namespace google
