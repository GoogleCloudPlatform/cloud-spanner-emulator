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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/options.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

using database_api::DatabaseDialect::POSTGRESQL;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;
using DatabaseOptionTest = SchemaUpdaterTest;

INSTANTIATE_TEST_SUITE_P(
    SchemaUpdaterPerDialectTests, DatabaseOptionTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<DatabaseOptionTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(DatabaseOptionTest, ValidWitnessLocationOptionName) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
      ALTER DATABASE db SET spanner.witness_location = 'us-east1'
                                      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      ALTER DATABASE db SET OPTIONS (witness_location = "us-east1")
        )"}));
  }
}

TEST_P(DatabaseOptionTest, ValidDefaultLeaderOptionName) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
      ALTER DATABASE db SET spanner.default_leader = 'us-east-1'
                                      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      ALTER DATABASE db SET OPTIONS (default_leader = "us-east-1")
        )"}));
  }
}

TEST_P(DatabaseOptionTest, ValidReadLeaseRegionsOptionName) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
      ALTER DATABASE db SET spanner.read_lease_regions = 'us-east1'
                                      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      ALTER DATABASE db SET OPTIONS (read_lease_regions = "us-east1")
        )"}));
  }
}

TEST_P(DatabaseOptionTest, ValidDefaultSequenceKindOptionName) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
      ALTER DATABASE db SET spanner.default_sequence_kind = DEFAULT
                                      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      ALTER DATABASE db SET OPTIONS (default_sequence_kind = NULL)
        )"}));
  }
}

TEST_P(DatabaseOptionTest, ValidDefaultTimeZoneOptionName) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC'
                                      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      ALTER DATABASE db SET OPTIONS (default_time_zone = 'UTC')
        )"}));
  }
  ASSERT_TRUE(schema->options()->default_time_zone().has_value());
  EXPECT_EQ(schema->options()->default_time_zone().value(), "UTC");
}

TEST_P(DatabaseOptionTest, InvalidDatabaseOptionName) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
      ALTER DATABASE db
      SET spanner.internal.cloud_default_sequence_kind = 'bit_reversed_positive'
                              )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("Database option "
                                   "<spanner.internal.cloud_default_sequence_"
                                   "kind> is not supported.")));

    EXPECT_THAT(
        CreateSchema({R"(
      ALTER DATABASE db SET OPTIONS (
        cloud_default_sequence_kind = 'bit_reversed_positive')
        )"}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("Option: cloud_default_sequence_kind is unknown.")));
  } else {
    EXPECT_THAT(
        CreateSchema({R"(
      ALTER DATABASE db SET OPTIONS (
        cloud_default_sequence_kind = 'bit_reversed_positive')
        )"}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("Option: cloud_default_sequence_kind is unknown.")));
  }
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
