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

#include <string>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/numeric.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

using cloud::spanner::MakePgNumeric;
using ::testing::ElementsAre;
using ::zetasql_base::testing::IsOkAndHolds;

namespace {

class PGColumnDefaultTest : public DatabaseTest {
 public:
  PGColumnDefaultTest()
      : feature_flags_({.enable_postgresql_interface = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PGColumnDefaultTest, AddNumericColumnDefault) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE t (
          k bigint primary key,
          value numeric default 1.1 not null
        )
      )",
  }));

  EXPECT_THAT(GetDatabaseDdl(), IsOkAndHolds(ElementsAre(R"(CREATE TABLE t (
  k bigint NOT NULL,
  value numeric DEFAULT 1.1 NOT NULL,
  PRIMARY KEY(k)
))")));

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO t(k) VALUES (1)")}));
  EXPECT_THAT(Query("SELECT value FROM t"),
              IsOkAndHoldsRows({{*MakePgNumeric("1.1")}}));
}

TEST_F(PGColumnDefaultTest, AddNumericColumnAfterInserting) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE t (
          k bigint primary key
        )
      )",
  }));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO t(k) VALUES (1)")}));

  ZETASQL_ASSERT_OK(UpdateSchema({
      "ALTER TABLE t ADD COLUMN value numeric default 1.1 not null",
  }));

  EXPECT_THAT(GetDatabaseDdl(), IsOkAndHolds(ElementsAre(R"(CREATE TABLE t (
  k bigint NOT NULL,
  value numeric DEFAULT 1.1 NOT NULL,
  PRIMARY KEY(k)
))")));
  EXPECT_THAT(Query("SELECT value FROM t"),
              IsOkAndHoldsRows({*MakePgNumeric("1.1")}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
