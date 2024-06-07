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

#include "backend/database/pg_oid_assigner/pg_oid_assigner.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using ::zetasql_base::testing::StatusIs;

class PgOidAssignerTest : public ::testing::Test {};

TEST_F(PgOidAssignerTest, SingleStatement) {
  auto pg_oid_assigner = PgOidAssigner(/*enabled=*/true);
  pg_oid_assigner.BeginAssignment();  // Marks start of DDL processing.
  // Assign 2 OIDs.
  pg_oid_assigner.GetNextPostgresqlOid();
  pg_oid_assigner.GetNextPostgresqlOid();
  // Marks end of a DDL statement.
  pg_oid_assigner.MarkNextPostgresqlOidForIntermediateSchema();
  // next_postgresql_oid() should not be updated until after EndAssignment().
  EXPECT_EQ(pg_oid_assigner.TEST_next_postgresql_oid(), 100000);
  EXPECT_EQ(pg_oid_assigner.TEST_tentative_next_postgresql_oid(), 100002);
  ZETASQL_EXPECT_OK(pg_oid_assigner.EndAssignment());  // Marks end of DDL processing.
  EXPECT_EQ(pg_oid_assigner.TEST_next_postgresql_oid(), 100002);
}

// This captures the case where a DDL statement is parsed correct but fails
// validation. In this case, the schema is rolled back to the state prior to the
// validation failure. The PgOidAssigner should match this rollback.
TEST_F(PgOidAssignerTest, IntermediateRollback) {
  auto pg_oid_assigner = PgOidAssigner(/*enabled=*/true);
  pg_oid_assigner.BeginAssignment();  // Marks start of DDL processing.
  // Assign 2 OIDs.
  pg_oid_assigner.GetNextPostgresqlOid();
  pg_oid_assigner.GetNextPostgresqlOid();
  // Marks end of first DDL statement.
  pg_oid_assigner.MarkNextPostgresqlOidForIntermediateSchema();
  EXPECT_EQ(pg_oid_assigner.TEST_tentative_next_postgresql_oid(), 100002);
  // Assign 3 OIDs.
  pg_oid_assigner.GetNextPostgresqlOid();
  pg_oid_assigner.GetNextPostgresqlOid();
  pg_oid_assigner.GetNextPostgresqlOid();
  // Marks end of second DDL statement.
  pg_oid_assigner.MarkNextPostgresqlOidForIntermediateSchema();
  EXPECT_EQ(pg_oid_assigner.TEST_next_postgresql_oid(), 100000);
  EXPECT_EQ(pg_oid_assigner.TEST_tentative_next_postgresql_oid(), 100005);
  // End assignment by rolling back to first statement.
  ZETASQL_EXPECT_OK(pg_oid_assigner.EndAssignmentAtIntermediateSchema(0));
  EXPECT_EQ(pg_oid_assigner.TEST_next_postgresql_oid(), 100002);
}

// This captures the case where a schema change fails during parsing. In this
// case, EndAssignment() is not called which means calling BeginAssignment()
// again will reset the tentative_next_postgresql_oid.
TEST_F(PgOidAssignerTest, FailedSchemaChange) {
  auto pg_oid_assigner = PgOidAssigner(/*enabled=*/true);
  pg_oid_assigner.BeginAssignment();  // Marks start of DDL processing.
  // Assign 2 OIDs.
  pg_oid_assigner.GetNextPostgresqlOid();
  pg_oid_assigner.GetNextPostgresqlOid();
  // Marks end of first DDL statement.
  pg_oid_assigner.MarkNextPostgresqlOidForIntermediateSchema();
  EXPECT_EQ(pg_oid_assigner.TEST_tentative_next_postgresql_oid(), 100002);
  pg_oid_assigner.BeginAssignment();  // Marks start of DDL processing.
  EXPECT_EQ(pg_oid_assigner.TEST_tentative_next_postgresql_oid(), 100000);
  EXPECT_EQ(pg_oid_assigner.TEST_next_postgresql_oid(), 100000);
}

TEST_F(PgOidAssignerTest, OidAssigner_EndAssignmentErrors) {
  auto pg_oid_assigner = PgOidAssigner(/*enabled=*/true);
  EXPECT_THAT(pg_oid_assigner.EndAssignment(),
              StatusIs(absl::StatusCode::kInternal));

  EXPECT_THAT(pg_oid_assigner.EndAssignmentAtIntermediateSchema(0),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(PgOidAssignerTest, OidAssigner_Disabled) {
  auto pg_oid_assigner = PgOidAssigner(/*enabled=*/false);
  pg_oid_assigner.BeginAssignment();  // Marks start of DDL processing.
  // Assign 2 OIDs.
  EXPECT_EQ(pg_oid_assigner.GetNextPostgresqlOid(), std::nullopt);
  EXPECT_EQ(pg_oid_assigner.GetNextPostgresqlOid(), std::nullopt);
  // Marks end of a DDL statement.
  pg_oid_assigner.MarkNextPostgresqlOidForIntermediateSchema();
  // next_postgresql_oid() should not be updated until after EndAssignment().
  EXPECT_EQ(pg_oid_assigner.TEST_next_postgresql_oid(), 100000);
  EXPECT_EQ(pg_oid_assigner.TEST_tentative_next_postgresql_oid(), 100000);
  ZETASQL_EXPECT_OK(pg_oid_assigner.EndAssignment());  // Marks end of DDL processing.
  EXPECT_EQ(pg_oid_assigner.TEST_next_postgresql_oid(), 100000);
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
