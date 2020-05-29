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

#include "backend/schema/catalog/versioned_catalog.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "absl/time/time.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

TEST(VersionedCatalogTest, FindSchemaAtTimeStamp) {
  VersionedCatalog catalog;

  absl::Time t1 = absl::Now();
  absl::Time t2 = t1 + absl::Seconds(1);
  absl::Time t3 = t2 + absl::Seconds(1);
  ZETASQL_EXPECT_OK(catalog.AddSchema(t1, absl::make_unique<const Schema>()));
  ZETASQL_EXPECT_OK(catalog.AddSchema(t3, absl::make_unique<const Schema>()));

  // Find schemas created at t1 and t3.
  const Schema* schema_t1 = catalog.GetSchema(t1);
  const Schema* schema_t3 = catalog.GetSchema(t3);
  EXPECT_NE(schema_t1, schema_t3);

  // Find a schema created at or before t2; expect the schema created at t1.
  EXPECT_EQ(catalog.GetSchema(t2), schema_t1);

  // Find a schema created at or before t4; expect the schema created at t3.
  absl::Time t4 = t3 + absl::Seconds(1);
  EXPECT_EQ(catalog.GetSchema(t4), schema_t3);
}

TEST(VersionedCatalog, FirstAndLastSchema) {
  VersionedCatalog catalog;
  absl::Time t1 = absl::Now();
  ZETASQL_EXPECT_OK(catalog.AddSchema(t1, absl::make_unique<const Schema>()));

  // Find the default initial schema using absl::InfinitePast(). Expect it to be
  // different from the schema created at t1.
  EXPECT_NE(catalog.GetSchema(absl::InfinitePast()), catalog.GetSchema(t1));

  // Find the last schema using absl::InfiniteFuture(). Expect the schema
  // created at t1.
  EXPECT_EQ(catalog.GetSchema(absl::InfiniteFuture()), catalog.GetSchema(t1));
}

TEST(VersionedCatalogTest, InitialSchema) {
  absl::Time t1 = absl::Now();
  VersionedCatalog catalog(absl::make_unique<const Schema>());
  absl::Time t0 = t1 - absl::Seconds(10);

  // Verify that the initial schema can be read with a timestamp in the past.
  EXPECT_EQ(catalog.GetSchema(t0), catalog.GetSchema(t1));
}

TEST(VersionedCatalogTest, FindFirstSchemaBeforeCreation) {
  VersionedCatalog catalog;

  absl::Time t1 = absl::Now();
  absl::Time t2 = t1 + absl::Seconds(1);
  ZETASQL_EXPECT_OK(catalog.AddSchema(t2, absl::make_unique<const Schema>()));

  // Confirm that the schema created at t2 is not visible at t1.
  EXPECT_NE(catalog.GetSchema(t1), catalog.GetSchema(t2));
}

TEST(VersionedCatalogTest, AddSchemaWithSameOrEarlierCreationTime) {
  VersionedCatalog catalog;
  absl::Time t1 = absl::Now();
  absl::Time t2 = t1 + absl::Seconds(1);

  ZETASQL_EXPECT_OK(catalog.AddSchema(t2, absl::make_unique<const Schema>()));
  EXPECT_THAT(catalog.AddSchema(t2, absl::make_unique<const Schema>()),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInternal,
                  testing::MatchesRegex(".*Failed to insert schema.*")));
  EXPECT_THAT(catalog.AddSchema(t1, absl::make_unique<const Schema>()),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInternal,
                  testing::MatchesRegex(".*Failed to insert schema.*")));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
