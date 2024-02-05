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

#include "backend/schema/catalog/named_schema.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// For the following tests, a custom PG DDL statement is required as translating
// expressions from GSQL to PG is not supported in tests.
using database_api::DatabaseDialect::POSTGRESQL;

// TODO: Add more tests once we can add tables, views, indexes, and
// sequences through SQL. We can then test adding these objects and DROP SCHEMA
// errors when the named schema still contains dependencies.
TEST_P(SchemaUpdaterTest, CreateNamedSchema_Basic) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
           CREATE SCHEMA mynamedschema
        )"},
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
         CREATE SCHEMA mynamedschema
      )"}));
  }

  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  EXPECT_NE(named_schema, nullptr);
  EXPECT_EQ(named_schema->Name(), "mynamedschema");
}

TEST_P(SchemaUpdaterTest, DropNamedSchema_Success) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
           CREATE SCHEMA mynamedschema
        )"},
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/true));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
         CREATE SCHEMA mynamedschema
      )"}));
  }

  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  EXPECT_NE(named_schema, nullptr);
  EXPECT_EQ(named_schema->Name(), "mynamedschema");

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
           DROP SCHEMA mynamedschema
        )"},
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/true));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
         DROP SCHEMA mynamedschema
      )"}));
  }
  const NamedSchema* dropped_named_schema =
      schema->FindNamedSchema("mynamedschema");
  EXPECT_EQ(dropped_named_schema, nullptr);
}

TEST_P(SchemaUpdaterTest, DropNamedSchema_Failed) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
           DROP SCHEMA mynamedschema
        )"},
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/true),
                StatusIs(error::NamedSchemaNotFound("mynamedschema")));
  } else {
    EXPECT_THAT(CreateSchema({R"(
         DROP SCHEMA mynamedschema
      )"}),
                StatusIs(error::NamedSchemaNotFound("mynamedschema")));
  }
}
}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
