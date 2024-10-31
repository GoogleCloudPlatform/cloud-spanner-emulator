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
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_node.h"
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
                                      /*proto_descriptor_bytes=*/"",
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

TEST_P(SchemaUpdaterTest, CreateNamedSchema_NestedFails) {
  // POSTGRESQL throws error on nested schema before the emulator does.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"(
         CREATE SCHEMA mynamedschema.nested
      )"}),
              StatusIs(error::SchemaObjectTypeUnsupportedInNamedSchema(
                  "Schema", "mynamedschema.nested")));
}

TEST_P(SchemaUpdaterTest, CreateNamedSchema_ReservedSchemaNamesFailed) {
  EXPECT_THAT(CreateSchema({R"(CREATE SCHEMA Net)"}),
              StatusIs(error::InvalidSchemaName("Schema", "Net")));

  EXPECT_THAT(
      CreateSchema({R"(CREATE SCHEMA DEFINITION_SCHEMA)"}),
      StatusIs(error::InvalidSchemaName("Schema", "DEFINITION_SCHEMA")));
}

TEST_P(SchemaUpdaterTest, DropNamedSchema_Success) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
           CREATE SCHEMA mynamedschema
        )"},
                                      /*proto_descriptor_bytes=*/"",
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
                                      /*proto_descriptor_bytes=*/"",
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
                             /*proto_descriptor_bytes=*/"",
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

TEST_P(SchemaUpdaterTest, TableWithNamedSchema_CreateAndDropSuccess) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(CREATE TABLE mynamedschema.T(col1 BIGINT PRIMARY KEY))"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(CREATE TABLE mynamedschema.T(col1 INT64) PRIMARY KEY(col1))"}));
  }

  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);

  const Table* t = named_schema->FindTable("mynamedschema.T");
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->Name(),
            GetParam() == POSTGRESQL ? "mynamedschema.t" : "mynamedschema.T");

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, UpdateSchema(schema.get(), {R"(DROP TABLE mynamedschema.T)"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, UpdateSchema(schema.get(), {R"(DROP TABLE mynamedschema.T)"}));
  }

  named_schema = schema->FindNamedSchema("mynamedschema");
  EXPECT_NE(named_schema, nullptr);
  EXPECT_EQ(
      named_schema->FindTable(GetParam() == POSTGRESQL ? "mynamedschema.t"
                                                       : "mynamedschema.T"),
      nullptr);
  EXPECT_EQ(named_schema->FindTableUsingSynonym("mynamedschema.syn"), nullptr);
}

TEST_P(SchemaUpdaterTest,
       TableWithNamedSchema_CreateAndDropWithSynonymSuccess) {
  // Synonyms not supported during table POSTGRESQL table creation
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema,
                       CreateSchema({R"(CREATE SCHEMA mynamedschema)",
                                     R"(
      CREATE TABLE mynamedschema.T (col1 INT64, SYNONYM(`mynamedschema.syn`))
      PRIMARY KEY (col1)
      )"}));

  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);

  const Table* synonym_t =
      named_schema->FindTableUsingSynonym("mynamedschema.syn");
  ASSERT_NE(synonym_t, nullptr);
  EXPECT_EQ(synonym_t->Name(), "mynamedschema.T");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema, UpdateSchema(schema.get(), {R"(DROP TABLE mynamedschema.T)"}));

  named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);
  EXPECT_EQ(named_schema->FindTable("mynamedschema.T"), nullptr);
  EXPECT_EQ(named_schema->FindTableUsingSynonym("mynamedschema.syn"), nullptr);
}

TEST_P(SchemaUpdaterTest, TableWithNamedSchema_NoExistingSchemaFails) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        CreateSchema(
            {R"(CREATE TABLE mynamedschema.T (col1 BIGINT PRIMARY KEY))"},
            /*proto_descriptor_bytes=*/"", /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::NamedSchemaNotFound("mynamedschema")));
  } else {
    EXPECT_THAT(CreateSchema({R"(CREATE TABLE mynamedschema.T (
                        col1 INT64,
                        ) PRIMARY KEY(col1))"}),
                StatusIs(error::NamedSchemaNotFound("mynamedschema")));
  }
}

TEST_P(SchemaUpdaterTest, TableWithNamedSchema_AddAndDropSynonym) {
  std::unique_ptr<const Schema> schema;
  std::string table_name =
      GetParam() == POSTGRESQL ? "mynamedschema.t" : "mynamedschema.T";
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {
                R"(CREATE SCHEMA mynamedschema)",
                absl::Substitute(R"(CREATE TABLE $0 (col1 BIGINT primary key))",
                                 table_name),
            },
            /*proto_descriptor_bytes=*/"", /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema({R"(CREATE SCHEMA mynamedschema)",
                      absl::Substitute(
                          R"(CREATE TABLE $0 (col1 INT64) PRIMARY KEY (col1))",
                          table_name)}));
  }
  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);
  ASSERT_NE(named_schema->FindTable(table_name), nullptr);

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        UpdateSchema(
            schema.get(),
            {absl::Substitute("ALTER TABLE $0 ADD SYNONYM syn", table_name)},
            /*proto_descriptor_bytes=*/"", /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, UpdateSchema(schema.get(),
                             {absl::Substitute("ALTER TABLE $0 ADD SYNONYM syn",
                                               table_name)}));
  }

  named_schema = schema->FindNamedSchema("mynamedschema");
  const Table* synonym_t = schema->FindTableUsingSynonym("syn");
  EXPECT_NE(synonym_t, nullptr);
  EXPECT_EQ(synonym_t->Name(), table_name);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema, UpdateSchema(schema.get(),
                           {absl::Substitute("ALTER TABLE $0 DROP SYNONYM syn",
                                             table_name)}));

  named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema->FindTable(table_name), nullptr);
  EXPECT_EQ(named_schema->FindTableUsingSynonym("syn"), nullptr);
}

TEST_P(SchemaUpdaterTest, ViewWithNamedSchema_CreateAndDropSuccess) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(
      CREATE TABLE mynamedschema.T (
        col1 bigint primary key,
        col2 varchar
      )
    )",
             R"(CREATE OR REPLACE VIEW mynamedschema.V SQL SECURITY INVOKER AS
             SELECT t.col1, t.col2 FROM mynamedschema.T as t)"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE SCHEMA mynamedschema)",
                                               R"(
      CREATE TABLE mynamedschema.T (
        col1 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY(col1)
    )",
                                               R"(
      CREATE OR REPLACE VIEW `mynamedschema.V` SQL SECURITY INVOKER AS
      SELECT t.col1, t.col2 FROM mynamedschema.T as t
    )"}));
  }

  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);

  const View* v = named_schema->FindView("mynamedschema.V");
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->Name(),
            GetParam() == POSTGRESQL ? "mynamedschema.v" : "mynamedschema.V");

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, UpdateSchema(schema.get(), {"DROP VIEW mynamedschema.V"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, UpdateSchema(schema.get(), {"DROP VIEW mynamedschema.V"}));
  }

  named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);
  ASSERT_EQ(
      named_schema->FindView(GetParam() == POSTGRESQL ? "mynamedschema.v"
                                                      : "mynamedschema.V"),
      nullptr);
}

TEST_P(SchemaUpdaterTest, ViewWithNamedSchema_NoExistingSchemaFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE TABLE T (
        col1 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY(col1)
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(),
                           {
                               R"(
      CREATE OR REPLACE VIEW `mynamedschema.V` SQL SECURITY INVOKER AS
      SELECT t.col1, t.col2 FROM T as t ORDER BY t.col2
    )"}),
              StatusIs(error::NamedSchemaNotFound("mynamedschema")));
}

TEST_P(SchemaUpdaterTest, SequenceWithNamedSchema_CreateAndDropSuccess) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(CREATE SEQUENCE mynamedschema.myseq BIT_REVERSED_POSITIVE)",
             R"(CREATE TABLE mynamedschema.T (
              col1 bigint DEFAULT nextval('mynamedschema.myseq') PRIMARY KEY)
              )"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, CreateSchema({R"(CREATE SCHEMA mynamedschema)",
                              R"(CREATE SEQUENCE mynamedschema.myseq OPTIONS (
                        sequence_kind = "bit_reversed_positive"
                        ))",
                              R"(CREATE TABLE mynamedschema.T (
                      col1 INT64 DEFAULT (
                        GET_NEXT_SEQUENCE_VALUE(SEQUENCE mynamedschema.myseq)
                      )) PRIMARY KEY (col1)
                     )"}));
  }

  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);

  const Sequence* sequence = named_schema->FindSequence("mynamedschema.myseq");
  ASSERT_NE(sequence, nullptr);

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(),
                                      {R"(DROP TABLE mynamedschema.T)",
                                       R"(DROP SEQUENCE mynamedschema.myseq)"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        UpdateSchema(schema.get(), {R"(DROP TABLE mynamedschema.T)",
                                    R"(DROP SEQUENCE mynamedschema.myseq)"}));
  }

  named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);
  EXPECT_EQ(named_schema->FindSequence("mynamedschema.myseq"), nullptr);
}

TEST_P(SchemaUpdaterTest, SequenceWithNamedSchema_CrossSchemaSuccess) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema1)",
             R"(CREATE SCHEMA mynamedschema2)",
             R"(CREATE SEQUENCE mynamedschema1.myseq BIT_REVERSED_POSITIVE)",
             R"(CREATE TABLE mynamedschema2.T (
              col1 bigint DEFAULT nextval('mynamedschema1.myseq') PRIMARY KEY)
              )"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, CreateSchema({R"(CREATE SCHEMA mynamedschema1)",
                              R"(CREATE SCHEMA mynamedschema2)",
                              R"(CREATE SEQUENCE mynamedschema1.myseq OPTIONS (
            sequence_kind = "bit_reversed_positive"))",
                              R"(CREATE TABLE mynamedschema2.T (
            col1 INT64 DEFAULT (
              GET_NEXT_SEQUENCE_VALUE(SEQUENCE mynamedschema1.myseq)
              )) PRIMARY KEY (col1))"}));
  }
  const NamedSchema* named_schema_1 = schema->FindNamedSchema("mynamedschema1");
  ASSERT_NE(named_schema_1, nullptr);
  const NamedSchema* named_schema_2 = schema->FindNamedSchema("mynamedschema2");
  ASSERT_NE(named_schema_2, nullptr);
  ASSERT_NE(named_schema_2->FindTable("mynamedschema2.T"), nullptr);

  const Sequence* sequence =
      named_schema_1->FindSequence("mynamedschema1.myseq");
  ASSERT_NE(sequence, nullptr);
}

TEST_P(SchemaUpdaterTest, CreateSequenceWithNamedSchema_NoExistingSchemaFails) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        CreateSchema(
            {R"(CREATE SEQUENCE mynamedschema.myseq BIT_REVERSED_POSITIVE)"},
            /*proto_descriptor_bytes=*/"", /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::NamedSchemaNotFound("mynamedschema")));
  } else {
    EXPECT_THAT(CreateSchema({R"(CREATE SEQUENCE mynamedschema.myseq OPTIONS (
                        sequence_kind = "bit_reversed_positive"
                        ))"}),
                StatusIs(error::NamedSchemaNotFound("mynamedschema")));
  }
}

TEST_P(SchemaUpdaterTest, IndexWithNamedSchema_CreateAndDropSuccess) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(CREATE TABLE mynamedschema.T (col1 bigint primary key, col2 varchar))",
             R"(CREATE INDEX idx1 ON mynamedschema.T(col2))"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE SCHEMA mynamedschema)",
                                               R"(
      CREATE TABLE mynamedschema.T (
        col1 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY(col1)
    )",
                                               R"(
      CREATE INDEX mynamedschema.idx1 ON mynamedschema.T(col2)
    )"}));
  }

  const NamedSchema* named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);

  const Index* index = named_schema->FindIndex("mynamedschema.idx1");
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->Name(), "mynamedschema.idx1");

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, UpdateSchema(schema.get(), {"DROP INDEX mynamedschema.idx1"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, UpdateSchema(schema.get(), {"DROP INDEX mynamedschema.idx1"}));
  }

  named_schema = schema->FindNamedSchema("mynamedschema");
  ASSERT_NE(named_schema, nullptr);
  EXPECT_EQ(named_schema->FindIndex("mynamedschema.idx1"), nullptr);
}

TEST_P(SchemaUpdaterTest, IndexWithNamedSchema_CrossSchemaFails) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema1)",
             R"(CREATE SCHEMA mynamedschema2)",
             R"(CREATE TABLE mynamedschema1.T (col1 bigint primary key, col2 varchar))",
             R"(CREATE INDEX mynamedschema.idx1 ON mynamedschema1.T(col2))"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false),
        ::zetasql_base::testing::StatusIs(
            absl::StatusCode::kInvalidArgument,
            ::testing::HasSubstr("syntax error at or near \".\";")));

    EXPECT_THAT(
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(CREATE TABLE T (col1 bigint primary key, col2 varchar))",
             R"(CREATE INDEX mynamedschema.idx1 ON T(col2))"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false),
        ::zetasql_base::testing::StatusIs(
            absl::StatusCode::kInvalidArgument,
            ::testing::HasSubstr("syntax error at or near \".\";")));
  } else {
    EXPECT_THAT(
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema1)",
             R"(CREATE SCHEMA mynamedschema2)",
             R"(CREATE TABLE mynamedschema1.T (col1 INT64, col2 STRING(MAX))
                PRIMARY KEY(col1))",
             R"(CREATE INDEX mynamedschema2.idx1 ON mynamedschema1.T(col2))"}),
        StatusIs(error::IndexInDifferentSchema("mynamedschema2.idx1",
                                               "mynamedschema1.T")));

    EXPECT_THAT(
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(CREATE TABLE mynamedschema.T (col1 INT64, col2 STRING(MAX))
                PRIMARY KEY(col1))",
             R"(CREATE INDEX idx1 ON mynamedschema.T(col2))"}),
        StatusIs(error::IndexInDifferentSchema("idx1", "mynamedschema.T")));

    EXPECT_THAT(
        CreateSchema(
            {R"(CREATE SCHEMA mynamedschema)",
             R"(CREATE TABLE T (col1 INT64, col2 STRING(MAX)) PRIMARY KEY(col1))",
             R"(CREATE INDEX mynamedschema.idx1 ON T(col2))"}),
        StatusIs(error::IndexInDifferentSchema("mynamedschema.idx1", "T")));
  }
}

TEST_P(SchemaUpdaterTest, IndexWithNamedSchema_NoExistingSchemaFails) {
  // POSTGRESQL creates indexes in the named schemas of their tables.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
  CREATE TABLE T (
    col1 INT64,
    col2 STRING(MAX)
  ) PRIMARY KEY(col1)
  )"}));
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      CREATE INDEX mynamedschema.idx1 ON T(col2)
    )"}),
              StatusIs(error::NamedSchemaNotFound("mynamedschema")));
}

TEST_P(SchemaUpdaterTest, IndexWithNamedSchema_DuplicateIndexFails) {
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<const Schema> schema,
        CreateSchema(
            {
                R"(CREATE SCHEMA mynamedschema)",
                R"(CREATE TABLE mynamedschema.t (col1 bigint primary key, col2 varchar))",
                R"(CREATE INDEX idx1 ON mynamedschema.t (col2))",
            },
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
    EXPECT_THAT(UpdateSchema(schema.get(),
                             {"CREATE INDEX idx1 ON mynamedschema.t (col2)"}),
                StatusIs(error::SchemaObjectAlreadyExists(
                    "Index", "mynamedschema.idx1")));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema,
                         CreateSchema({R"(CREATE SCHEMA mynamedschema)",
                                       R"(
      CREATE TABLE mynamedschema.T (
        col1 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY(col1)
    )",
                                       R"(
      CREATE INDEX mynamedschema.idx1 ON mynamedschema.T(col2)
    )"}));
    EXPECT_THAT(
        UpdateSchema(
            schema.get(),
            {"CREATE INDEX mynamedschema.idx1 ON mynamedschema.T(col2)"}),
        StatusIs(
            error::SchemaObjectAlreadyExists("Index", "mynamedschema.idx1")));
  }
}

TEST_P(SchemaUpdaterTest, UDFWithNamedSchema_CreateAndDropSuccess) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE SCHEMA my_schema)",
          R"(CREATE FUNCTION my_schema.udf1() RETURNS INT64 SQL SECURITY INVOKER AS (1))",
      }));

  const NamedSchema* named_schema = schema->FindNamedSchema("my_schema");
  ASSERT_NE(named_schema, nullptr);

  const Udf* udf = named_schema->FindUdf("my_schema.udf1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "my_schema.udf1");

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE FUNCTION my_schema.udf1() RETURNS INT64 SQL SECURITY INVOKER AS (2))"}),
      StatusIs(error::SchemaObjectAlreadyExists("Function", "my_schema.udf1")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema, UpdateSchema(schema.get(), {"DROP FUNCTION my_schema.udf1"}));

  named_schema = schema->FindNamedSchema("my_schema");
  ASSERT_NE(named_schema, nullptr);
  EXPECT_EQ(named_schema->FindUdf("my_schema.udf1"), nullptr);
}

TEST_P(SchemaUpdaterTest, UDFWithNamedSchema_SameNameInDifferentNamedSchemas) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE SCHEMA schema1)",
          R"(CREATE SCHEMA schema2)",
          R"(CREATE FUNCTION schema1.udf1(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (x + 1))",
          R"(CREATE FUNCTION schema2.udf1(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (x + 2))",
      }));

  const NamedSchema* schema1 = schema->FindNamedSchema("schema1");
  ASSERT_NE(schema1, nullptr);
  const Udf* udf1_schema1 = schema1->FindUdf("schema1.udf1");
  ASSERT_NE(udf1_schema1, nullptr);
  ASSERT_EQ(udf1_schema1->Name(), "schema1.udf1");

  const NamedSchema* schema2 = schema->FindNamedSchema("schema2");
  ASSERT_NE(schema2, nullptr);
  const Udf* udf1_schema2 = schema2->FindUdf("schema2.udf1");
  ASSERT_NE(udf1_schema2, nullptr);
  ASSERT_EQ(udf1_schema2->Name(), "schema2.udf1");

  EXPECT_NE(udf1_schema1, udf1_schema2);
}

TEST_P(SchemaUpdaterTest, UDFWithNamedSchema_ReferencingUDFInNamedSchemas) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE SCHEMA schema1)", R"(CREATE SCHEMA schema2)",
           R"(CREATE FUNCTION schema2.udf2(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (x + 2))",
           R"(CREATE FUNCTION schema1.udf1(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (schema2.udf2(x) + 1))",
           R"(CREATE FUNCTION schema1.udf2(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (schema1.udf1(x) + 1))"}));

  const NamedSchema* schema1 = schema->FindNamedSchema("schema1");
  ASSERT_NE(schema1, nullptr);
  const Udf* udf1_schema1 = schema1->FindUdf("schema1.udf1");
  ASSERT_NE(udf1_schema1, nullptr);

  const NamedSchema* schema2 = schema->FindNamedSchema("schema2");
  ASSERT_NE(schema2, nullptr);
  const Udf* udf2_schema2 = schema2->FindUdf("schema2.udf2");
  ASSERT_NE(udf2_schema2, nullptr);

  EXPECT_EQ(udf1_schema1->dependencies().size(), 1);
  EXPECT_THAT(udf1_schema1->dependencies(),
              testing::UnorderedElementsAreArray(
                  std::vector<const SchemaNode*>{udf2_schema2}));

  const Udf* udf2_schema1 = schema1->FindUdf("schema1.udf2");
  ASSERT_NE(udf2_schema1, nullptr);
  EXPECT_EQ(udf2_schema1->dependencies().size(), 1);
  EXPECT_THAT(udf2_schema1->dependencies(),
              testing::UnorderedElementsAreArray(
                  std::vector<const SchemaNode*>{udf1_schema1}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(DROP FUNCTION schema2.udf2)"}),
              StatusIs(error::InvalidDropDependentFunction(
                  "UDF", "schema2.udf2", "schema1.udf1")));
}

TEST_P(SchemaUpdaterTest,
       UDFWithNamedSchema_ReferencingUDFWithoutSchemaQualification) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE SCHEMA my_schema)",
          R"(CREATE FUNCTION my_schema.udf1(x INT64) RETURNS INT64 SQL
            SECURITY INVOKER AS (x + 1))",
      }));

  const NamedSchema* my_schema = schema->FindNamedSchema("my_schema");
  ASSERT_NE(my_schema, nullptr);

  const Udf* udf1 = my_schema->FindUdf("my_schema.udf1");
  ASSERT_NE(udf1, nullptr);
  EXPECT_EQ(udf1->Name(), "my_schema.udf1");

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE FUNCTION my_schema.udf2(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (udf1(x) * 2))"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("Function not found: udf1")));
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
