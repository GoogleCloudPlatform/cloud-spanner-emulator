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

#include "backend/schema/catalog/sequence.h"

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// For the following tests, a custom PG DDL statement is required as translating
// expressions from GSQL to PG is not supported in tests.
using database_api::DatabaseDialect::GOOGLE_STANDARD_SQL;
using database_api::DatabaseDialect::POSTGRESQL;
using ::google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using testing::Optional;

class SequenceSchemaUpdaterTest : public SchemaUpdaterTest {
 public:
  SequenceSchemaUpdaterTest()
      : flag_setter_({.enable_identity_columns = true}) {}
  const ScopedEmulatorFeatureFlagsSetter flag_setter_;
};

INSTANTIATE_TEST_SUITE_P(
    SchemaUpdaterPerDialectTests, SequenceSchemaUpdaterTest,
    testing::Values(GOOGLE_STANDARD_SQL, POSTGRESQL),
    [](const testing::TestParamInfo<SequenceSchemaUpdaterTest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(SequenceSchemaUpdaterTest, SequenceNotSupportedWhenFlagIsOff) {
  if (GetParam() == GOOGLE_STANDARD_SQL) {
    GTEST_SKIP();
  }

  ScopedEmulatorFeatureFlagsSetter setter(
      {.enable_bit_reversed_positive_sequences_postgresql = false});

  EXPECT_THAT(CreateSchema({R"(
      CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
      )"},
                           /*proto_descriptor_bytes=*/"",
                           /*dialect=*/POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false),
              StatusIs(error::SequenceNotSupportedInPostgreSQL()));

  EXPECT_THAT(CreateSchema({R"(
      ALTER SEQUENCE myseq RESTART COUNTER 1
      )"},
                           /*proto_descriptor_bytes=*/"",
                           /*dialect=*/POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false),
              StatusIs(error::SequenceNotSupportedInPostgreSQL()));

  EXPECT_THAT(CreateSchema({R"(
      DROP SEQUENCE myseq
      )"},
                           /*proto_descriptor_bytes=*/"",
                           /*dialect=*/POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false),
              StatusIs(error::SequenceNotSupportedInPostgreSQL()));
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_Basic) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);

  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(sequence->start_with_counter().value(), 1);
    EXPECT_EQ(sequence->DebugString(),
              R"(Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE
  start_with_counter: 1)");
  } else {
    EXPECT_FALSE(sequence->start_with_counter().has_value());
    EXPECT_EQ(sequence->DebugString(),
              "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE");
  }
  EXPECT_FALSE(sequence->skip_range_min().has_value());
  EXPECT_FALSE(sequence->skip_range_max().has_value());
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_BasicWithDefaultValue) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint DEFAULT nextval('myseq') PRIMARY KEY,
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                 }));
  }
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(sequence->start_with_counter().value(), 1);
  } else {
    EXPECT_FALSE(sequence->start_with_counter().has_value());
  }
  EXPECT_FALSE(sequence->skip_range_min().has_value());
  EXPECT_FALSE(sequence->skip_range_max().has_value());
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_AllOptions) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
            SKIP RANGE 1 1000 START COUNTER 5000;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 5000,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter().value(), 5000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);

  EXPECT_EQ(sequence->DebugString(),
            R"(Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE
  start_with_counter: 5000
  skipped range: [1, 1000])");
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_NullOptions) {
  // PostgreSQL doesn't accept NULL in sequence SQL clause.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE SEQUENCE myseq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_min = NULL,
        skip_range_max = NULL,
        start_with_counter = NULL
      )
    )"}));
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_FALSE(sequence->start_with_counter().has_value());
  EXPECT_FALSE(sequence->skip_range_min().has_value());
  EXPECT_FALSE(sequence->skip_range_max().has_value());
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_OneSkipRangeValueSet) {
  // PostgreSQL doesn't accept NULL or only 1 value in the `SKIP RANGE` clause.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(CreateSchema({R"(
      CREATE SEQUENCE myseq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_min = NULL,
        skip_range_max = 1000
      )
    )"}),
              StatusIs(error::SequenceSkipRangeMinMaxNotSetTogether()));

  EXPECT_THAT(CreateSchema({R"(
      CREATE SEQUENCE myseq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_min = 1,
        skip_range_max = NULL
      )
    )"}),
              StatusIs(error::SequenceSkipRangeMinMaxNotSetTogether()));

  EXPECT_THAT(CreateSchema({R"(
      CREATE SEQUENCE myseq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_max = 1000
      )
    )"}),
              StatusIs(error::SequenceSkipRangeMinMaxNotSetTogether()));

  EXPECT_THAT(CreateSchema({R"(
      CREATE SEQUENCE myseq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_min = 1
      )
    )"}),
              StatusIs(error::SequenceSkipRangeMinMaxNotSetTogether()));
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_NegativeStartWithCounter) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
        CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE START COUNTER -1
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::InvalidSequenceStartWithCounterValue()));
  } else {
    EXPECT_THAT(CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = -1
        )
      )"}),
                StatusIs(error::InvalidSequenceStartWithCounterValue()));
  }
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_NegativeSkippedRange) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        CreateSchema({R"(
        CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE SKIP RANGE -100 -1
        )"},
                     /*proto_descriptor_bytes=*/"",
                     /*dialect=*/POSTGRESQL,
                     /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::SequenceSkippedRangeHasAtleastOnePositiveNumber()));
  } else {
    EXPECT_THAT(
        CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          skip_range_min = -100,
          skip_range_max = -1
        )
      )"}),
        StatusIs(error::SequenceSkippedRangeHasAtleastOnePositiveNumber()));
  }
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_InvaliSkippedRange) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
        CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE SKIP RANGE 100 1
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::SequenceSkipRangeMinLargerThanMax()));
  } else {
    EXPECT_THAT(CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          skip_range_min = 100,
          skip_range_max = 1
        )
      )"}),
                StatusIs(error::SequenceSkipRangeMinLargerThanMax()));
  }
}

TEST_P(SequenceSchemaUpdaterTest, CreateSequence_DuplicateSequenceGivesError) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  // Create a sequence with the same name. Expect error.
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        UpdateSchema(schema.get(), {R"(
        CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
          SKIP RANGE 1 1000 START COUNTER 500
        )"},
                     /*proto_descriptor_bytes=*/"",
                     /*dialect=*/POSTGRESQL,
                     /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::SchemaObjectAlreadyExists("Sequence", "myseq")));
  } else {
    EXPECT_THAT(
        UpdateSchema(schema.get(), {R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 500,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}),
        StatusIs(error::SchemaObjectAlreadyExists("Sequence", "myseq")));
  }
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(sequence->start_with_counter().value(), 1);
  } else {
    EXPECT_FALSE(sequence->start_with_counter().has_value());
  }
  EXPECT_FALSE(sequence->skip_range_min().has_value());
  EXPECT_FALSE(sequence->skip_range_max().has_value());
}

TEST_P(SequenceSchemaUpdaterTest,
       CreateSequence_DuplicateSequenceWithIfNotExists) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }
  const Sequence* sequence = schema->FindSequence("myseq");
  std::string sequence_id = sequence->id();

  // Create a sequence with the same name, but we use the IF NOT EXISTS clause
  // here, so the statement succeeds. But the new sequence should not overwrite
  // the existing one.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        CREATE SEQUENCE IF NOT EXISTS myseq BIT_REVERSED_POSITIVE
          SKIP RANGE 1 1000 START COUNTER 500
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        CREATE SEQUENCE IF NOT EXISTS myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 500,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }
  // Indicate that the DDL statement is a no-op.
  EXPECT_EQ(schema, nullptr);
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_AlterNonExistsSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE nonexist SKIP RANGE 1 1000
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::SequenceNotFound("nonexist")));
  } else {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE nonexist SET OPTIONS (
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}),
                StatusIs(error::SequenceNotFound("nonexist")));
  }
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_WithIfExists) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE IF EXISTS nonexist SKIP RANGE 1 1000
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE IF EXISTS nonexist SET OPTIONS (
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_SetAllOptions) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
          ALTER SEQUENCE myseq SKIP RANGE 1 1000 RESTART COUNTER 5000;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 5000,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }

  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 5000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);
  EXPECT_EQ(sequence->DebugString(),
            "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 5000\n  skipped range: [1, 1000]");
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_ChangeStartWithCounter) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(sequence->start_with_counter().value(), 1);
  } else {
    EXPECT_FALSE(sequence->start_with_counter().has_value());
  }
  EXPECT_FALSE(sequence->skip_range_min().has_value());
  EXPECT_FALSE(sequence->skip_range_max().has_value());

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
          ALTER SEQUENCE myseq RESTART COUNTER 5000;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          start_with_counter = 5000
        )
      )"}));
  }

  sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 5000);
  EXPECT_FALSE(sequence->skip_range_min().has_value());
  EXPECT_FALSE(sequence->skip_range_max().has_value());
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_ChangeAllOptions) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
            SKIP RANGE 1 1000 START COUNTER 2000 ;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 2000,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }

  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 2000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
          ALTER SEQUENCE myseq SKIP RANGE 1000 10000 RESTART COUNTER 5000;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          start_with_counter = 5000,
          skip_range_min = 1000,
          skip_range_max = 10000
        )
      )"}));
  }

  sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 5000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1000);
  EXPECT_EQ(sequence->skip_range_max().value(), 10000);
}

TEST_P(SequenceSchemaUpdaterTest,
       AlterSequence_SetAllOptionsChangeStartWithCounter) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
            SKIP RANGE 1 1000 START COUNTER 2000 ;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 2000,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }

  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 2000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
          ALTER SEQUENCE myseq RESTART COUNTER 3456;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          start_with_counter = 3456
        )
      )"}));
  }

  sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 3456);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_ClearAllOptions) {
  // PostgreSQL doesn't accept NULL in the `SKIP RANGE` clause.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
            SKIP RANGE 1 1000 START COUNTER 2000 ;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 2000,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }

  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 2000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq SET OPTIONS (
        skip_range_min = NULL,
        skip_range_max = NULL,
        start_with_counter = NULL
      )
    )"}));
  sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_FALSE(sequence->start_with_counter().has_value());
  EXPECT_FALSE(sequence->skip_range_min().has_value());
  EXPECT_FALSE(sequence->skip_range_max().has_value());
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_ChangeOneSkipRangeValue) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
            SKIP RANGE 1 1000 START COUNTER 2000 ;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 2000,
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )"}));
  }

  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 2000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
          ALTER SEQUENCE myseq SKIP RANGE 1 10000;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          skip_range_max = 10000
        )
      )"}));
  }

  sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->start_with_counter(), 2000);
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 10000);
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_OneSkipRangeValueSetToNull) {
  // PostgreSQL doesn't accept NULL or only 1 value in the `SKIP RANGE` clause.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
          CREATE SEQUENCE myseq OPTIONS (
            sequence_kind = "bit_reversed_positive",
            skip_range_min = 1,
            skip_range_max = 1000
          )
        )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq SET OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_min = NULL,
        skip_range_max = 1000
      )
    )"}),
              StatusIs(error::SequenceSkipRangeMinMaxNotSetTogether()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq SET OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_min = 1,
        skip_range_max = NULL
      )
    )"}),
              StatusIs(error::SequenceSkipRangeMinMaxNotSetTogether()));
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_NegativeStartWithCounter) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq RESTART COUNTER -1
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::InvalidSequenceStartWithCounterValue()));
  } else {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          start_with_counter = -1
        )
      )"}),
                StatusIs(error::InvalidSequenceStartWithCounterValue()));
  }
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_NegativeSkippedRange) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SKIP RANGE -100 -1
        )"},
                     /*proto_descriptor_bytes=*/"",
                     /*dialect=*/POSTGRESQL,
                     /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::SequenceSkippedRangeHasAtleastOnePositiveNumber()));
  } else {
    EXPECT_THAT(
        UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          skip_range_min = -100,
          skip_range_max = -1
        )
      )"}),
        StatusIs(error::SequenceSkippedRangeHasAtleastOnePositiveNumber()));
  }
}

TEST_P(SequenceSchemaUpdaterTest, AlterSequence_InvaliSkippedRange) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SKIP RANGE 100 1
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::SequenceSkipRangeMinLargerThanMax()));
  } else {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          skip_range_min = 100,
          skip_range_max = 1
        )
      )"}),
                StatusIs(error::SequenceSkipRangeMinLargerThanMax()));
  }
}

TEST_P(SequenceSchemaUpdaterTest, DropSequence_Basic) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq
      )"}));
  }

  EXPECT_EQ(schema->FindSequence("myseq"), nullptr);
}

TEST_P(SequenceSchemaUpdaterTest, DropSequence_DropAndCreateAgain) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq
      )"}));
  }

  EXPECT_EQ(schema->FindSequence("myseq"), nullptr);

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  EXPECT_NE(schema->FindSequence("myseq"), nullptr);
}

TEST_P(SequenceSchemaUpdaterTest, DropSequence_DropNonExistSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE notmyseq
    )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::SequenceNotFound("notmyseq")));
  } else {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE notmyseq
      )"}),
                StatusIs(error::SequenceNotFound("notmyseq")));
  }

  EXPECT_NE(schema->FindSequence("myseq"), nullptr);
}

TEST_P(SequenceSchemaUpdaterTest,
       DropSequence_DropNonExistSequenceWithIfExists) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE IF NOT EXISTS myseq BIT_REVERSED_POSITIVE;
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE IF EXISTS notmyseq
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE IF EXISTS notmyseq
      )"}));
  }

  EXPECT_NE(schema->FindSequence("myseq"), nullptr);
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_CreateAndDropColumn) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            id bigint PRIMARY KEY,
            int64_col bigint DEFAULT nextval('myseq'),
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          id INT64,
          int64_col INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          value INT64
        ) PRIMARY KEY (id)
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const Column* int64_col =
      schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(int64_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  // Try to drop the sequence, receive error because a column is using it.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
              StatusIs(error::InvalidDropDependentColumn(
                  "SEQUENCE", "myseq", "test_table.int64_col")));

  // Drop the column, dependencies are dropped accordingly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table DROP COLUMN int64_col
      )"}));

  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_EQ(int64_col, nullptr);

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest,
       SequenceDependency_AlterSequenceDoesNotAffectColumnDependency) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            id bigint PRIMARY KEY,
            int64_col bigint DEFAULT nextval('myseq'),
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          id INT64,
          int64_col INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          value INT64
        ) PRIMARY KEY (id)
      )",
                                 }));
  }

  const Column* int64_col =
      schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(int64_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  // Alter the sequence
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq RESTART COUNTER 100
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          start_with_counter = 100
        )
    )"}));
  }

  // Try to drop the sequence, receive error because column int64_col is using
  // it.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
              StatusIs(error::InvalidDropDependentColumn(
                  "SEQUENCE", "myseq", "test_table.int64_col")));

  // Drop the column default value, dependencies are dropped accordingly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col DROP DEFAULT
      )"}));

  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_TRUE(int64_col->sequences_used().empty());

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest,
       SequenceDependency_CreateAndDropColumnDefaultValue) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint DEFAULT 1 + 10 +nextval('myseq') PRIMARY KEY,
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64 DEFAULT
              (1 + 10 + GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const Column* int64_col =
      schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(int64_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  // Try to drop the sequence, receive error because a column is using it.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
              StatusIs(error::InvalidDropDependentColumn(
                  "SEQUENCE", "myseq", "test_table.int64_col")));

  // Drop the column default value, dependencies are dropped accordingly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col DROP DEFAULT
      )"}));

  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_TRUE(int64_col->sequences_used().empty());

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_TwoColumnDefaultValues) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint DEFAULT nextval('myseq') PRIMARY KEY,
            second_col bigint DEFAULT nextval('myseq'),
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          second_col INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const Column* int64_col =
      schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(int64_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  const Column* second_col =
      schema->FindTable("test_table")->FindColumn("second_col");
  EXPECT_NE(second_col, nullptr);
  EXPECT_THAT(second_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  // Try to drop the sequence, receive error because a column is using it.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
              StatusIs(error::InvalidDropDependentColumn(
                  "SEQUENCE", "myseq", "test_table.int64_col")));

  // Drop one column default value, only one dependency is dropped.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col DROP DEFAULT
      )"}));

  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_TRUE(int64_col->sequences_used().empty());

  second_col = schema->FindTable("test_table")->FindColumn("second_col");
  EXPECT_NE(second_col, nullptr);
  EXPECT_THAT(second_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  // Drop the other column default value, now there is no dependency left.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN second_col DROP DEFAULT
      )"}));

  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_TRUE(int64_col->sequences_used().empty());

  second_col = schema->FindTable("test_table")->FindColumn("second_col");
  EXPECT_NE(second_col, nullptr);
  EXPECT_TRUE(second_col->sequences_used().empty());

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest,
       SequenceDependency_TwoSequencesInOneColumnDefaultValue) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE SEQUENCE myseq2 BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint DEFAULT
              nextval('myseq') + nextval('myseq2') PRIMARY KEY,
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE SEQUENCE myseq2 OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64 DEFAULT
            (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq) +
             GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq2)),
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const Column* int64_col =
      schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(
      int64_col->sequences_used(),
      testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
          schema->FindSequence("myseq"), schema->FindSequence("myseq2")}));

  // Try to drop the sequences, receive error because a column is using it.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
              StatusIs(error::InvalidDropDependentColumn(
                  "SEQUENCE", "myseq", "test_table.int64_col")));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq2
    )"}),
              StatusIs(error::InvalidDropDependentColumn(
                  "SEQUENCE", "myseq2", "test_table.int64_col")));

  // Drop the column default value, corresponding dependencies are now dropped.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col DROP DEFAULT
      )"}));

  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_TRUE(int64_col->sequences_used().empty());

  // Now we can drop the sequences.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq2)"}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_AlterColumnToUseSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint PRIMARY KEY,
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64,
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                 }));
  }

  // There is no dependency registered
  const Column* int64_col =
      schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_TRUE(int64_col->sequences_used().empty());

  // Alter column to set default value that uses the sequence
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col SET DEFAULT
          nextval('myseq')
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col SET DEFAULT
            (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq))
    )"}));
  }

  // Ensure dependencies are set up correctly
  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(int64_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));
}

TEST_P(SequenceSchemaUpdaterTest,
       SequenceDependency_AlterColumnToAddMoreSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE SEQUENCE myseq2 BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint PRIMARY KEY DEFAULT nextval('myseq'),
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE SEQUENCE myseq2 OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64 DEFAULT (
            GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                 }));
  }

  // `int64_col` has one dependency on `myseq`
  const Column* int64_col =
      schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(int64_col->sequences_used(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  // Alter column to add one more dependency on myseq2.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col SET DEFAULT
          nextval('myseq') + nextval('myseq2')
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER TABLE test_table ALTER COLUMN int64_col SET DEFAULT
            (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq) +
             GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq2))
    )"}));
  }

  // `int64_col` now has two dependencies on `myseq` and `myseq2`.
  int64_col = schema->FindTable("test_table")->FindColumn("int64_col");
  EXPECT_NE(int64_col, nullptr);
  EXPECT_THAT(
      int64_col->sequences_used(),
      testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
          schema->FindSequence("myseq"), schema->FindSequence("myseq2")}));
}

TEST_P(SequenceSchemaUpdaterTest,
       SequenceDependency_NewColumnUsesNonExistSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
          CREATE TABLE test_table (
            int64_col bigint DEFAULT nextval('nonexist') PRIMARY KEY,
            value bigint
          )
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Sequence not found: nonexist")));
  } else {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
          CREATE TABLE test_table (
            int64_col INT64 DEFAULT
                (GET_NEXT_SEQUENCE_VALUE(SEQUENCE nonexist)),
            value INT64
          ) PRIMARY KEY (int64_col)
      )"}),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Sequence not found: nonexist")));
  }
}

TEST_P(SequenceSchemaUpdaterTest,
       SequenceDependency_AlterColumnUsesNonExistSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint DEFAULT nextval('myseq') PRIMARY KEY,
            second_col bigint,
            value bigint
          )
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          second_col INT64,
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                 }));
  }

  // Alter column SET DEFAULT should fail:
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
          ALTER TABLE test_table ALTER COLUMN second_col SET DEFAULT
              nextval('nonexist')
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Sequence not found: nonexist")));
  } else {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
          ALTER TABLE test_table ALTER COLUMN second_col SET DEFAULT
              (GET_NEXT_SEQUENCE_VALUE(SEQUENCE nonexist))
      )"}),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Sequence not found: nonexist")));
  }

  // Alter column set the whole column definition should fail. This syntax is
  // only in ZetaSQL.
  if (GetParam() == GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
          ALTER TABLE test_table ALTER COLUMN second_col INT64 DEFAULT
              (GET_NEXT_SEQUENCE_VALUE(SEQUENCE nonexist))
      )"}),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Sequence not found: nonexist")));
  }
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_CreateAndDropView) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE VIEW myview SQL SECURITY INVOKER AS
            SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
          CREATE VIEW myview SQL SECURITY INVOKER AS
            SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const View* myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_THAT(myview->dependencies(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));

  // Try to drop the sequence, receive error because a view is using it.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot drop SEQUENCE `myseq` on which there "
                             "are dependent views")));

  // Drop the view, dependencies are dropped accordingly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        DROP VIEW myview
      )"}));

  EXPECT_EQ(schema->FindView("myview"), nullptr);

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_CreateViewQueryTable) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE TABLE test_table (
            int64_col bigint PRIMARY KEY,
            value bigint
          )
        )",
                                       R"(
          CREATE VIEW myview SQL SECURITY INVOKER AS
            SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state,
                   t.int64_col AS col FROM test_table t
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE TABLE test_table (
          int64_col INT64,
          value INT64
        ) PRIMARY KEY (int64_col)
      )",
                                     R"(
        CREATE VIEW myview SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state,
                 t.int64_col AS col FROM test_table t
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const View* myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_THAT(
      myview->dependencies(),
      testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
          schema->FindSequence("myseq"), schema->FindTable("test_table"),
          schema->FindTable("test_table")->FindColumn("int64_col")}));

  // Try to drop the sequence, receive error because a view is using it.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot drop SEQUENCE `myseq` on which there "
                             "are dependent views")));

  // Drop the view, dependencies are dropped accordingly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        DROP VIEW myview
      )"}));

  EXPECT_EQ(schema->FindView("myview"), nullptr);

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_CreateAndReplaceView) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )",
                                       R"(
          CREATE VIEW myview SQL SECURITY INVOKER AS
            SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE VIEW myview SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);

  const View* myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_THAT(myview->dependencies(),
              testing::UnorderedElementsAreArray(
                  (std::vector<const SchemaNode*>{sequence})));

  // Try to drop the sequence, receive error because a column is using it.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot drop SEQUENCE `myseq` on which there "
                             "are dependent views")));

  // Replace the view to not use sequence, dependencies are dropped accordingly.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
    )"}));
  }

  sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);

  myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_TRUE(myview->dependencies().empty());

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest,
       SequenceDependency_AlterSequenceDoesNotAffectViewDependency) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )",
                                       R"(
          CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
            SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const View* myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_THAT(
      myview->dependencies(),
      testing::UnorderedElementsAreArray(
          (std::vector<const SchemaNode*>{schema->FindSequence("myseq")})));

  // Alter the sequence
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq RESTART COUNTER 100
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        ALTER SEQUENCE myseq SET OPTIONS (
          start_with_counter = 100
          )
    )"}));
  }

  // Try to drop the sequence, receive error because a view is using it.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot drop SEQUENCE `myseq` on which there "
                             "are dependent views")));

  // Replace the view to not use sequence, dependencies are dropped accordingly.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
    )"}));
  }

  myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_TRUE(myview->dependencies().empty());

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_TwoViewsUseOneSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state
        )",
                                       R"(
          CREATE OR REPLACE VIEW myview2 SQL SECURITY INVOKER AS
          SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
          CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state
        )",
                                     R"(
          CREATE OR REPLACE VIEW myview2 SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state
        )"}));
  }
  // Ensure dependencies are set up correctly
  const View* myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_THAT(
      myview->dependencies(),
      testing::UnorderedElementsAreArray(
          (std::vector<const SchemaNode*>{schema->FindSequence("myseq")})));

  const View* myview2 = schema->FindView("myview2");
  EXPECT_NE(myview2, nullptr);
  EXPECT_THAT(
      myview2->dependencies(),
      testing::UnorderedElementsAreArray(
          (std::vector<const SchemaNode*>{schema->FindSequence("myseq")})));

  // Try to drop the sequence, receive error because a view is using it.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot drop SEQUENCE `myseq` on which there "
                             "are dependent views")));

  // Replace one view to not use sequence, one dependency is dropped.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
    )"}));
  }

  myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_TRUE(myview->dependencies().empty());

  myview2 = schema->FindView("myview2");
  EXPECT_NE(myview2, nullptr);
  EXPECT_THAT(
      myview2->dependencies(),
      testing::UnorderedElementsAreArray(
          (std::vector<const SchemaNode*>{schema->FindSequence("myseq")})));

  // Drop the other view, dependencies are dropped accordingly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        DROP VIEW myview2
      )"}));

  EXPECT_EQ(schema->FindView("myview2"), nullptr);

  // Now we can drop the sequence.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_TwoSequencesInOneView) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE SEQUENCE myseq2 BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
            SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state,
                   spanner.get_internal_sequence_state('myseq2') AS myseq2_state
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE SEQUENCE myseq2 OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state,
                 get_internal_sequence_state(SEQUENCE myseq2) AS myseq2_state
      )",
                                 }));
  }
  // Ensure dependencies are set up correctly
  const View* myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_THAT(
      myview->dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const SchemaNode*>{
          schema->FindSequence("myseq"), schema->FindSequence("myseq2")})));

  // Try to drop the sequences, receive error.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot drop SEQUENCE `myseq` on which there "
                             "are dependent views")));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP SEQUENCE myseq2
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot drop SEQUENCE `myseq2` on which there "
                             "are dependent views")));

  // Replace the view to not use sequences, corresponding dependencies are now
  // dropped.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
    )"}));
  }

  myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_TRUE(myview->dependencies().empty());

  // Now we can drop the sequences.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq)"}));
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        DROP SEQUENCE myseq2)"}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_ReplaceViewToUseSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE;
        )",
                                       R"(
          CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                                     R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS SELECT 1 AS one
      )",
                                 }));
  }

  // There is no dependency registered
  const View* myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_TRUE(myview->dependencies().empty());

  // Replace view to use the sequence
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT spanner.get_internal_sequence_state('myseq') AS myseq_state
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE myseq) AS myseq_state,
    )"}));
  }

  // Ensure dependencies are set up correctly
  myview = schema->FindView("myview");
  EXPECT_NE(myview, nullptr);
  EXPECT_THAT(myview->dependencies(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  schema->FindSequence("myseq")}));
}

TEST_P(SequenceSchemaUpdaterTest, SequenceDependency_ViewUsesNonExistSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
          CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        )"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )"}));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT spanner.get_internal_sequence_state('nonexist') AS myseq_state
        )"},
                             /*proto_descriptor_bytes=*/"",
                             /*dialect=*/POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Sequence not found: nonexist")));
  } else {
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE VIEW myview SQL SECURITY INVOKER AS
          SELECT get_internal_sequence_state(SEQUENCE nonexist) AS myseq_state
      )"}),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Sequence not found: nonexist")));
  }
}

TEST_P(SequenceSchemaUpdaterTest, GSQLSequenceClauseNotSupportedWhenFlagIsOff) {
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP();
  }

  ScopedEmulatorFeatureFlagsSetter setter({.enable_identity_columns = false});

  EXPECT_THAT(CreateSchema({R"(
      CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
      )"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(
                      "Using SQL clauses to configure sequence options is not "
                      "supported in CREATE SEQUENCE statements")));

  EXPECT_THAT(CreateSchema({R"(
      ALTER SEQUENCE myseq RESTART COUNTER WITH 1
      )"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("RESTART COUNTER WITH is not supported in "
                                     "ALTER SEQUENCE statements")));
}

TEST_P(SequenceSchemaUpdaterTest, GSQLSequenceClause_CreateSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        SKIP RANGE 1000, 2000 START COUNTER WITH 100
    )"}));
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);

  EXPECT_TRUE(sequence->start_with_counter().has_value());
  EXPECT_EQ(sequence->DebugString(),
            "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 100\n  skipped range: [1000, 2000]");
  EXPECT_THAT(sequence->start_with_counter(), Optional(100));
  EXPECT_THAT(sequence->skip_range_min(), Optional(1000));
  EXPECT_THAT(sequence->skip_range_max(), Optional(2000));
}

TEST_P(SequenceSchemaUpdaterTest,
       GSQLSequenceClause_SpecifyNoSequenceKindFailed) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP();
  }
  EXPECT_THAT(CreateSchema({"CREATE SEQUENCE myseq"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(
                      "The sequence does not have a valid sequence kind. "
                      "Please specify the sequence kind explicitly or set "
                      "the database option `default_sequence_kind`.")));
}

TEST_P(SequenceSchemaUpdaterTest,
       GSQLSequenceClause_SpecifyNoSequenceKindSuccess) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                   R"(ALTER DATABASE db SET OPTIONS (
        default_sequence_kind = 'bit_reversed_positive'))",
                                   R"(
      CREATE SEQUENCE myseq
        SKIP RANGE 1000, 2000 START COUNTER WITH 100
    )",
                                   R"(
      CREATE SEQUENCE myseq2 OPTIONS (
          start_with_counter = 200,
          skip_range_min = 3000,
          skip_range_max = 4000
        )
    )",
                                   R"(
      CREATE SEQUENCE myseq3
    )"}));
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->sequence_kind_name(), "BIT_REVERSED_POSITIVE");
  EXPECT_TRUE(sequence->start_with_counter().has_value());
  EXPECT_EQ(sequence->DebugString(),
            "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 100\n  skipped range: [1000, 2000]");
  EXPECT_THAT(sequence->start_with_counter(), Optional(100));
  EXPECT_THAT(sequence->skip_range_min(), Optional(1000));
  EXPECT_THAT(sequence->skip_range_max(), Optional(2000));

  const Sequence* sequence2 = schema->FindSequence("myseq2");
  EXPECT_NE(sequence2, nullptr);
  EXPECT_EQ(sequence2->sequence_kind_name(), "BIT_REVERSED_POSITIVE");
  EXPECT_TRUE(sequence2->start_with_counter().has_value());
  EXPECT_EQ(sequence2->DebugString(),
            "Sequence myseq2. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 200\n  skipped range: [3000, 4000]");
  EXPECT_THAT(sequence2->start_with_counter(), Optional(200));
  EXPECT_THAT(sequence2->skip_range_min(), Optional(3000));
  EXPECT_THAT(sequence2->skip_range_max(), Optional(4000));

  const Sequence* sequence3 = schema->FindSequence("myseq3");
  EXPECT_NE(sequence3, nullptr);
  EXPECT_EQ(sequence3->sequence_kind_name(), "BIT_REVERSED_POSITIVE");
  EXPECT_EQ(sequence3->DebugString(),
            "Sequence myseq3. Sequence kind: BIT_REVERSED_POSITIVE");
  EXPECT_FALSE(sequence3->start_with_counter().has_value());
  EXPECT_FALSE(sequence3->skip_range_min().has_value());
  EXPECT_FALSE(sequence3->skip_range_max().has_value());
}

TEST_P(SequenceSchemaUpdaterTest,
       GSQLSequenceClause_UseBothClausesAndOptions_CreateFailed) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP();
  }
  EXPECT_THAT(
      CreateSchema({R"(
            CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
              OPTIONS (sequence_kind = "bit_reversed_positive")
          )"}),
      StatusIs(error::CannotSetSequenceClauseAndOptionTogether("myseq")));

  EXPECT_THAT(
      CreateSchema({R"(
            CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
              OPTIONS (start_with_counter = 5000)
          )"}),
      StatusIs(error::CannotSetSequenceClauseAndOptionTogether("myseq")));
}

TEST_P(SequenceSchemaUpdaterTest,
       GSQLSequenceClause_UseBothClausesAndOptions_AlterFailed) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP();
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
            CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
          )",
                                             R"(
            CREATE SEQUENCE myseq2
              OPTIONS (sequence_kind = "bit_reversed_positive")
          )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq SET OPTIONS (start_with_counter = 5000)
  )"}),
      StatusIs(error::CannotSetSequenceClauseAndOptionTogether("myseq")));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq2 RESTART COUNTER WITH 5000
  )"}),
      StatusIs(error::CannotSetSequenceClauseAndOptionTogether("myseq2")));
}

TEST_P(SequenceSchemaUpdaterTest, GSQLSequenceClause_AlterSequence) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE
        SKIP RANGE 1000, 2000 START COUNTER WITH 100
    )"}));
  const Sequence* sequence = schema->FindSequence("myseq");
  EXPECT_NE(sequence, nullptr);
  EXPECT_EQ(sequence->DebugString(),
            "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 100\n  skipped range: [1000, 2000]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq SKIP RANGE 3000, 4000
    )"}));
  sequence = schema->FindSequence("myseq");
  EXPECT_EQ(sequence->DebugString(),
            "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 100\n  skipped range: [3000, 4000]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq RESTART COUNTER WITH 200
    )"}));
  sequence = schema->FindSequence("myseq");
  EXPECT_EQ(sequence->DebugString(),
            "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 200\n  skipped range: [3000, 4000]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER SEQUENCE myseq NO SKIP RANGE
    )"}));
  sequence = schema->FindSequence("myseq");
  EXPECT_EQ(sequence->DebugString(),
            "Sequence myseq. Sequence kind: BIT_REVERSED_POSITIVE\n  "
            "start_with_counter: 200");
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
