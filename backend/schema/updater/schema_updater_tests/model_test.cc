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



#include "backend/schema/catalog/model.h"

#include <optional>
#include <string>

#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {
namespace {

using database_api::DatabaseDialect::POSTGRESQL;
using ::google::spanner::emulator::test::EqualsProto;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Optional;
using ::testing::SizeIs;
using ::zetasql_base::testing::StatusIs;

TEST_P(SchemaUpdaterTest, CreateModel_Basic) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE MODEL m
      INPUT ( feature INT64 )
      OUTPUT ( label STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      )
    )"}));
  EXPECT_THAT(
      schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
            }
            remote: true
            input { column_name: "feature" type: INT64 }
            output { column_name: "label" type: STRING }
          }
        }
      )pb"));

  const Model* model = schema->FindModel("m");
  EXPECT_NE(model, nullptr);
  EXPECT_EQ(model->Name(), "m");
  ASSERT_THAT(model->input(), SizeIs(1));
  EXPECT_EQ(model->input()[0].name, "feature");
  EXPECT_EQ(model->input()[0].type, zetasql::types::Int64Type());
  EXPECT_EQ(model->input()[0].is_required, std::nullopt);
  ASSERT_THAT(model->output(), SizeIs(1));
  EXPECT_EQ(model->output()[0].name, "label");
  EXPECT_EQ(model->output()[0].type, zetasql::types::StringType());
  EXPECT_EQ(model->output()[0].is_required, std::nullopt);
  EXPECT_EQ(model->is_remote(), true);
  EXPECT_THAT(model->endpoint(),
              Optional(std::string("//aiplatform.googleapis.com/projects/aaa/"
                                   "locations/bbb/endpoints/ccc")));
  EXPECT_THAT(model->endpoints(), IsEmpty());
  EXPECT_EQ(model->default_batch_size(), std::nullopt);
}

TEST_P(SchemaUpdaterTest, CreateModel_AllOptions) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE MODEL m
      INPUT ( feature ARRAY<INT64> OPTIONS (required = false))
      OUTPUT ( label STRUCT<l STRING(MAX)> OPTIONS (required = true))
      REMOTE OPTIONS (
        endpoints = [
         '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc',
         '//aiplatform.googleapis.com/projects/aaa/locations/ddd/endpoints/eee'
        ],
        default_batch_size = 10
      )
    )"}));
  EXPECT_THAT(
      schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoints"
              string_list_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
              string_list_value: "//aiplatform.googleapis.com/projects/aaa/locations/ddd/endpoints/eee"
            }
            set_options { option_name: "default_batch_size" int64_value: 10 }
            remote: true
            input {
              column_name: "feature"
              type: ARRAY
              set_options { option_name: "required" bool_value: false }
              array_subtype { type: INT64 }
            }
            output {
              column_name: "label"
              type: STRUCT
              set_options { option_name: "required" bool_value: true }
              type_definition {
                type: STRUCT
                struct_descriptor {
                  field {
                    name: "l"
                    type { type: STRING }
                  }
                }
              }
            }
          }
        }
      )pb"));

  const Model* model = schema->FindModel("m");
  EXPECT_NE(model, nullptr);
  EXPECT_EQ(model->Name(), "m");
  ASSERT_THAT(model->input(), SizeIs(1));
  EXPECT_EQ(model->input()[0].name, "feature");
  ASSERT_TRUE(model->input()[0].type->IsArray());
  EXPECT_EQ(model->input()[0].type->AsArray()->element_type(),
            zetasql::types::Int64Type());
  EXPECT_THAT(model->input()[0].is_required, Optional(false));
  ASSERT_THAT(model->output(), SizeIs(1));
  EXPECT_EQ(model->output()[0].name, "label");
  ASSERT_TRUE(model->output()[0].type->IsStruct());
  ASSERT_THAT(model->output()[0].type->AsStruct()->fields(), SizeIs(1));
  EXPECT_EQ(model->output()[0].type->AsStruct()->field(0).name, "l");
  EXPECT_EQ(model->output()[0].type->AsStruct()->field(0).type,
            zetasql::types::StringType());
  EXPECT_THAT(model->output()[0].is_required, Optional(true));
  EXPECT_EQ(model->is_remote(), true);
  EXPECT_EQ(model->endpoint(), std::nullopt);
  EXPECT_THAT(model->endpoints(),
              ElementsAre("//aiplatform.googleapis.com/projects/aaa/locations/"
                          "bbb/endpoints/ccc",
                          "//aiplatform.googleapis.com/projects/aaa/locations/"
                          "ddd/endpoints/eee"));
  EXPECT_THAT(model->default_batch_size(), Optional(10));
}

TEST_P(SchemaUpdaterTest, CreateModel_OrReplace) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE OR REPLACE MODEL m
      INPUT ( feature INT64 )
      OUTPUT ( label STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      )
    )"}));
  EXPECT_THAT(
      schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
            }
            remote: true
            input { column_name: "feature" type: INT64 }
            output { column_name: "label" type: STRING }
          }
        }
      )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
        CREATE OR REPLACE MODEL m
        INPUT ( f INT64 )
        OUTPUT ( l STRING(MAX) )
        REMOTE OPTIONS (
          endpoint =
          '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
        )
      )"}));
  EXPECT_THAT(
      updated_schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
            }
            remote: true
            input { column_name: "f" type: INT64 }
            output { column_name: "l" type: STRING }
          }
        }
      )pb"));
}

TEST_P(SchemaUpdaterTest, CreateModel_IfNotExists) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE MODEL IF NOT EXISTS m
      INPUT ( feature INT64 )
      OUTPUT ( label STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      )
    )"}));
  EXPECT_THAT(
      schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
            }
            remote: true
            input { column_name: "feature" type: INT64 }
            output { column_name: "label" type: STRING }
          }
        }
      )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
      CREATE MODEL IF NOT EXISTS m
      INPUT ( f INT64 )
      OUTPUT ( l STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      )
    )"}));
  EXPECT_THAT(
      updated_schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
            }
            remote: true
            input { column_name: "feature" type: INT64 }
            output { column_name: "label" type: STRING }
          }
        }
      )pb"));
}

TEST_P(SchemaUpdaterTest, CreateModel_NoRemote_Error) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  EXPECT_THAT(CreateSchema({R"(
      CREATE MODEL m
      INPUT ( feature INT64 )
      OUTPUT ( label STRING(MAX) )
      OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      )
    )"}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Model m must specify REMOTE attribute")));
}

TEST_P(SchemaUpdaterTest, AlterModel) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE MODEL m
      INPUT ( feature INT64 )
      OUTPUT ( label STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      )
    )sql",
                                        R"sql(
      ALTER MODEL m SET OPTIONS (
        endpoint = NULL,
        endpoints = ['//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc']
      )
    )sql"}));
  EXPECT_THAT(
      schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoints"
              string_list_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
            }
            remote: true
            input { column_name: "feature" type: INT64 }
            output { column_name: "label" type: STRING }
          }
        }
      )pb"));
}

TEST_P(SchemaUpdaterTest, DropModel) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE MODEL m
      INPUT ( feature INT64 )
      OUTPUT ( label STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      ))sql",
                                        R"sql(
      DROP MODEL m
    )sql"}));
  EXPECT_THAT(schema->Dump(), EqualsProto(R"pb()pb"));
}

TEST_P(SchemaUpdaterTest, DropModel_NonExistent_Error) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  EXPECT_THAT(
      CreateSchema({R"(
      DROP MODEL m
    )"}),
      StatusIs(absl::StatusCode::kNotFound, HasSubstr("Model `m` not found")));
}

TEST_P(SchemaUpdaterTest, DropModel_IfExists) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      DROP MODEL IF EXISTS m
    )"}));
  EXPECT_THAT(schema->Dump(), EqualsProto(R"pb()pb"));
}

TEST_P(SchemaUpdaterTest, RecreateModel) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE MODEL m
      INPUT ( feature INT64 )
      OUTPUT ( label STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      ))",
                                                  R"(
      DROP MODEL m
      )",
                                                  R"(
      CREATE MODEL m
      INPUT ( f INT64 )
      OUTPUT ( l STRING(MAX) )
      REMOTE OPTIONS (
        endpoint = '//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc'
      )
    )"}));
  EXPECT_THAT(
      schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/aaa/locations/bbb/endpoints/ccc"
            }
            remote: true
            input { column_name: "f" type: INT64 }
            output { column_name: "l" type: STRING }
          }
        }
      )pb"));
}

TEST_P(SchemaUpdaterTest, MultipleStatements) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
    CREATE MODEL m1
    INPUT ( feature INT64 )
    OUTPUT ( label STRING(MAX) )
    REMOTE OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p1/locations/l1/endpoints/e1'
    ))sql",
                                        R"sql(
    CREATE MODEL m2
    INPUT ( feature INT64 )
    OUTPUT ( label STRING(MAX) )
    REMOTE OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p2/locations/l2/endpoints/e2'
    ))sql",
                                        R"sql(
    CREATE MODEL m3
    INPUT ( feature INT64 )
    OUTPUT ( label STRING(MAX) )
    REMOTE OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p3/locations/l3/endpoints/e3'
    ))sql",
                                        R"sql(
    DROP MODEL m1
    )sql",
                                        R"sql(
    ALTER MODEL m2 SET OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p2/locations/l2/endpoints/e2'
    ))sql"}));
  EXPECT_THAT(
      schema->Dump(), EqualsProto(R"pb(
        statement {
          create_model {
            model_name: "m2"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/p2/locations/l2/endpoints/e2"
            }
            remote: true
            input { column_name: "feature" type: INT64 }
            output { column_name: "label" type: STRING }
          }
        }
        statement {
          create_model {
            model_name: "m3"
            set_options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/p3/locations/l3/endpoints/e3"
            }
            remote: true
            input { column_name: "feature" type: INT64 }
            output { column_name: "label" type: STRING }
          }
        }
      )pb"));
}

}  // namespace
}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
