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

#include "backend/query/index_hint_validator.h"

#include "zetasql/public/analyzer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/schema.h"
#include "common/errors.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

class IndexHintValidatorTest : public testing::Test {
 public:
  IndexHintValidatorTest() : fn_catalog_(&type_factory_) {}

  void SetUp() override {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema_, test::CreateSchemaFromDDL(
                                      {
                                          R"(
      CREATE TABLE T1 (
        k1 INT64,
        col1 STRING(MAX)
      ) PRIMARY KEY (k1))",
                                          R"(
      CREATE TABLE T2 (
        k2 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY (k2))",
                                          R"(
      CREATE TABLE T3 (
        k3 INT64 NOT NULL,
        col3 STRING(MAX) NOT NULL
      ) PRIMARY KEY (k3))",
                                          R"(
      CREATE INDEX I1 ON T1(col1))",
                                          R"(
      CREATE INDEX I2 ON T2(col2))",
                                          R"(
      CREATE NULL_FILTERED INDEX NF_I1 ON T1(col1))",
                                          R"(
      CREATE NULL_FILTERED INDEX I3 ON T3(col3))",
                                          R"(
      ALTER TABLE T2 ADD FOREIGN KEY(col2) REFERENCES T1(col1))"},
                                      &type_factory_));

    catalog_ = absl::make_unique<Catalog>(schema_.get(), &fn_catalog_,
                                          /*reader=*/nullptr);
  }

  std::unique_ptr<const zetasql::AnalyzerOutput> AnalyzeQuery(
      const std::string& sql) {
    std::unique_ptr<const zetasql::AnalyzerOutput> output;
    ZETASQL_EXPECT_OK(zetasql::AnalyzeStatement(sql, MakeGoogleSqlAnalyzerOptions(),
                                          catalog_.get(), &type_factory_,
                                          &output));
    return output;
  }

  const Schema* schema() const { return schema_.get(); }

 private:
  zetasql::TypeFactory type_factory_;

  const FunctionCatalog fn_catalog_;

  std::unique_ptr<const Schema> schema_;

  std::unique_ptr<Catalog> catalog_;
};

TEST_F(IndexHintValidatorTest, ValidateTableIndexHint) {
  auto output = AnalyzeQuery("SELECT k1 FROM T1@{force_index=I1}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  ZETASQL_EXPECT_OK(stmt->Accept(&validator));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintReturnsError) {
  auto output = AnalyzeQuery("SELECT k1 FROM T1@{force_index=I2}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintInInsertReturnsError) {
  auto output =
      AnalyzeQuery("INSERT INTO T1(k1) SELECT k1 FROM T1@{force_index=I2}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintInUpdateReturnsError) {
  auto output = AnalyzeQuery(
      "UPDATE T1 SET k1 = 1 WHERE k1 IN "
      "(SELECT k1 FROM T1@{force_index=I2})");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintInDeleteReturnsError) {
  auto output = AnalyzeQuery(
      "DELETE FROM T1 WHERE k1 IN "
      "(SELECT k1 FROM T1@{force_index=I2})");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest,
       NullFilteredIndexCannotBeUsedForNullableColumns) {
  auto output = AnalyzeQuery("SELECT col1 FROM T1@{force_index=NF_I1}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::NullFilteredIndexUnusable("NF_I1"));
}

TEST_F(IndexHintValidatorTest, DisableNullFilteredIndexCheck) {
  auto stmt1 = AnalyzeQuery(
      "SELECT col1 FROM T1@{force_index=NF_I1} "
      "WHERE col1 IS NOT NULL");
  // Statement is rejected without the disabling index hint.
  {
    IndexHintValidator validator{schema()};
    EXPECT_EQ(stmt1->resolved_statement()->Accept(&validator),
              error::NullFilteredIndexUnusable("NF_I1"));
  }

  // Statement is accepted after disabling the check through a hint.
  auto stmt2 = AnalyzeQuery(
      "SELECT col1 FROM T1"
      "@{force_index=NF_I1, "
      "spanner_emulator.disable_query_null_filtered_index_check=true} "
      "WHERE col1 IS NOT NULL");
  {
    IndexHintValidator validator{schema(),
                                 /*disable_null_filtered_index_check=*/true};
    ZETASQL_EXPECT_OK(stmt2->resolved_statement()->Accept(&validator));
  }
}

TEST_F(IndexHintValidatorTest,
       NullFilteredIndexCanOnlyBeUsedForNotNullColumns) {
  auto output = AnalyzeQuery("SELECT col3 FROM T3@{force_index=I3}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  ZETASQL_EXPECT_OK(stmt->Accept(&validator));
}

TEST_F(IndexHintValidatorTest, EmulatorManagedIndexName) {
  auto output = AnalyzeQuery(
      "SELECT k1 FROM T1@{force_index=IDX_T1_col1_U_EA26CF5871E82344}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator), error::QueryHintManagedIndexNotSupported(
                                          "IDX_T1_col1_U_EA26CF5871E82344"));
}

TEST_F(IndexHintValidatorTest, NonEmulatorManagedIndexName) {
  auto output = AnalyzeQuery(
      "SELECT k1 FROM T1@{force_index=IDX_T1_col1_U_EA26CF5871E82340}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  ZETASQL_EXPECT_OK(stmt->Accept(&validator));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
