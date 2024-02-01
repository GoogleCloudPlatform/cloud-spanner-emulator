//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/test_catalog/test_catalog.h"

#include "zetasql/public/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/test_catalog/emulator_catalog.h"
#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"

namespace postgres_translator::spangres::test {
namespace {

TEST(TestCatalogTest, BasicContents) {
  zetasql::EnumerableCatalog* test_catalog =
      GetSpangresTestSpannerUserCatalog();
  ASSERT_NE(test_catalog, nullptr);

  // Check that we have some tables.
  absl::flat_hash_set<const zetasql::Table*> output{};
  ZETASQL_EXPECT_OK(test_catalog->GetTables(&output));
  EXPECT_THAT(output,
              Contains(testing::Property(&zetasql::Table::Name, "keyvalue")));
  EXPECT_THAT(output, Contains(testing::Property(&zetasql::Table::Name,
                                                 "AllSpangresTypes")));
}

// Test that the catalog tables have columns as we expect.
TEST(TestCatalogTest, TableColumns) {
  zetasql::EnumerableCatalog* test_catalog =
      GetSpangresTestSpannerUserCatalog();
  ASSERT_NE(test_catalog, nullptr);

  const zetasql::Table* kv_table;
  ZETASQL_ASSERT_OK(test_catalog->FindTable({"keyvalue"}, &kv_table));
  ASSERT_NE(kv_table, nullptr);
  int kv_num_columns = 2;
  EXPECT_EQ(kv_table->NumColumns(), kv_num_columns);
  const zetasql::Column* key_column = kv_table->FindColumnByName("key");
  ASSERT_NE(key_column, nullptr);
  EXPECT_TRUE(key_column->GetType()->IsInt64());
  EXPECT_EQ(kv_table->GetColumn(0), key_column);
  const zetasql::Column* value_column = kv_table->FindColumnByName("value");
  ASSERT_NE(value_column, nullptr);
  EXPECT_TRUE(value_column->GetType()->IsString());
  EXPECT_EQ(kv_table->GetColumn(1), value_column);

  const zetasql::Table* spangres_table;
  ZETASQL_ASSERT_OK(test_catalog->FindTable({"AllSpangresTypes"}, &spangres_table));
  ASSERT_NE(spangres_table, nullptr);
  int all_types_num_columns = 10;
  EXPECT_EQ(spangres_table->NumColumns(), all_types_num_columns);
  const zetasql::Column* int64_column =
      spangres_table->FindColumnByName("int64_value");
  ASSERT_NE(int64_column, nullptr);
  EXPECT_TRUE(int64_column->GetType()->IsInt64());
  const zetasql::Column* bool_column =
      spangres_table->FindColumnByName("bool_value");
  ASSERT_NE(bool_column, nullptr);
  EXPECT_TRUE(bool_column->GetType()->IsBool());
  const zetasql::Column* double_column =
      spangres_table->FindColumnByName("double_value");
  ASSERT_NE(double_column, nullptr);
  EXPECT_TRUE(double_column->GetType()->IsDouble());
  const zetasql::Column* string_column =
      spangres_table->FindColumnByName("string_value");
  ASSERT_NE(string_column, nullptr);
  EXPECT_TRUE(string_column->GetType()->IsString());
  const zetasql::Column* bytes_column =
      spangres_table->FindColumnByName("bytes_value");
  ASSERT_NE(bytes_column, nullptr);
  EXPECT_TRUE(bytes_column->GetType()->IsBytes());
  const zetasql::Column* timestamp_column =
      spangres_table->FindColumnByName("timestamp_value");
  ASSERT_NE(timestamp_column, nullptr);
  EXPECT_TRUE(timestamp_column->GetType()->IsTimestamp());
  const zetasql::Column* date_column =
      spangres_table->FindColumnByName("date_value");
  ASSERT_NE(date_column, nullptr);
  EXPECT_TRUE(date_column->GetType()->IsDate());
  const zetasql::Column* numeric_column =
      spangres_table->FindColumnByName("numeric_value");
  ASSERT_NE(numeric_column, nullptr);
  EXPECT_TRUE(numeric_column->GetType()->IsExtendedType());
  const zetasql::Column* jsonb_column =
      spangres_table->FindColumnByName("jsonb_value");
  ASSERT_NE(jsonb_column, nullptr);
  EXPECT_TRUE(numeric_column->GetType()->IsExtendedType());
  const zetasql::Column* float_column =
      spangres_table->FindColumnByName("float_value");
  ASSERT_NE(float_column, nullptr);
}

// Basic test of ArrayTypes table--verify it exists and has a couple of the
// expected columns.
TEST(TestCatalogTest, Arrays) {
  zetasql::EnumerableCatalog* test_catalog =
      GetSpangresTestSpannerUserCatalog();
  ASSERT_NE(test_catalog, nullptr);

  const zetasql::Table* array_table;
  ZETASQL_ASSERT_OK(test_catalog->FindTable({"ArrayTypes"}, &array_table));
  const zetasql::Column* int_array_column =
      array_table->FindColumnByName("int_array");
  const zetasql::Type* int_array_type = int_array_column->GetType();
  EXPECT_TRUE(int_array_type->IsArray());
  EXPECT_TRUE(int_array_type->AsArray()->element_type()->IsInt64());
  const zetasql::Column* string_array_column =
      array_table->FindColumnByName("string_array");
  const zetasql::Type* string_array_type = string_array_column->GetType();
  EXPECT_TRUE(string_array_type->IsArray());
  EXPECT_TRUE(string_array_type->AsArray()->element_type()->IsString());
}

// Lookup a table that doesn't exist, ensuring we get null and don't crash.
TEST(TestCatalogTest, FailedLookup) {
  zetasql::EnumerableCatalog* test_catalog =
      GetSpangresTestSpannerUserCatalog();
  ASSERT_NE(test_catalog, nullptr);
  const zetasql::Table* no_table;
  ASSERT_EQ(test_catalog->FindTable({"no_such_table"}, &no_table).code(),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(no_table, nullptr);
}

}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
