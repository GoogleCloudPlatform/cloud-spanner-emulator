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

#include "backend/schema/catalog/foreign_key.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace {

struct Expected {
  std::string constraint_name;
  std::string generated_name;
  const Table* referencing_table;
  std::vector<const Column*> referencing_columns;
  const Table* referenced_table;
  std::vector<const Column*> referenced_columns;
};

Expected BuildExpected(
    const Schema* schema, const std::string& constraint_name,
    const std::string& generated_name,
    const std::string& referencing_table_name,
    const std::vector<std::string>& referencing_column_names,
    const std::string& referenced_table_name,
    const std::vector<std::string>& referenced_column_names) {
  auto columns = [&](const Table* table,
                     absl::Span<const std::string> column_names) {
    std::vector<const Column*> columns;
    for (const std::string& column_name : column_names) {
      columns.push_back(ASSERT_NOT_NULL(table->FindColumn(column_name)));
    }
    return columns;
  };
  const Table* referencing_table =
      ASSERT_NOT_NULL(schema->FindTable(referencing_table_name));
  const Table* referenced_table =
      ASSERT_NOT_NULL(schema->FindTable(referenced_table_name));
  Expected expected{
      constraint_name,   generated_name,
      referencing_table, columns(referencing_table, referencing_column_names),
      referenced_table,  columns(referenced_table, referenced_column_names)};
  return expected;
}

std::string Print(const Expected& expected) {
  return absl::Substitute(
      "FK:$0:$1($2):$3($4)",
      absl::StrCat(expected.constraint_name, expected.generated_name),
      expected.referencing_table->Name(),
      PrintNames(expected.referencing_columns),
      expected.referenced_table->Name(),
      PrintNames<Column>(expected.referenced_columns));
}

MATCHER_P2(IsForeignKeyOf, table, expected, Print(expected)) {
  bool match = true;
  auto check = [&](bool condition, absl::string_view message) {
    if (!condition) {
      if (match) {
        match = false;
        *result_listener << message;
      } else {
        *result_listener << ", " << message;
      }
    }
  };

  check(table == expected.referencing_table, "Wrong expected tables");

  const ForeignKey* fk = table->FindForeignKey(arg->Name());
  check(fk != nullptr, "Foreign key not found");
  if (fk == nullptr) {
    return false;
  }
  check(fk == arg, "Wrong foreign key");

  check(fk->referencing_table() == expected.referencing_table,
        "Wrong referencing table");
  check(fk->referenced_table() == expected.referenced_table,
        "Wrong referenced table");
  check(expected.referenced_table->FindReferencingForeignKey(arg->Name()) == fk,
        "Referencing foreign key not found");

  if (fk->referencing_table() == arg->referenced_table()) {
    check(expected.referencing_table->FindReferencingForeignKey(arg->Name()) ==
              arg,
          "Self-referencing foreign key not found on the referencing table");
    check(expected.referenced_table->FindForeignKey(arg->Name()) == arg,
          "Self-referencing foreign key not found on the referenced table");
  } else {
    check(expected.referencing_table->FindReferencingForeignKey(arg->Name()) ==
              nullptr,
          "Referencing foreign key found on the referencing table");
    check(expected.referenced_table->FindForeignKey(arg->Name()) == nullptr,
          "Foreign key found on the referenced table");
  }

  check(fk->constraint_name() == expected.constraint_name,
        "Wrong constraint name");
  check(fk->generated_name() == expected.generated_name,
        "Wrong generated name");

  auto check_columns = [&](absl::string_view label,
                           absl::Span<const Column* const> actual,
                           absl::Span<const Column* const> expected) {
    check(actual.size() == expected.size(),
          absl::StrCat("Wrong number of ", label, " columns"));
    if (actual.size() == expected.size()) {
      for (int i = 0; i < actual.size(); ++i) {
        check(actual.at(i) == expected.at(i),
              absl::StrCat("Wrong ", label, " column at index ", i));
      }
    }
  };
  check_columns("referencing", fk->referencing_columns(),
                expected.referencing_columns);
  check_columns("referenced", fk->referenced_columns(),
                expected.referenced_columns);
  return match;
}

TEST_F(SchemaUpdaterTest, CreateTableWithForeignKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        X INT64,
        Y INT64,
      ) PRIMARY KEY (X)
    )",
                                        R"(
      CREATE TABLE U (
        A INT64,
        B INT64,
        CONSTRAINT C FOREIGN KEY (A, B) REFERENCES T (X, Y),
      ) PRIMARY KEY (A)
    )"}));

  const Table* u = ASSERT_NOT_NULL(schema->FindTable("U"));
  const ForeignKey* c = ASSERT_NOT_NULL(u->FindForeignKey("C"));
  EXPECT_THAT(c, IsForeignKeyOf(u, BuildExpected(schema.get(), "C", "", "U",
                                                 {"A", "B"}, "T", {"X", "Y"})));
}

TEST_F(SchemaUpdaterTest, CreateTableWithUnnamedForeignKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        X INT64,
      ) PRIMARY KEY (X)
    )",
                                        R"(
      CREATE TABLE U (
        A INT64,
        FOREIGN KEY (A) REFERENCES T (X),
      ) PRIMARY KEY (A)
    )"}));

  const Table* u = ASSERT_NOT_NULL(schema->FindTable("U"));
  const ForeignKey* c =
      ASSERT_NOT_NULL(u->FindForeignKey("FK_U_T_FFADEDEE3430D435_1"));
  EXPECT_THAT(c, IsForeignKeyOf(u, BuildExpected(schema.get(), "",
                                                 "FK_U_T_FFADEDEE3430D435_1",
                                                 "U", {"A"}, "T", {"X"})));
}

TEST_F(SchemaUpdaterTest, CreateTableWithSelfReferencingForeignKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE U (
        A INT64,
        B INT64,
        CONSTRAINT C FOREIGN KEY (B) REFERENCES U (A),
      ) PRIMARY KEY (A)
    )"}));

  const Table* u = ASSERT_NOT_NULL(schema->FindTable("U"));
  const ForeignKey* c = ASSERT_NOT_NULL(u->FindForeignKey("C"));
  EXPECT_THAT(c, IsForeignKeyOf(u, BuildExpected(schema.get(), "C", "", "U",
                                                 {"B"}, "U", {"A"})));
}

TEST_F(SchemaUpdaterTest, AddForeignKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        X INT64,
      ) PRIMARY KEY (X)
    )",
                                        R"(
      CREATE TABLE U (
        A INT64,
      ) PRIMARY KEY (A)
    )",
                                        R"(
      ALTER TABLE U ADD CONSTRAINT C FOREIGN KEY (A) REFERENCES T (X)
    )"}));

  const Table* u = ASSERT_NOT_NULL(schema->FindTable("U"));
  const ForeignKey* c = ASSERT_NOT_NULL(u->FindForeignKey("C"));
  EXPECT_THAT(c, IsForeignKeyOf(u, BuildExpected(schema.get(), "C", "", "U",
                                                 {"A"}, "T", {"X"})));
}

TEST_F(SchemaUpdaterTest, DropForeignKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        X INT64,
      ) PRIMARY KEY (X)
    )",
                                        R"(
      CREATE TABLE U (
        A INT64,
        CONSTRAINT C FOREIGN KEY (A) REFERENCES T (X),
      ) PRIMARY KEY (A)
    )",
                                        R"(
      ALTER TABLE U DROP CONSTRAINT C
    )"}));

  const Table* t = ASSERT_NOT_NULL(schema->FindTable("T"));
  const Table* u = ASSERT_NOT_NULL(schema->FindTable("U"));
  EXPECT_EQ(u->FindForeignKey("C"), nullptr);
  EXPECT_EQ(u->foreign_keys().size(), 0);
  EXPECT_EQ(t->referencing_foreign_keys().size(), 0);
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
