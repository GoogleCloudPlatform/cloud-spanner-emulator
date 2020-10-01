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

#include <initializer_list>

#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/schema/updater/global_schema_names.h"
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
  const Index* referencing_index;

  const Table* referenced_table;
  std::vector<const Column*> referenced_columns;
  const Index* referenced_index;
};

Expected BuildExpected(const Schema* schema, const std::string& constraint_name,
                       const std::string& referencing_table_name,
                       const std::vector<std::string>& referencing_column_names,
                       const std::string& referencing_index_name,
                       const std::string& referenced_table_name,
                       const std::vector<std::string>& referenced_column_names,
                       const std::string& referenced_index_name,
                       int sequence_number = 1) {
  // Generates and returns a constraint name if one was not provided. Returns an
  // empty string if a name was provided.
  auto generated_name = [&]() -> std::string {
    if (!constraint_name.empty()) {
      return "";
    }
    GlobalSchemaNames names;
    for (int i = 1; true; ++i) {
      std::string name = names
                             .GenerateForeignKeyName(referencing_table_name,
                                                     referenced_table_name)
                             .value();
      if (i == sequence_number) {
        return name;
      }
    }
  };

  // Returns a vector of catalog columns corresponding to a given list of name.
  auto columns = [&](const Table* table,
                     absl::Span<const std::string> column_names) {
    std::vector<const Column*> columns;
    for (const std::string& column_name : column_names) {
      columns.push_back(ASSERT_NOT_NULL(table->FindColumn(column_name)));
    }
    return columns;
  };

  // Looks up the expected schema objects.
  const Table* referencing_table =
      ASSERT_NOT_NULL(schema->FindTable(referencing_table_name));
  const Index* referencing_index =
      referencing_index_name.empty()
          ? nullptr
          : ASSERT_NOT_NULL(schema->FindIndex(referencing_index_name));
  const Table* referenced_table =
      ASSERT_NOT_NULL(schema->FindTable(referenced_table_name));
  const Index* referenced_index =
      referenced_index_name.empty()
          ? nullptr
          : ASSERT_NOT_NULL(schema->FindIndex(referenced_index_name));

  // Returns the expected results.
  return {constraint_name,
          generated_name(),
          referencing_table,
          columns(referencing_table, referencing_column_names),
          referencing_index,
          referenced_table,
          columns(referenced_table, referenced_column_names),
          referenced_index};
}

std::string Print(const Index* index) {
  return index == nullptr ? "PK" : index->Name();
}

std::string Print(const Expected& expected) {
  return absl::Substitute(
      "FK:$0:$1($2)[$3]:$4($5)[$6]",
      absl::StrCat(expected.constraint_name, expected.generated_name),
      expected.referencing_table->Name(),
      PrintNames(expected.referencing_columns),
      Print(expected.referencing_index), expected.referenced_table->Name(),
      PrintNames<Column>(expected.referenced_columns),
      Print(expected.referenced_index));
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

  auto contains_managing_node = [](const Index* index, const ForeignKey* node) {
    auto managing_nodes = index->managing_nodes();
    return absl::c_find(managing_nodes, node) != managing_nodes.end();
  };
  check(fk->referencing_index() == expected.referencing_index,
        "Wrong referencing index");
  if (expected.referencing_index != nullptr) {
    check(contains_managing_node(expected.referencing_index, fk),
          "Foreign key not found in the referencing index managing nodes");
  }
  check(fk->referenced_index() == expected.referenced_index,
        "Wrong referenced index");
  if (expected.referenced_index != nullptr) {
    check(contains_managing_node(expected.referenced_index, fk),
          "Foreign key not found in the referenced index managing nodes");
  }
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
  EXPECT_THAT(
      c, IsForeignKeyOf(
             u, BuildExpected(schema.get(), "C", "U", {"A", "B"},
                              "IDX_U_A_B_N_8C11B65ACA7F01B9", "T", {"X", "Y"},
                              "IDX_T_X_Y_U_5AD6E41B495C5BB9")));
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
        A INT64 NOT NULL,
        FOREIGN KEY (A) REFERENCES T (X),
      ) PRIMARY KEY (A)
    )"}));

  Expected expected =
      BuildExpected(schema.get(), "", "U", {"A"}, "", "T", {"X"}, "");

  const Table* u = ASSERT_NOT_NULL(schema->FindTable("U"));
  const ForeignKey* c =
      ASSERT_NOT_NULL(u->FindForeignKey(expected.generated_name));
  EXPECT_THAT(c, IsForeignKeyOf(u, expected));
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
  EXPECT_THAT(c, IsForeignKeyOf(u, BuildExpected(schema.get(), "C", "U", {"B"},
                                                 "IDX_U_B_N_DC7E529471D378CF",
                                                 "U", {"A"}, "")));
}

TEST_F(SchemaUpdaterTest, AddForeignKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        X INT64,
        Y INT64,
      ) PRIMARY KEY (X)
    )",
                                        R"(
      CREATE TABLE U (
        A INT64 NOT NULL,
      ) PRIMARY KEY (A)
    )",
                                        R"(
      ALTER TABLE U ADD CONSTRAINT C FOREIGN KEY (A) REFERENCES T (Y)
    )"}));

  const Table* u = ASSERT_NOT_NULL(schema->FindTable("U"));
  const ForeignKey* c = ASSERT_NOT_NULL(u->FindForeignKey("C"));
  EXPECT_THAT(
      c, IsForeignKeyOf(u, BuildExpected(schema.get(), "C", "U", {"A"}, "", "T",
                                         {"Y"}, "IDX_T_Y_U_3E1CA8A966CF5C7A")));
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
