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

#include "backend/schema/catalog/property_graph.h"

#include <cassert>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {
namespace {

using database_api::DatabaseDialect::POSTGRESQL;

using GraphElementTable = PropertyGraph::GraphElementTable;
using GraphNodeReference = PropertyGraph::GraphElementTable::GraphNodeReference;
using PropertyDefinition = PropertyGraph::GraphElementTable::PropertyDefinition;

void ValidateLabels(
    const PropertyGraph& graph,
    const absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>&
        expected_property_names_for_each_label) {
  ASSERT_EQ(graph.Labels().size(),
            expected_property_names_for_each_label.size());
  // Check that the label matches and so does the property names per label.
  for (size_t i = 0; i < graph.Labels().size(); ++i) {
    const std::string& label_name = graph.Labels().at(i).name;
    const auto& expected_property_names =
        expected_property_names_for_each_label.at(label_name);
    EXPECT_EQ(graph.Labels().at(i).property_names.size(),
              expected_property_names.size())
        << graph.Labels().at(i).name;
    EXPECT_THAT(graph.Labels().at(i).property_names,
                testing::UnorderedElementsAreArray(expected_property_names));
  }
}

void ValidatePropertyDeclarations(
    const PropertyGraph& graph,
    const std::vector<std::pair<std::string, std::string>>&
        expected_property_declarations) {
  absl::flat_hash_map<std::string, std::string> actual_property_declarations;
  for (const auto& property_declaration : graph.PropertyDeclarations()) {
    actual_property_declarations[property_declaration.name] =
        property_declaration.type;
  }
  EXPECT_THAT(
      actual_property_declarations,
      testing::UnorderedElementsAreArray(expected_property_declarations));
}

void ValidateGraphElementTable(
    const GraphElementTable& table, const std::string& expected_name,
    const std::string& expected_alias,
    const std::vector<std::string>& expected_key_column_names,
    const std::vector<PropertyDefinition>& expected_property_definitions,
    const GraphNodeReference& expected_source_node_reference = {},
    const GraphNodeReference& expected_target_node_reference = {}) {
  // Check name and alias.
  EXPECT_EQ(table.name(), expected_name);
  EXPECT_EQ(table.alias(), expected_alias);

  // Check key clause columns.
  EXPECT_THAT(table.key_clause_columns(),
              testing::UnorderedElementsAreArray(expected_key_column_names));

  // Check property definitions.
  EXPECT_EQ(table.property_definitions().size(),
            expected_property_definitions.size());
  absl::flat_hash_set<std::string> actual_property_definition_names;
  for (size_t i = 0; i < table.property_definitions().size(); ++i) {
    const auto& property_definition = table.property_definitions().at(i);
    actual_property_definition_names.insert(property_definition.name);
  }
  absl::flat_hash_set<std::string> expected_property_definition_names;
  for (const auto& property_definition : expected_property_definitions) {
    expected_property_definition_names.insert(property_definition.name);
  }
  EXPECT_EQ(actual_property_definition_names,
            expected_property_definition_names);

  // Check source and target node references.
  EXPECT_EQ(table.source_node_reference().node_table_name,
            expected_source_node_reference.node_table_name);
  EXPECT_THAT(table.source_node_reference().node_table_column_names,
              testing::UnorderedElementsAreArray(
                  expected_source_node_reference.node_table_column_names))
      << table.source_node_reference().DebugString();
  EXPECT_THAT(table.source_node_reference().edge_table_column_names,
              testing::UnorderedElementsAreArray(
                  expected_source_node_reference.edge_table_column_names));
  EXPECT_EQ(table.target_node_reference().node_table_name,
            expected_target_node_reference.node_table_name);
  EXPECT_THAT(table.target_node_reference().node_table_column_names,
              testing::UnorderedElementsAreArray(
                  expected_target_node_reference.node_table_column_names));
  EXPECT_THAT(table.target_node_reference().edge_table_column_names,
              testing::UnorderedElementsAreArray(
                  expected_target_node_reference.edge_table_column_names));
}

TEST_P(SchemaUpdaterTest, CreatePropertyGraphBasic) {
  if (GetParam() == POSTGRESQL) {
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
        CREATE TABLE Account (
      AccountID INT64 NOT NULL,
    ) PRIMARY KEY (AccountID))",
                                        R"(
    CREATE PROPERTY GRAPH aml
      NODE TABLES (
        Account KEY(AccountID)
      )
    )"}));

  const PropertyGraph* graph = schema->FindPropertyGraph("aml");
  ASSERT_NE(graph, nullptr);
  EXPECT_EQ(graph->Name(), "aml");

  ValidatePropertyDeclarations(*graph, {{"AccountID", "INT64"}});

  ValidateLabels(*graph, {{"Account", {"AccountID"}}});

  EXPECT_EQ(graph->NodeTables().size(), 1);
  EXPECT_EQ(graph->EdgeTables().size(), 0);
  ValidateGraphElementTable(graph->NodeTables().at(0),
                            /*expected_name=*/"Account",
                            /*expected_alias=*/"Account",
                            /*expected_key_column_names=*/{"AccountID"},
                            /*expected_property_definitions=*/
                            {{"AccountID", "AccountID"}});
}

TEST_P(SchemaUpdaterTest, CreatePropertyGraphMultiNodeAndEdge) {
  if (GetParam() == POSTGRESQL) {
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(CREATE TABLE Account (
        AccountID INT64 NOT NULL,
        AccountAlias STRING(1024),
        AliasHistory ARRAY<STRING(128)>,
        WireHistory JSON NOT NULL,
        AmountSavingAccount FLOAT64,
        AmountCheckingAccount NUMERIC,
        CreatedTs TIMESTAMP,
      ) PRIMARY KEY(AccountID))",
                                        R"(

      CREATE TABLE Person (
        PersonID INT64 NOT NULL,
        FullName STRING(1025),
        Gender STRING(MAX),
        GovId STRING(MAX),
      ) PRIMARY KEY(PersonID))",
                                        R"(

      CREATE TABLE PersonHasAccount (
        PersonID INT64 NOT NULL,
        AccountID INT64 NOT NULL,
        PersonName STRING(1024),
        AccountAlias STRING(1024),
        CONSTRAINT FkAccountID FOREIGN KEY
          (AccountID) REFERENCES Account(AccountID)
      ) PRIMARY KEY(PersonID, AccountID))",
                                        R"(

      CREATE PROPERTY GRAPH aml
        NODE TABLES (
          Account AS BankAccount
            KEY(AccountID)
            LABEL BANK_ACCOUNT PROPERTIES (
              AccountAlias AS alias,
              WireHistory AS wire_history,
              AmountSavingAccount AS amount_saving,
              AmountCheckingAccount AS amount_checking),
          Person AS BankClient
            KEY(PersonID)
            LABEL CLIENT PROPERTIES (
              FullName AS name
            )
        )
        EDGE TABLES (
          PersonHasAccount as HasAccount
            KEY(PersonID, AccountID)
              SOURCE KEY(PersonID, PersonName)
                REFERENCES BankClient(PersonID, FullName)
              DESTINATION KEY(AccountID, AccountAlias)
                REFERENCES BankAccount(AccountID, AccountAlias)
        ))",
                                    }));

  const PropertyGraph* graph = schema->FindPropertyGraph("aml");
  ASSERT_NE(graph, nullptr);
  EXPECT_EQ(graph->Name(), "aml");

  ValidatePropertyDeclarations(*graph, {{"alias", "STRING"},
                                        {"wire_history", "JSON"},
                                        {"amount_saving", "FLOAT64"},
                                        {"amount_checking", "NUMERIC"},
                                        {"name", "STRING"},
                                        {"PersonID", "INT64"},
                                        {"AccountID", "INT64"},
                                        {"PersonName", "STRING"},
                                        {"AccountAlias", "STRING"}});

  ValidateLabels(
      *graph, {{"BANK_ACCOUNT",
                {"amount_saving", "amount_checking", "wire_history", "alias"}},
               {"CLIENT", {"name"}},
               {"HasAccount",
                {"PersonID", "AccountID", "PersonName", "AccountAlias"}}});

  EXPECT_EQ(graph->NodeTables().size(), 2);
  EXPECT_EQ(graph->EdgeTables().size(), 1);
  ValidateGraphElementTable(graph->NodeTables().at(0),
                            /*expected_name=*/"Account",
                            /*expected_alias=*/"BankAccount",
                            /*expected_key_column_names=*/{"AccountID"},
                            /*expected_property_definitions=*/
                            {{"alias", "AccountAlias"},
                             {"wire_history", "WireHistory"},
                             {"amount_saving", "AmountSavingAccount"},
                             {"amount_checking", "AmountCheckingAccount"}});

  ValidateGraphElementTable(
      graph->NodeTables().at(1),
      /*expected_name=*/"Person",
      /*expected_alias=*/"BankClient",
      /*expected_key_column_names=*/{"PersonID"},
      /*expected_property_definitions=*/{{"name", "FullName"}});

  ValidateGraphElementTable(
      graph->EdgeTables().at(0),
      /*expected_name=*/"PersonHasAccount",
      /*expected_alias=*/"HasAccount",
      /*expected_key_column_names=*/{"PersonID", "AccountID"},
      /*expected_property_definitions=*/
      {{"AccountID", "AccountID"},
       {"PersonName", "PersonName"},
       {"PersonID", "PersonID"},
       {"AccountAlias", "AccountAlias"}},
      /*expected_source_node_reference=*/
      {"BankClient", {"PersonID", "FullName"}, {"PersonID", "PersonName"}},
      /*expected_target_node_reference=*/
      {"BankAccount",
       {"AccountID", "AccountAlias"},
       {"AccountID", "AccountAlias"}});
}

TEST_P(SchemaUpdaterTest, CreatePropertyGraphMultiLabels) {
  if (GetParam() == POSTGRESQL) {
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
        CREATE TABLE Account (
          id               INT64 NOT NULL,
          create_time       TIMESTAMP,
          is_blocked        BOOL,
          type             STRING(MAX),
          nick_name         STRING(MAX),
          phone_number      STRING(MAX),
          email            STRING(MAX),
          freq_login_type    STRING(MAX),
          last_login_time_ms  INT64,
          account_level     STRING(MAX),
        ) PRIMARY KEY (id))",
                                        R"(
        CREATE TABLE AccountTransferAccount (
          id               INT64 NOT NULL,
          to_id             INT64 NOT NULL,
          amount           FLOAT64,
          create_time       TIMESTAMP NOT NULL,
          order_number      STRING(MAX),
          comment          STRING(MAX),
          pay_type         STRING(MAX),
          goods_type       STRING(MAX),
        ) PRIMARY KEY (id, to_id, create_time),
        INTERLEAVE IN PARENT Account ON DELETE CASCADE)",
                                        R"(
        CREATE PROPERTY GRAPH FinGraph
          NODE TABLES (
            Account
          )
          EDGE TABLES (
            AccountTransferAccount
              SOURCE      KEY(id)         REFERENCES Account
              DESTINATION KEY(to_id)       REFERENCES Account
              LABEL       TRANSFERS
              LABEL       GIVES NO PROPERTIES
              LABEL       CONNECTS PROPERTIES (create_time, amount)
              LABEL       REACHES NO PROPERTIES,
            AccountTransferAccount AS SecondTransfer
              SOURCE      KEY(id)         REFERENCES Account
              DESTINATION KEY(to_id)       REFERENCES Account
              LABEL       DONATES PROPERTIES (amount)
              LABEL       LEADS
          ))",
                                    }));

  const PropertyGraph* graph = schema->FindPropertyGraph("FinGraph");
  ASSERT_NE(graph, nullptr);
  EXPECT_EQ(graph->Name(), "FinGraph");

  ValidatePropertyDeclarations(*graph, {{"id", "INT64"},
                                        {"create_time", "TIMESTAMP"},
                                        {"is_blocked", "BOOL"},
                                        {"type", "STRING"},
                                        {"nick_name", "STRING"},
                                        {"phone_number", "STRING"},
                                        {"email", "STRING"},
                                        {"freq_login_type", "STRING"},
                                        {"last_login_time_ms", "INT64"},
                                        {"account_level", "STRING"},
                                        {"to_id", "INT64"},
                                        {"amount", "FLOAT64"},
                                        {"order_number", "STRING"},
                                        {"comment", "STRING"},
                                        {"pay_type", "STRING"},
                                        {"goods_type", "STRING"}});

  ValidateLabels(
      *graph,
      {{"Account",
        {"id", "create_time", "is_blocked", "type", "nick_name", "phone_number",
         "email", "freq_login_type", "last_login_time_ms", "account_level"}},
       {"TRANSFERS",
        {"to_id", "amount", "pay_type", "order_number", "id", "create_time",
         "goods_type", "comment"}},
       {"GIVES", {}},
       {"CONNECTS", {"create_time", "amount"}},
       {"REACHES", {}},
       {"DONATES", {"amount"}},
       {"LEADS",
        {"to_id", "amount", "pay_type", "order_number", "id", "create_time",
         "goods_type", "comment"}}});

  EXPECT_EQ(graph->NodeTables().size(), 1);
  EXPECT_EQ(graph->EdgeTables().size(), 2);
  ValidateGraphElementTable(graph->NodeTables().at(0),
                            /*expected_name=*/"Account",
                            /*expected_alias=*/"Account",
                            /*expected_key_column_names=*/{"id"},
                            /*expected_property_definitions=*/
                            {{"id", "id"},
                             {"create_time", "create_time"},
                             {"is_blocked", "is_blocked"},
                             {"type", "type"},
                             {"nick_name", "nick_name"},
                             {"phone_number", "phone_number"},
                             {"email", "email"},
                             {"freq_login_type", "freq_login_type"},
                             {"last_login_time_ms", "last_login_time_ms"},
                             {"account_level", "account_level"}});

  ValidateGraphElementTable(
      graph->EdgeTables().at(0),
      /*expected_name=*/"AccountTransferAccount",
      /*expected_alias=*/"AccountTransferAccount",
      /*expected_key_column_names=*/{"id", "to_id", "create_time"},
      /*expected_property_definitions=*/
      {{"id", "id"},
       {"to_id", "to_id"},
       {"create_time", "create_time"},
       {"amount", "amount"},
       {"order_number", "order_number"},
       {"comment", "comment"},
       {"pay_type", "pay_type"},
       {"goods_type", "goods_type"}},
      /*expected_source_node_reference=*/
      {"Account", {"id"}, {"id"}},
      /*expected_target_node_reference=*/
      {"Account", {"id"}, {"to_id"}});

  ValidateGraphElementTable(
      graph->EdgeTables().at(1),
      /*expected_name=*/"AccountTransferAccount",
      /*expected_alias=*/"SecondTransfer",
      /*expected_key_column_names=*/{"id", "to_id", "create_time"},
      /*expected_property_definitions=*/
      {{"id", "id"},
       {"to_id", "to_id"},
       {"create_time", "create_time"},
       {"amount", "amount"},
       {"order_number", "order_number"},
       {"comment", "comment"},
       {"pay_type", "pay_type"},
       {"goods_type", "goods_type"}},
      /*expected_source_node_reference=*/
      {"Account", {"id"}, {"id"}},
      /*expected_target_node_reference=*/
      {"Account", {"id"}, {"to_id"}});
}

TEST_P(SchemaUpdaterTest, CreatePropertyGraphWithIndex) {
  if (GetParam() == POSTGRESQL) {
    return;
  }
  // Element key uniqueness constraint can be satisfied by unique index.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
        CREATE TABLE Person (
          PersonID INT64 NOT NULL,
          FirstName STRING(1025),
          LastName STRING(1025),
          Gender STRING(MAX),
          GovId STRING(MAX),
        ) PRIMARY KEY(PersonID))",
                                        R"(
        CREATE UNIQUE INDEX PersonByFirstLastName ON Person(FirstName, LastName))",
                                        R"(
        CREATE PROPERTY GRAPH aml
          NODE TABLES (
            Person KEY(FirstName, LastName)
          ))"}));

  const PropertyGraph* graph = schema->FindPropertyGraph("aml");
  ASSERT_NE(graph, nullptr);
  EXPECT_EQ(graph->Name(), "aml");

  ValidatePropertyDeclarations(*graph, {{"PersonID", "INT64"},
                                        {"FirstName", "STRING"},
                                        {"LastName", "STRING"},
                                        {"Gender", "STRING"},
                                        {"GovId", "STRING"}});

  ValidateLabels(
      *graph,
      {{"Person", {"PersonID", "FirstName", "LastName", "Gender", "GovId"}}});

  EXPECT_EQ(graph->NodeTables().size(), 1);
  EXPECT_EQ(graph->EdgeTables().size(), 0);
  ValidateGraphElementTable(
      graph->NodeTables().at(0),
      /*expected_name=*/"Person",
      /*expected_alias=*/"Person",
      /*expected_key_column_names=*/{"FirstName", "LastName"},
      /*expected_property_definitions=*/
      {{"PersonID", "PersonID"},
       {"FirstName", "FirstName"},
       {"LastName", "LastName"},
       {"Gender", "Gender"},
       {"GovId", "GovId"}});
}

TEST_P(SchemaUpdaterTest, ReplacePropertyGraph) {
  if (GetParam() == POSTGRESQL) {
    return;
  }
  // Create a schema with a property graph and replace it.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
        CREATE TABLE Account (
      AccountID INT64 NOT NULL,
      AliasHistory ARRAY<STRING(128)>,
      OwnerFirstName STRING(128),
      OwnerLastName STRING(128),
      FullName STRING(257) AS (ARRAY_TO_STRING([OwnerFirstName, OwnerLastName], " ")) STORED,
      FullNameReverseOrder STRING(MAX) AS (CONCAT(OwnerLastName, ' ', OwnerFirstName)),
      WireHistory JSON NOT NULL,
      AmountSavingAccount FLOAT64,
      AmountCheckingAccount NUMERIC,
      CreatedTs TIMESTAMP,
    ) PRIMARY KEY (AccountID))",
                                        R"(
    CREATE PROPERTY GRAPH aml
      NODE TABLES (
        Account KEY(AccountID)
      ))",
                                        R"(
      CREATE OR REPLACE PROPERTY GRAPH aml
        NODE TABLES (
          Account KEY(AccountID, FullName)
            LABEL BANK_ACCOUNT
              PROPERTIES (AliasHistory, FullNameReverseOrder, CreatedTs)
        ))",
                                    }));

  const PropertyGraph* graph = schema->FindPropertyGraph("aml");
  ASSERT_NE(graph, nullptr);
  EXPECT_EQ(graph->Name(), "aml");

  ValidatePropertyDeclarations(*graph, {{"AliasHistory", "ARRAY<STRING>"},
                                        {"FullNameReverseOrder", "STRING"},
                                        {"CreatedTs", "TIMESTAMP"}});

  ValidateLabels(*graph,
                 {{"BANK_ACCOUNT",
                   {"AliasHistory", "FullNameReverseOrder", "CreatedTs"}}});

  EXPECT_EQ(graph->NodeTables().size(), 1);
  EXPECT_EQ(graph->EdgeTables().size(), 0);

  // Only expect the reduced set of property definitions from the
  // CREATE OR REPLACE statement.
  ValidateGraphElementTable(
      graph->NodeTables().at(0),
      /*expected_name=*/"Account",
      /*expected_alias=*/"Account",
      /*expected_key_column_names=*/{"AccountID", "FullName"},
      /*expected_property_definitions=*/
      {{"AliasHistory", "AliasHistory"},
       {"FullNameReverseOrder", "FullNameReverseOrder"},
       {"CreatedTs", "CreatedTs"}});
}

TEST_P(SchemaUpdaterTest, DropPropertyGraph) {
  if (GetParam() == POSTGRESQL) {
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
        CREATE TABLE Account (
      AccountID INT64 NOT NULL,
    ) PRIMARY KEY (AccountID))",
                                        R"(
    CREATE PROPERTY GRAPH aml
      NODE TABLES (
        Account KEY(AccountID)
      ))",
                                        R"(
      DROP PROPERTY GRAPH aml
      )"}));

  const PropertyGraph* graph = schema->FindPropertyGraph("aml");
  ASSERT_EQ(graph, nullptr);
}

}  // namespace
}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
