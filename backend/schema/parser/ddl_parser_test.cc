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

#include "backend/schema/parser/ddl_parser.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

namespace {

using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

// CREATE DATABASE

TEST(ParseCreateDatabase, CanParseCreateDatabase) {
  EXPECT_THAT(ParseCreateDatabase("CREATE DATABASE mydb"),
              IsOkAndHolds(test::EqualsProto("database_name: 'mydb'")));
}

TEST(ParseCreateDatabase, CanParsesCreateDatabaseWithQuotes) {
  EXPECT_THAT(ParseCreateDatabase("CREATE DATABASE `mydb`"),
              IsOkAndHolds(test::EqualsProto("database_name: 'mydb'")));
}

TEST(ParseCreateDatabase, CanParseCreateDatabaseWithHyphen) {
  // If database ID contains a hyphen, it must be enclosed in backticks.

  // Fails without backticks.
  EXPECT_THAT(ParseCreateDatabase("CREATE DATABASE mytestdb-1"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Passes with backticks.
  EXPECT_THAT(
      ParseCreateDatabase("CREATE DATABASE `mytestdb-1`"),
      IsOkAndHolds(test::EqualsProto("database_name: 'mytestdb-1'")));
}

TEST(ParseCreateDatabase, CannotParseEmptyDatabaseName) {
  EXPECT_THAT(ParseCreateDatabase("CREATE DATABASE"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// CREATE TABLE

TEST(ParseCreateTable, CanParseCreateTableWithNoColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                    ) PRIMARY KEY ()
                    )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      constraints { primary_key {} }
                    }
                  )")));
}

TEST(ParseCreateTable, CannotParseCreateTableWithoutName) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE (
                    ) PRIMARY KEY ()
                    )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyAKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyAKeyColumnTrailingComma) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyANonKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      Name STRING(MAX)
                    ) PRIMARY KEY ()
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                      }
                      constraints { primary_key {} }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyANonKeyColumnTrailingComma) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      Name STRING(MAX),
                    ) PRIMARY KEY ()
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                      }
                      constraints { primary_key {} }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithKeyAndNonKeyColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX)
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithTwoKeyColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX) NOT NULL
                    ) PRIMARY KEY (UserId, Name)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                        constraints { not_null { nullable: false } }
                      }
                      constraints {
                        primary_key {
                          key_part { key_column_name: "UserId" }
                          key_part { key_column_name: "Name" }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithTwoNonKeyColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64,
                      Name STRING(MAX)
                    ) PRIMARY KEY ()
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                      }
                      constraints { primary_key {} }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithTwoKeyColumnsAndANonKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX) NOT NULL,
                      Notes STRING(MAX)
                    ) PRIMARY KEY (UserId, Name)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Notes"
                        properties { column_type { type: STRING } }
                      }
                      constraints {
                        primary_key {
                          key_part { key_column_name: "UserId" }
                          key_part { key_column_name: "Name" }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithAKeyColumnAndTwoNonKeyColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX),
                      Notes STRING(MAX)
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                      }
                      columns {
                        column_name: "Notes"
                        properties { column_type { type: STRING } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateInterleavedTableWithNoColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Albums (
                    ) PRIMARY KEY (), INTERLEAVE IN PARENT Users ON DELETE CASCADE
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Albums"
                      constraints { primary_key {} }
                      constraints {
                        interleave {
                          type: IN_PARENT
                          parent: "Users"
                          on_delete { action: CASCADE }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateInterleavedTableWithKeyAndNonKeyColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Albums (
                      UserId INT64 NOT NULL,
                      AlbumId INT64 NOT NULL,
                      Name STRING(1024),
                      Description STRING(1024)
                    ) PRIMARY KEY (UserId, AlbumId),
                      INTERLEAVE IN PARENT Users ON DELETE CASCADE
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Albums"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "AlbumId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                        constraints { column_length { max_length: 1024 } }
                      }
                      columns {
                        column_name: "Description"
                        properties { column_type { type: STRING } }
                        constraints { column_length { max_length: 1024 } }
                      }
                      constraints {
                        primary_key {
                          key_part { key_column_name: "UserId" }
                          key_part { key_column_name: "AlbumId" }
                        }
                      }
                      constraints {
                        interleave {
                          type: IN_PARENT
                          parent: "Users"
                          on_delete { action: CASCADE }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable,
     CanParseCreateInterleavedTableWithExplicitOnDeleteNoAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Albums (
                    ) PRIMARY KEY (), INTERLEAVE IN PARENT Users ON DELETE NO ACTION
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Albums"
                      constraints { primary_key {} }
                      constraints {
                        interleave {
                          type: IN_PARENT
                          parent: "Users"
                          on_delete { action: NO_ACTION }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable,
     CanParseCreateInterleavedTableWithImplicitOnDeleteNoAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Albums (
                    ) PRIMARY KEY (), INTERLEAVE IN PARENT Users
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Albums"
                      constraints { primary_key {} }
                      constraints {
                        interleave {
                          type: IN_PARENT
                          parent: "Users"
                          on_delete { action: NO_ACTION }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithAnArrayField) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Names ARRAY<STRING(20)>,
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Names"
                        properties {
                          column_type {
                            type: ARRAY
                            array_subtype: { type: STRING }
                          }
                        }
                        constraints { column_length { max_length: 20 } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithNotNullArrayField) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Names ARRAY<STRING(MAX)> NOT NULL,
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Names"
                        properties {
                          column_type {
                            type: ARRAY
                            array_subtype: { type: STRING }
                          }
                        }
                        constraints { not_null { nullable: false } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithoutInterleaveClause) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX)
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseCreateTableWithForeignKeys) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE T (
                      A INT64,
                      B STRING(MAX),
                      FOREIGN KEY (B) REFERENCES U (Y),
                      CONSTRAINT FK_UXY FOREIGN KEY (B, A) REFERENCES U (X, Y),
                    ) PRIMARY KEY (A)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "T"
                      columns {
                        column_name: "A"
                        properties {
                          column_type {
                            type: INT64
                          }
                        }
                      }
                      columns {
                        column_name: "B"
                        properties {
                          column_type {
                            type: STRING
                          }
                        }
                      }
                      constraints {
                        foreign_key {
                          referencing_column_name: "B"
                          referenced_table_name: "U"
                          referenced_column_name: "Y"
                        }
                      }
                      constraints {
                        foreign_key {
                          constraint_name: "FK_UXY"
                          referencing_column_name: "B"
                          referencing_column_name: "A"
                          referenced_table_name: "U"
                          referenced_column_name: "X"
                          referenced_column_name: "Y"
                        }
                      }
                      constraints {
                        primary_key {
                          key_part {
                            key_column_name: "A"
                          }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseAlterTableWithAddUnnamedForeignKey) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE T ADD FOREIGN KEY (B, A) REFERENCES U (X, Y)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "T"
                      alter_constraint {
                        type: ADD
                        constraint {
                          foreign_key {
                            referencing_column_name: "B"
                            referencing_column_name: "A"
                            referenced_table_name: "U"
                            referenced_column_name: "X"
                            referenced_column_name: "Y"
                          }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseAlterTableWithAddNamedForeignKey) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE T ADD CONSTRAINT FK_UXY FOREIGN KEY (B, A)
                        REFERENCES U (X, Y)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "T"
                      alter_constraint {
                        constraint_name: "FK_UXY"
                        type: ADD
                        constraint {
                          foreign_key {
                            constraint_name: "FK_UXY"
                            referencing_column_name: "B"
                            referencing_column_name: "A"
                            referenced_table_name: "U"
                            referenced_column_name: "X"
                            referenced_column_name: "Y"
                          }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateTable, CanParseAlterTableWithDropConstraint) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE T DROP CONSTRAINT FK_UXY
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "T"
                      alter_constraint {
                        constraint_name: "FK_UXY"
                        type: DROP
                      }
                    }
                  )")));
}

// CREATE INDEX

TEST(ParseCreateIndex, CanParseCreateIndexBasicImplicitlyGlobal) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE NULL_FILTERED INDEX UsersByUserId ON Users(UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "UsersByUserId"
                      table_name: "Users"
                      properties { null_filtered: true }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateIndex, CanParseCreateIndexBasic) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE NULL_FILTERED INDEX GlobalAlbumsByName
                        ON Albums(Name)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "GlobalAlbumsByName"
                      table_name: "Albums"
                      properties { null_filtered: true }
                      constraints {
                        primary_key { key_part { key_column_name: "Name" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateIndex, CanParseCreateIndexBasicInterleaved) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE NULL_FILTERED INDEX LocalAlbumsByName
                        ON Albums(UserId, Name DESC), INTERLEAVE IN Users
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "LocalAlbumsByName"
                      table_name: "Albums"
                      properties { null_filtered: true }
                      constraints {
                        primary_key {
                          key_part { key_column_name: "UserId" }
                          key_part { key_column_name: "Name" order: DESC }
                        }
                      }
                      constraints { interleave { parent: "Users" } }
                    }
                  )")));
}

TEST(ParseCreateIndex, CanParseCreateIndexStoringAColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE NULL_FILTERED INDEX GlobalAlbumsByName ON Albums(Name)
                        STORING (Description)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "GlobalAlbumsByName"
                      table_name: "Albums"
                      columns {
                        column_name: "Description"
                        properties { stored: "Description" }
                      }
                      properties { null_filtered: true }
                      constraints {
                        primary_key { key_part { key_column_name: "Name" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateIndex, CanParseCreateIndexASCColumn) {
  // The default sort order is ASC for index columns.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE NULL_FILTERED INDEX UsersAsc ON Users(UserId ASC)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "UsersAsc"
                      table_name: "Users"
                      properties { null_filtered: true }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateIndex, CanParseCreateIndexDESCColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE NULL_FILTERED INDEX UsersAsc ON Users(UserId DESC)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "UsersAsc"
                      table_name: "Users"
                      properties { null_filtered: true }
                      constraints {
                        primary_key {
                          key_part { key_column_name: "UserId" order: DESC }
                        }
                      }
                    }
                  )")));
}

TEST(ParseCreateIndex, CanParseCreateIndexNotNullFiltered) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE INDEX UsersByUserId ON Users(UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "UsersByUserId"
                      table_name: "Users"
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

TEST(ParseCreateIndex, CanParseCreateUniqueIndex) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE UNIQUE INDEX UsersByUserId ON Users(UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_index {
                      index_name: "UsersByUserId"
                      table_name: "Users"
                      properties { unique: true }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));
}

// DROP TABLE

TEST(ParseDropTable, CanParseDropTableBasic) {
  EXPECT_THAT(
      ParseDDLStatement("DROP TABLE Users"),
      IsOkAndHolds(test::EqualsProto("drop_table { table_name: 'Users' }")));
}

TEST(ParseDropTable, CannotParseDropTableMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("DROP TABLE"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseDropTable, CannotParseDropTableInappropriateQuotes) {
  EXPECT_THAT(ParseDDLStatement("DROP `TABLE` Users"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseDropColumn, CannotParseDropColumnWithoutTable) {
  EXPECT_THAT(ParseDDLStatement("DROP COLUMN `TABLE`"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// DROP INDEX

TEST(ParseDropIndex, CanParseDropIndexBasic) {
  EXPECT_THAT(ParseDDLStatement("DROP INDEX LocalAlbumsByName"),
              IsOkAndHolds(test::EqualsProto(
                  "drop_index { index_name: 'LocalAlbumsByName' }")));
}

TEST(ParseDropIndex, CannotParseDropIndexMissingIndexName) {
  EXPECT_THAT(ParseDDLStatement("DROP INDEX"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseDropIndex, CannotParseDropIndexInappropriateQuotes) {
  EXPECT_THAT(ParseDDLStatement("DROP `INDEX` LocalAlbumsByName"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// ALTER TABLE ADD COLUMN

TEST(ParseAlterTable, CanParseAddColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE Users ADD COLUMN Notes STRING(MAX)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        type: ADD
                        column {
                          column_name: "Notes"
                          properties { column_type { type: STRING } }
                        }
                      }
                    }
                  )")));
}

TEST(ParseAlterTable, CanParseAddColumnNamedColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE Users ADD COLUMN `COLUMN` STRING(MAX)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        type: ADD
                        column {
                          column_name: "COLUMN"
                          properties { column_type { type: STRING } }
                        }
                      }
                    }
                  )")));
}

TEST(ParseAlterTable, CanParseAddColumnNamedColumnNoQuotes) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE Users ADD COLUMN COLUMN STRING(MAX)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        type: ADD
                        column {
                          column_name: "COLUMN"
                          properties { column_type { type: STRING } }
                        }
                      }
                    }
                  )")));
}

TEST(ParseAlterTable, CanParseAddColumnNoColumnName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD COLUMN STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAddColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users ADD Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users ADD COLUMN Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAddColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ADD Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ADD COLUMN Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD COLUMN Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE Users ADD `COLUMN` Notes STRING(MAX)"),
      StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// ALTER TABLE DROP COLUMN

TEST(ParseAlterTable, CanParseDropColumn) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN Notes"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column { type: DROP column_name: "Notes" }
                    }
                  )")));

  // We can even drop columns named "COLUMN" with quotes.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN `COLUMN`"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column { type: DROP column_name: "COLUMN" }
                    }
                  )")));

  // And then we can omit the quotes if we want.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN COLUMN"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column { type: DROP column_name: "COLUMN" }
                    }
                  )")));

  // But this one fails, since it doesn't mention column name.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseDropColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users DROP Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users DROP COLUMN Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseDropColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE DROP Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE DROP COLUMN Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP `COLUMN` Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// ALTER TABLE ALTER COLUMN

TEST(ParseAlterTable, CanParseAlterColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE Users ALTER COLUMN Notes STRING(MAX)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        column_name: "Notes"
                        type: ALTER
                        column {
                          column_name: "Notes"
                          properties { column_type { type: STRING } }
                        }
                      }
                    }
                  )")));
}

TEST(ParseAlterTable, CanParseAlterColumnNotNull) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE Users ALTER COLUMN Notes STRING(MAX) NOT NULL
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        column_name: "Notes"
                        type: ALTER
                        column {
                          column_name: "Notes"
                          properties { column_type { type: STRING } }
                          constraints { not_null { nullable: false } }
                        }
                      }
                    }
                  )")));
}

TEST(ParseAlterTable, CanParseAlterColumnNamedColumn) {
  // Columns named "COLUMN" with quotes can be modified.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE Users ALTER COLUMN `COLUMN` STRING(MAX)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        column_name: "COLUMN"
                        type: ALTER
                        column {
                          column_name: "COLUMN"
                          properties { column_type { type: STRING } }
                        }
                      }
                    }
                  )")));

  // Columns named "COLUMN" can be modified even without quotes.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    ALTER TABLE Users ALTER COLUMN COLUMN STRING(MAX)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        column_name: "COLUMN"
                        type: ALTER
                        column {
                          column_name: "COLUMN"
                          properties { column_type { type: STRING } }
                        }
                      }
                    }
                  )")));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingColumnName) {
  // Below statement is ambiguous and fails, unlike column named 'column'.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER COLUMN STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users ALTER Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users ALTER COLUMN Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ALTER Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ALTER COLUMN Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingColumnProperties) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER COLUMN Notes"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMiscErrors) {
  // Missing column name.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Multiple column names.
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE Users ALTER `COLUMN` Notes STRING(MAX)"),
      StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Missing table keyword.
  EXPECT_THAT(ParseDDLStatement("ALTER COLUMN Users.Notes STRING(MAX)"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// ALTER TABLE SET ONDELETE

TEST(ParseAlterTable, CanParseSetOnDeleteNoAction) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"(
            ALTER TABLE Albums SET ON DELETE NO ACTION
          )"),
      IsOkAndHolds(test::EqualsProto(
          R"(
            alter_table {
              table_name: "Albums"
              alter_constraint {
                type: ALTER
                constraint { interleave { on_delete { action: NO_ACTION } } }
              }
            }
          )")));
}

// MISCELLANEOUS

TEST(Miscellaneous, CannotParseNonAsciiCharacters) {
  // The literal escape character is not considered a valid ascii character.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE \x1b Users () PRIMARY KEY()
                  )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CanParseExtraWhitespaceCharacters) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE   Users () PRIMARY KEY()
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      constraints { primary_key {} }
                    }
                  )")));
}

TEST(Miscellaneous, CannotParseSmartQuotes) {
  // Smart quote characters are not considered valid quote characters.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      “Name” STRING(MAX)
                    ) PRIMARY KEY()
                  )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CanParseMixedCaseStatements) {
  // DDL Statements are case insensitive.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    cREaTE TABLE Users (
                      UserId iNT64 NOT NULL,
                      Name stRIng(maX)
                    ) PRIMARY KEY (UserId)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Users"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "UserId" } }
                      }
                    }
                  )")));

  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Albums (
                      UserId Int64 NOT NULL,
                      AlbumId INt64 NOT NULL,
                      Name STrinG(1024),
                      Description string(1024)
                    ) PRIMary KEY (UserId, AlbumId),
                      INTERLEAVE in PARENT Users ON DELETE CASCADE
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Albums"
                      columns {
                        column_name: "UserId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "AlbumId"
                        properties { column_type { type: INT64 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                        constraints { column_length { max_length: 1024 } }
                      }
                      columns {
                        column_name: "Description"
                        properties { column_type { type: STRING } }
                        constraints { column_length { max_length: 1024 } }
                      }
                      constraints {
                        primary_key {
                          key_part { key_column_name: "UserId" }
                          key_part { key_column_name: "AlbumId" }
                        }
                      }
                      constraints {
                        interleave {
                          type: IN_PARENT
                          parent: "Users"
                          on_delete { action: CASCADE }
                        }
                      }
                    }
                  )")));
}

TEST(Miscellaneous, CanParseCustomFieldLengths) {
  // Passing hex integer literals for length is also supported.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Sizes (
                      Name STRING(1) NOT NULL,
                      Email STRING(MAX),
                      PhotoSmall BYTES(1),
                      PhotoLarge BYTES(MAX),
                      HexLength STRING(0x42),
                    ) PRIMARY KEY (Name)
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Sizes"
                      columns {
                        column_name: "Name"
                        properties { column_type { type: STRING } }
                        constraints { column_length { max_length: 1 } }
                        constraints { not_null { nullable: false } }
                      }
                      columns {
                        column_name: "Email"
                        properties { column_type { type: STRING } }
                      }
                      columns {
                        column_name: "PhotoSmall"
                        properties { column_type { type: BYTES } }
                        constraints { column_length { max_length: 1 } }
                      }
                      columns {
                        column_name: "PhotoLarge"
                        properties { column_type { type: BYTES } }
                      }
                      columns {
                        column_name: "HexLength"
                        properties { column_type { type: STRING } }
                        constraints { column_length { max_length: 66 } }
                      }
                      constraints {
                        primary_key { key_part { key_column_name: "Name" } }
                      }
                    }
                  )")));
}

TEST(Miscellaneous, CanParseTimestamps) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Sizes (
                      Age INT64,
                      LastModified TIMESTAMP,
                      BirthDate DATE
                    ) PRIMARY KEY ()
                  )"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    create_table {
                      table_name: "Sizes"
                      columns {
                        column_name: "Age"
                        properties { column_type { type: INT64 } }
                      }
                      columns {
                        column_name: "LastModified"
                        properties { column_type { type: TIMESTAMP } }
                      }
                      columns {
                        column_name: "BirthDate"
                        properties { column_type { type: DATE } }
                      }
                      constraints { primary_key {} }
                    }
                  )")));
}

TEST(Miscellaneous, CannotParseStringFieldsWithoutLength) {
  // A custom field length is required for string fields.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Sizes (
                      Name STRING NOT NULL,
                    ) PRIMARY KEY (Name)
                  )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CannotParseNonStringFieldsWithLength) {
  // Non-string/bytes field types (e.g. int) don't allow the size option.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Sizes (
                      Name STRING(128) NOT NULL,
                      Age INT64(4),
                    ) PRIMARY KEY (Name)
                  )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CanParseQuotedIdentifiers) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"(
            CREATE TABLE `T` (
              `C` INT64 NOT NULL,
            ) PRIMARY KEY (`C`)
          )"),
      IsOkAndHolds(test::EqualsProto(
          R"(
            create_table {
              table_name: "T"
              columns {
                column_name: "C"
                properties { column_type { type: INT64 } }
                constraints { not_null { nullable: false } }
              }
              constraints { primary_key { key_part { key_column_name: "C" } } }
            }
          )")));
}

// AllowCommitTimestamp

TEST(AllowCommitTimestamp, CanParseSingleOption) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"(
            CREATE TABLE Users (
              UpdateTs TIMESTAMP OPTIONS (
                allow_commit_timestamp= true
              )
            ) PRIMARY KEY ()
          )"),
      IsOkAndHolds(test::EqualsProto(
          R"(
            create_table {
              table_name: "Users"
              columns {
                column_name: "UpdateTs"
                properties { column_type { type: TIMESTAMP } }
                options {
                  option_val { name: "allow_commit_timestamp" bool_value: true }
                }
              }
              constraints { primary_key {} }
            }
          )")));
}

TEST(AllowCommitTimestamp, CanClearOptionWithNull) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"(
            CREATE TABLE Users (
              UpdateTs TIMESTAMP OPTIONS (
                allow_commit_timestamp= null
              )
            ) PRIMARY KEY ()
          )"),
      IsOkAndHolds(test::EqualsProto(
          R"(
            create_table {
              table_name: "Users"
              columns {
                column_name: "UpdateTs"
                properties { column_type { type: TIMESTAMP } }
                options {
                  option_val { name: "allow_commit_timestamp" null_value: true }
                }
              }
              constraints { primary_key {} }
            }
          )")));
}

TEST(AllowCommitTimestamp, CannotParseSingleInvalidOption) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64,
                      UpdateTs TIMESTAMP OPTIONS (
                        bogus_option= true
                      )
                    ) PRIMARY KEY ()
                  )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Cannot also set an invalid option with null value.
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64,
                      UpdateTs TIMESTAMP OPTIONS (
                        bogus_option= null
                      )
                    ) PRIMARY KEY ()
                  )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(AllowCommitTimestamp, CanParseMultipleOptions) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"(
            CREATE TABLE Users (
              UserId INT64,
              UpdateTs TIMESTAMP OPTIONS (
                allow_commit_timestamp= true,
                allow_commit_timestamp= false
              )
            ) PRIMARY KEY ()
          )"),
      IsOkAndHolds(test::EqualsProto(
          R"(
            create_table {
              table_name: "Users"
              columns {
                column_name: "UserId"
                properties { column_type { type: INT64 } }
              }
              columns {
                column_name: "UpdateTs"
                properties { column_type { type: TIMESTAMP } }
                options {
                  option_val { name: "allow_commit_timestamp" bool_value: true }
                  option_val {
                    name: "allow_commit_timestamp"
                    bool_value: false
                  }
                }
              }
              constraints { primary_key {} }
            }
          )")));
}

TEST(AllowCommitTimestamp, CannotParseMultipleOptionsWithTrailingComma) {
  EXPECT_THAT(ParseDDLStatement(
                  R"(
                    CREATE TABLE Users (
                      UserId INT64,
                      UpdateTs TIMESTAMP OPTIONS (
                        allow_commit_timestamp= true,
                      )
                    ) PRIMARY KEY ()
                  )"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST(AllowCommitTimestamp, SetThroughOptions) {
  EXPECT_THAT(ParseDDLStatement(R"(
    ALTER TABLE Users ALTER COLUMN UpdateTs
    SET OPTIONS (allow_commit_timestamp = true))"),
              IsOkAndHolds(test::EqualsProto(
                  R"(
                    alter_table {
                      table_name: "Users"
                      alter_column {
                        column_name: "UpdateTs"
                        type: ALTER
                        column {
                          column_name: "UpdateTs"
                          options {
                            option_val {
                              name: "allow_commit_timestamp"
                              bool_value: true
                            }
                          }
                        }
                      }
                    }
                  )")));
}

}  // namespace

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
