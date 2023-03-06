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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

absl::StatusOr<DDLStatement> ParseDDLStatement(
    absl::string_view ddl
) {
  DDLStatement statement;
  absl::Status s = ParseDDLStatement(ddl, &statement);
  if (s.ok()) {
    return statement;
  }
  return s;
}

// CREATE DATABASE

TEST(ParseCreateDatabase, CanParseCreateDatabase) {
  EXPECT_THAT(ParseDDLStatement("CREATE DATABASE mydb"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_database { db_name: "mydb" }
              )pb")));
}

TEST(ParseCreateDatabase, CanParsesCreateDatabaseWithQuotes) {
  EXPECT_THAT(ParseDDLStatement("CREATE DATABASE `mydb`"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_database { db_name: "mydb" }
              )pb")));
}

TEST(ParseCreateDatabase, CanParseCreateDatabaseWithHyphen) {
  // If database ID contains a hyphen, it must be enclosed in backticks.

  // Fails without backticks.
  EXPECT_THAT(ParseDDLStatement("CREATE DATABASE mytestdb-1"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Passes with backticks.
  EXPECT_THAT(ParseDDLStatement("CREATE DATABASE `mytestdb-1`"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_database { db_name: "mytestdb-1" }
              )pb")));
}

TEST(ParseCreateDatabase, CannotParseEmptyDatabaseName) {
  EXPECT_THAT(ParseDDLStatement("CREATE DATABASE"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// CREATE TABLE

TEST(ParseCreateTable, CanParseCreateTableWithNoColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                    ) PRIMARY KEY ()
                    )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                    }
                  )pb")));
}

TEST(ParseCreateTable, CannotParseCreateTableWithoutName) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE (
                    ) PRIMARY KEY ()
                    )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseCreateTable, CannotParseCreateTableWithoutPrimaryKey) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX)
                    )
                    )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Expecting 'PRIMARY' but found 'EOF'")));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyAKeyColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              primary_key { key_name: "UserId" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyAKeyColumnTrailingComma) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              primary_key { key_name: "UserId" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyANonKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      Name STRING(MAX)
                    ) PRIMARY KEY ()
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                      column { column_name: "Name" type: STRING }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithOnlyANonKeyColumnTrailingComma) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      Name STRING(MAX),
                    ) PRIMARY KEY ()
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                      column { column_name: "Name" type: STRING }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithKeyAndNonKeyColumns) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX)
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING }
              primary_key { key_name: "UserId" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithTwoKeyColumns) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX) NOT NULL
                    ) PRIMARY KEY (UserId, Name)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING not_null: true }
              primary_key { key_name: "UserId" }
              primary_key { key_name: "Name" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithTwoNonKeyColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      UserId INT64,
                      Name STRING(MAX)
                    ) PRIMARY KEY ()
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                      column { column_name: "UserId" type: INT64 }
                      column { column_name: "Name" type: STRING }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithTwoKeyColumnsAndANonKeyColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX) NOT NULL,
                      Notes STRING(MAX)
                    ) PRIMARY KEY (UserId, Name)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING not_null: true }
              column { column_name: "Notes" type: STRING }
              primary_key { key_name: "UserId" }
              primary_key { key_name: "Name" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithAKeyColumnAndTwoNonKeyColumns) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX),
                      Notes STRING(MAX)
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING }
              column { column_name: "Notes" type: STRING }
              primary_key { key_name: "UserId" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateInterleavedTableWithNoColumns) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Albums (
                    ) PRIMARY KEY (), INTERLEAVE IN PARENT Users ON DELETE CASCADE
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Albums"
              interleave_clause { table_name: "Users" on_delete: CASCADE }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateInterleavedTableWithKeyAndNonKeyColumns) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Albums (
                      UserId INT64 NOT NULL,
                      AlbumId INT64 NOT NULL,
                      Name STRING(1024),
                      Description STRING(1024)
                    ) PRIMARY KEY (UserId, AlbumId),
                      INTERLEAVE IN PARENT Users ON DELETE CASCADE
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Albums"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "AlbumId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING length: 1024 }
              column { column_name: "Description" type: STRING length: 1024 }
              primary_key { key_name: "UserId" }
              primary_key { key_name: "AlbumId" }
              interleave_clause { table_name: "Users" on_delete: CASCADE }
            }
          )pb")));
}

TEST(ParseCreateTable,
     CanParseCreateInterleavedTableWithExplicitOnDeleteNoAction) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Albums (
                    ) PRIMARY KEY (), INTERLEAVE IN PARENT Users ON DELETE NO ACTION
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Albums"
              interleave_clause { table_name: "Users" on_delete: NO_ACTION }
            }
          )pb")));
}

TEST(ParseCreateTable,
     CanParseCreateInterleavedTableWithImplicitOnDeleteNoAction) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Albums (
                    ) PRIMARY KEY (), INTERLEAVE IN PARENT Users
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Albums"
              interleave_clause { table_name: "Users" on_delete: NO_ACTION }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithAnArrayField) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Names ARRAY<STRING(20)>,
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column {
                column_name: "Names"
                type: ARRAY
                array_subtype { type: STRING length: 20 }
              }
              primary_key { key_name: "UserId" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithNotNullArrayField) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Names ARRAY<STRING(MAX)> NOT NULL,
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column {
                column_name: "Names"
                type: ARRAY
                not_null: true
                array_subtype { type: STRING }
              }
              primary_key { key_name: "UserId" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithoutInterleaveClause) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX)
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING }
              primary_key { key_name: "UserId" }
            }
          )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithForeignKeys) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE T (
                      A INT64,
                      B STRING(MAX),
                      FOREIGN KEY (B) REFERENCES U (Y),
                      CONSTRAINT FK_UXY FOREIGN KEY (B, A) REFERENCES U (X, Y),
                    ) PRIMARY KEY (A)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "T"
                      column {
                        column_name: "A"
                        type: INT64
                      }
                      column {
                        column_name: "B"
                        type: STRING
                      }
                      primary_key {
                        key_name: "A"
                      }
                      foreign_key {
                        constrained_column_name: "B"
                        referenced_table_name: "U"
                        referenced_column_name: "Y"
                        enforced: true
                      }
                      foreign_key {
                        constraint_name: "FK_UXY"
                        constrained_column_name: "B"
                        constrained_column_name: "A"
                        referenced_table_name: "U"
                        referenced_column_name: "X"
                        referenced_column_name: "Y"
                        enforced: true
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseAlterTableWithAddUnnamedForeignKey) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD FOREIGN KEY (B, A) REFERENCES U (X, Y)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "T"
                      add_foreign_key {
                        foreign_key {
                          constrained_column_name: "B"
                          constrained_column_name: "A"
                          referenced_table_name: "U"
                          referenced_column_name: "X"
                          referenced_column_name: "Y"
                          enforced: true
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseAlterTableWithAddNamedForeignKey) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD CONSTRAINT FK_UXY FOREIGN KEY (B, A)
                        REFERENCES U (X, Y)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "T"
                      add_foreign_key {
                        foreign_key {
                          constraint_name: "FK_UXY"
                          constrained_column_name: "B"
                          constrained_column_name: "A"
                          referenced_table_name: "U"
                          referenced_column_name: "X"
                          referenced_column_name: "Y"
                          enforced: true
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseAlterTableWithDropConstraint) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T DROP CONSTRAINT FK_UXY
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "T"
                      drop_constraint {
                        name: "FK_UXY"
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithJson) {
  EmulatorFeatureFlags::Flags flags;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE T (
                      K INT64 NOT NULL,
                      JsonVal JSON,
                      JsonArr ARRAY<JSON>
                    ) PRIMARY KEY (K)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "T"
                      column { column_name: "K" type: INT64 not_null: true }
                      column { column_name: "JsonVal" type: JSON }
                      column {
                        column_name: "JsonArr"
                        type: ARRAY
                        array_subtype { type: JSON }
                      }
                      primary_key { key_name: "K" }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithNumeric) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE T (
                      K INT64 NOT NULL,
                      NumericVal NUMERIC,
                      NumericArr ARRAY<NUMERIC>
                    ) PRIMARY KEY (K)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "T"
                      column { column_name: "K" type: INT64 not_null: true }
                      column { column_name: "NumericVal" type: NUMERIC }
                      column {
                        column_name: "NumericArr"
                        type: ARRAY
                        array_subtype { type: NUMERIC }
                      }
                      primary_key { key_name: "K" }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithRowDeletionPolicy) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      Key INT64,
      CreatedAt TIMESTAMP,
    ) PRIMARY KEY (Key), ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY))
  )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column { column_name: "Key" type: INT64 }
                  column { column_name: "CreatedAt" type: TIMESTAMP }
                  primary_key { key_name: "Key" }
                  row_deletion_policy {
                    column_name: "CreatedAt"
                    older_than { count: 7 unit: DAYS }
                  }
                }
              )pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      Key INT64,
      CreatedAt TIMESTAMP,
    ) PRIMARY KEY (Key), ROW DELETION POLICY (Older_thaN(CreatedAt, INTERVAL 7 DAY))
  )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column { column_name: "Key" type: INT64 }
                  column { column_name: "CreatedAt" type: TIMESTAMP }
                  primary_key { key_name: "Key" }
                  row_deletion_policy {
                    column_name: "CreatedAt"
                    older_than { count: 7 unit: DAYS }
                  }
                }
              )pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
        CREATE TABLE T(
          Key INT64,
          CreatedAt TIMESTAMP OPTIONS (allow_commit_timestamp = true),
        ) PRIMARY KEY (Key), ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY))
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column { column_name: "Key" type: INT64 }
                  column {
                    column_name: "CreatedAt"
                    type: TIMESTAMP
                    set_options {
                      option_name: "allow_commit_timestamp"
                      bool_value: true
                    }
                  }
                  primary_key { key_name: "Key" }
                  row_deletion_policy {
                    column_name: "CreatedAt"
                    older_than { count: 7 unit: DAYS }
                  }
                }
              )pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      Key INT64,
      CreatedAt TIMESTAMP,
    ) PRIMARY KEY (Key), ROW DELETION POLICY (YOUNGER_THAN(CreatedAt, INTERVAL 7 DAY))
  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       R"(Error parsing Spanner DDL statement:
    CREATE TABLE T(
      Key INT64,
      CreatedAt TIMESTAMP,
    ) PRIMARY KEY (Key), ROW DELETION POLICY (YOUNGER_THAN(CreatedAt, INTERVAL 7 DAY))
   : Only OLDER_THAN is supported.)"));
}

// CREATE INDEX

TEST(ParseCreateIndex, CanParseCreateIndexBasicImplicitlyGlobal) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX UsersByUserId ON Users(UserId)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "UsersByUserId"
                      index_base_name: "Users"
                      key { key_name: "UserId" }
                      null_filtered: true
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateIndexBasic) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX GlobalAlbumsByName
                        ON Albums(Name)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "GlobalAlbumsByName"
                      index_base_name: "Albums"
                      key { key_name: "Name" }
                      null_filtered: true
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateIndexBasicInterleaved) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX LocalAlbumsByName
                        ON Albums(UserId, Name DESC), INTERLEAVE IN Users
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "LocalAlbumsByName"
                      index_base_name: "Albums"
                      key { key_name: "UserId" }
                      key { key_name: "Name" order: DESC }
                      null_filtered: true
                      interleave_in_table: "Users"
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateIndexStoringAColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX GlobalAlbumsByName ON Albums(Name)
                        STORING (Description)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "GlobalAlbumsByName"
                      index_base_name: "Albums"
                      key { key_name: "Name" }
                      null_filtered: true
                      stored_column_definition { name: "Description" }
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateIndexASCColumn) {
  // The default sort order is ASC for index columns.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX UsersAsc ON Users(UserId ASC)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "UsersAsc"
                      index_base_name: "Users"
                      key { key_name: "UserId" }
                      null_filtered: true
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateIndexDESCColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX UsersDesc ON Users(UserId DESC)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "UsersDesc"
                      index_base_name: "Users"
                      key { key_name: "UserId" order: DESC }
                      null_filtered: true
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateIndexNotNullFiltered) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE INDEX UsersByUserId ON Users(UserId)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "UsersByUserId"
                      index_base_name: "Users"
                      key { key_name: "UserId" }
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateUniqueIndex) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE UNIQUE INDEX UsersByUserId ON Users(UserId)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "UsersByUserId"
                      index_base_name: "Users"
                      key { key_name: "UserId" }
                      unique: true
                    }
                  )pb")));
}

// DROP TABLE

TEST(ParseDropTable, CanParseDropTableBasic) {
  EXPECT_THAT(
      ParseDDLStatement("DROP TABLE Users"),
      IsOkAndHolds(test::EqualsProto("drop_table { table_name: 'Users' }")));
}

TEST(ParseDropTable, CannotParseDropTableMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("DROP TABLE"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseDropTable, CannotParseDropTableInappropriateQuotes) {
  EXPECT_THAT(ParseDDLStatement("DROP `TABLE` Users"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseDropColumn, CannotParseDropColumnWithoutTable) {
  EXPECT_THAT(ParseDDLStatement("DROP COLUMN `TABLE`"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// DROP INDEX

TEST(ParseDropIndex, CanParseDropIndexBasic) {
  EXPECT_THAT(ParseDDLStatement("DROP INDEX LocalAlbumsByName"),
              IsOkAndHolds(test::EqualsProto(
                  "drop_index { index_name: 'LocalAlbumsByName' }")));
}

TEST(ParseDropIndex, CannotParseDropIndexMissingIndexName) {
  EXPECT_THAT(ParseDDLStatement("DROP INDEX"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseDropIndex, CannotParseDropIndexInappropriateQuotes) {
  EXPECT_THAT(ParseDDLStatement("DROP `INDEX` LocalAlbumsByName"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// ALTER TABLE ADD COLUMN

TEST(ParseAlterTable, CanParseAddColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    ALTER TABLE Users ADD COLUMN Notes STRING(MAX)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Users"
              add_column { column { column_name: "Notes" type: STRING } }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseAddColumnNamedColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    ALTER TABLE Users ADD COLUMN `COLUMN` STRING(MAX)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Users"
              add_column { column { column_name: "COLUMN" type: STRING } }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseAddColumnNamedColumnNoQuotes) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    ALTER TABLE Users ADD COLUMN COLUMN STRING(MAX)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Users"
              add_column { column { column_name: "COLUMN" type: STRING } }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseAddNumericColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD COLUMN G NUMERIC
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "T"
                      add_column { column { column_name: "G" type: NUMERIC } }
                    }
                  )pb")));
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD COLUMN H ARRAY<NUMERIC>
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "T"
                      add_column {
                        column {
                          column_name: "H"
                          type: ARRAY
                          array_subtype { type: NUMERIC }
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseAddJsonColumn) {
  EmulatorFeatureFlags::Flags flags;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD COLUMN G JSON
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "T"
                      add_column { column { column_name: "G" type: JSON } }
                    }
                  )pb")));
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD COLUMN H ARRAY<JSON>
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "T"
                      add_column {
                        column {
                          column_name: "H"
                          type: ARRAY
                          array_subtype { type: JSON }
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseAddColumnNoColumnName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD COLUMN STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAddColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users ADD Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users ADD COLUMN Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAddColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ADD Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ADD COLUMN Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD COLUMN Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE Users ADD `COLUMN` Notes STRING(MAX)"),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

// ALTER TABLE DROP COLUMN

TEST(ParseAlterTable, CanParseDropColumn) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN Notes"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table { table_name: "Users" drop_column: "Notes" }
                  )pb")));

  // We can even drop columns named "COLUMN" with quotes.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN `COLUMN`"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table { table_name: "Users" drop_column: "COLUMN" }
                  )pb")));

  // And then we can omit the quotes if we want.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN COLUMN"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table { table_name: "Users" drop_column: "COLUMN" }
                  )pb")));

  // But this one fails, since it doesn't mention column name.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP COLUMN"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseDropColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users DROP Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users DROP COLUMN Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseDropColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE DROP Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE DROP COLUMN Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP `COLUMN` Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// ALTER TABLE ALTER COLUMN

TEST(ParseAlterTable, CanParseAlterColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    ALTER TABLE Users ALTER COLUMN Notes STRING(MAX)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Users"
              alter_column { column { column_name: "Notes" type: STRING } }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseAlterColumnNotNull) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    ALTER TABLE Users ALTER COLUMN Notes STRING(MAX) NOT NULL
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Users"
              alter_column {
                column { column_name: "Notes" type: STRING not_null: true }
              }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseAlterColumnNamedColumn) {
  // Columns named "COLUMN" with quotes can be modified.
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    ALTER TABLE Users ALTER COLUMN `COLUMN` STRING(MAX)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Users"
              alter_column { column { column_name: "COLUMN" type: STRING } }
            }
          )pb")));

  // Columns named "COLUMN" can be modified even without quotes.
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    ALTER TABLE Users ALTER COLUMN COLUMN STRING(MAX)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Users"
              alter_column { column { column_name: "COLUMN" type: STRING } }
            }
          )pb")));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingColumnName) {
  // Below statement is ambiguous and fails, unlike column named 'column'.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER COLUMN STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users ALTER Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users ALTER COLUMN Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ALTER Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ALTER COLUMN Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingColumnProperties) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER COLUMN Notes"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMiscErrors) {
  // Missing column name.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Multiple column names.
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE Users ALTER `COLUMN` Notes STRING(MAX)"),
      StatusIs(absl::StatusCode::kInvalidArgument));

  // Missing table keyword.
  EXPECT_THAT(ParseDDLStatement("ALTER COLUMN Users.Notes STRING(MAX)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// ALTER TABLE SET ONDELETE

TEST(ParseAlterTable, CanParseSetOnDeleteNoAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
            ALTER TABLE Albums SET ON DELETE NO ACTION
          )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "Albums"
                      set_on_delete { action: NO_ACTION }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseAlterTableWithRowDeletionPolicy) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE T ADD ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY))
  )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  add_row_deletion_policy {
                    column_name: "CreatedAt"
                    older_than { count: 7 unit: DAYS }
                  }
                }
              )pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE T REPLACE ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY))
  )sql"),

              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_row_deletion_policy {
                    column_name: "CreatedAt"
                    older_than { count: 7 unit: DAYS }
                  }
                }
              )pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE T DROP ROW DELETION POLICY
  )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  drop_row_deletion_policy {}
                }
              )pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE T DROP ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY))
  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Syntax error on line 2, column 44: Expecting "
                                 "'EOF' but found '('")));
}

// MISCELLANEOUS

TEST(Miscellaneous, CannotParseNonAsciiCharacters) {
  // The literal escape character is not considered a valid ascii character.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE \x1b Users () PRIMARY KEY()
                  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CanParseExtraWhitespaceCharacters) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE   Users () PRIMARY KEY()
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                    }
                  )pb")));
}

TEST(Miscellaneous, CannotParseSmartQuotes) {
  // Smart quote characters are not considered valid quote characters.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      “Name” STRING(MAX)
                    ) PRIMARY KEY()
                  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CanParseMixedCaseStatements) {
  // DDL Statements are case insensitive.
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    cREaTE TABLE Users (
                      UserId iNT64 NOT NULL,
                      Name stRIng(maX)
                    ) PRIMARY KEY (UserId)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Users"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING }
              primary_key { key_name: "UserId" }
            }
          )pb")));

  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Albums (
                      UserId Int64 NOT NULL,
                      AlbumId INt64 NOT NULL,
                      Name STrinG(1024),
                      Description string(1024)
                    ) PRIMary KEY (UserId, AlbumId),
                      INTERLEAVE in PARENT Users ON DELETE CASCADE
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Albums"
              column { column_name: "UserId" type: INT64 not_null: true }
              column { column_name: "AlbumId" type: INT64 not_null: true }
              column { column_name: "Name" type: STRING length: 1024 }
              column { column_name: "Description" type: STRING length: 1024 }
              primary_key { key_name: "UserId" }
              primary_key { key_name: "AlbumId" }
              interleave_clause { table_name: "Users" on_delete: CASCADE }
            }
          )pb")));
}

TEST(Miscellaneous, CanParseCustomFieldLengthsAndTimestamps) {
  // Passing hex integer literals for length is also supported.
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Sizes (
                      Name STRING(1) NOT NULL,
                      Email STRING(MAX),
                      PhotoSmall BYTES(1),
                      PhotoLarge BYTES(MAX),
                      HexLength STRING(0x42),
                      Age INT64,
                      LastModified TIMESTAMP,
                      BirthDate DATE
                    ) PRIMARY KEY (Name)
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            create_table {
              table_name: "Sizes"
              column {
                column_name: "Name"
                type: STRING
                not_null: true
                length: 1
              }
              column { column_name: "Email" type: STRING }
              column { column_name: "PhotoSmall" type: BYTES length: 1 }
              column { column_name: "PhotoLarge" type: BYTES }
              column { column_name: "HexLength" type: STRING length: 66 }
              column { column_name: "Age" type: INT64 }
              column { column_name: "LastModified" type: TIMESTAMP }
              column { column_name: "BirthDate" type: DATE }
              primary_key { key_name: "Name" }
            }
          )pb")));
}

TEST(Miscellaneous, CannotParseStringFieldsWithoutLength) {
  // A custom field length is required for string fields.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Sizes (
                      Name STRING NOT NULL,
                    ) PRIMARY KEY (Name)
                  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CannotParseNonStringFieldsWithLength) {
  // Non-string/bytes field types (e.g. int) don't allow the size option.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Sizes (
                      Name STRING(128) NOT NULL,
                      Age INT64(4),
                    ) PRIMARY KEY (Name)
                  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CanParseQuotedIdentifiers) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
            CREATE TABLE `T` (
              `C` INT64 NOT NULL,
            ) PRIMARY KEY (`C`)
          )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "T"
                      column { column_name: "C" type: INT64 not_null: true }
                      primary_key { key_name: "C" }
                    }
                  )pb")));
}

// AllowCommitTimestamp

TEST(AllowCommitTimestamp, CanParseSingleOption) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
            CREATE TABLE Users (
              UpdateTs TIMESTAMP OPTIONS (
                allow_commit_timestamp = true
              )
            ) PRIMARY KEY ()
          )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                      column {
                        column_name: "UpdateTs"
                        type: TIMESTAMP
                        set_options {
                          option_name: "allow_commit_timestamp"
                          bool_value: true
                        }
                      }
                    }
                  )pb")));
}

TEST(AllowCommitTimestamp, CanClearOptionWithNull) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
            CREATE TABLE Users (
              UpdateTs TIMESTAMP OPTIONS (
                allow_commit_timestamp= null
              )
            ) PRIMARY KEY ()
          )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                      column {
                        column_name: "UpdateTs"
                        type: TIMESTAMP
                        set_options {
                          option_name: "allow_commit_timestamp"
                          null_value: true
                        }
                      }
                    }
                  )pb")));
}

TEST(AllowCommitTimestamp, CannotParseSingleInvalidOption) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      UserId INT64,
                      UpdateTs TIMESTAMP OPTIONS (
                        bogus_option= true
                      )
                    ) PRIMARY KEY ()
                  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Cannot also set an invalid option with null value.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      UserId INT64,
                      UpdateTs TIMESTAMP OPTIONS (
                        bogus_option= null
                      )
                    ) PRIMARY KEY ()
                  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(AllowCommitTimestamp, CanParseMultipleOptions) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
            CREATE TABLE Users (
              UserId INT64,
              UpdateTs TIMESTAMP OPTIONS (
                allow_commit_timestamp= true,
                allow_commit_timestamp= false
              )
            ) PRIMARY KEY ()
          )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                      column { column_name: "UserId" type: INT64 }
                      column {
                        column_name: "UpdateTs"
                        type: TIMESTAMP
                        set_options {
                          option_name: "allow_commit_timestamp"
                          bool_value: true
                        }
                        set_options {
                          option_name: "allow_commit_timestamp"
                          bool_value: false
                        }
                      }
                    }
                  )pb")));
}

TEST(AllowCommitTimestamp, CannotParseMultipleOptionsWithTrailingComma) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      UserId INT64,
                      UpdateTs TIMESTAMP OPTIONS (
                        allow_commit_timestamp= true,
                      )
                    ) PRIMARY KEY ()
                  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(AllowCommitTimestamp, SetThroughOptions) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER COLUMN UpdateTs
    SET OPTIONS (allow_commit_timestamp = true))sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            set_column_options {
              column_path { table_name: "Users" column_name: "UpdateTs" }
              options { option_name: "allow_commit_timestamp" bool_value: true }
            }
          )pb")));
}

TEST(AllowCommitTimestamp, CannotParseInvalidOptionValue) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64,
                      UpdateTs TIMESTAMP OPTIONS (
                        allow_commit_timestamp= bogus,
                      )
                    ) PRIMARY KEY ()
                  )sql"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'bogus' while parsing: option_key_val")));
}

TEST(ParseToken, CannotParseUnterminatedTripleQuote) {
  static const char *const statements[] = {
      "'''",        "''''",          "'''''",       "'''abc",
      "'''abc''",   "'''abc'",       "r'''abc",     "b'''abc",
      "\"\"\"",     "\"\"\"\"",      "\"\"\"\"\"",  "rb\"\"\"abc",
      "\"\"\"abc",  "\"\"\"abc\"\"", "\"\"\"abc\"", "r\"\"\"abc",
      "b\"\"\"abc", "rb\"\"\"abc",
  };
  for (const char *statement : statements) {
    EXPECT_THAT(
        ParseDDLStatement(statement),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("Encountered an unclosed triple quoted string")));
  }
}

TEST(ParseToken, CannotParseIllegalStringEscape) {
  EXPECT_THAT(
      ParseDDLStatement("\"\xc2\""),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Encountered Structurally invalid UTF8 string")));
}

TEST(ParseToken, CannotParseIllegalBytesEscape) {
  EXPECT_THAT(
      ParseDDLStatement("b'''k\\u0030'''"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "Encountered Illegal escape sequence: Unicode escape sequence")));
}

class GeneratedColumns : public ::testing::Test {
 public:
  GeneratedColumns()
      : feature_flags_({.enable_stored_generated_columns = true}) {}

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(GeneratedColumns, CanParseCreateTableWithStoredGeneratedColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
                CREATE TABLE T (
                  K INT64 NOT NULL,
                  V INT64,
                  G INT64 AS (K + V) STORED,
                  G2 INT64 AS (G +
                               K * V) STORED,
                ) PRIMARY KEY (K))sql"),
              IsOkAndHolds(test::EqualsProto(R"d(
                create_table   {
                  table_name: "T"
                  column {
                    column_name: "K"
                    type: INT64
                    not_null: true
                  }
                  column {
                    column_name: "V"
                    type: INT64
                  }
                  column {
                    column_name: "G"
                    type: INT64
                    generated_column {
                      expression: "(K + V)"
                      stored: true
                    }
                  }
                  column {
                    column_name: "G2"
                    type: INT64
                    generated_column {
                      expression: "(G +\n                               K * V)"
                      stored: true
                    }
                  }
                  primary_key {
                    key_name: "K"
                  }
                }
              )d")));
}

TEST_F(GeneratedColumns, CanParseAlterTableAddStoredGeneratedColumn) {
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ADD COLUMN G INT64 AS (K + V) STORED"),
      IsOkAndHolds(test::EqualsProto(
          R"d(
            alter_table {
              table_name: "T"
              add_column {
                column {
                  column_name: "G"
                  type: INT64
                  generated_column {
                    expression: "(K + V)"
                    stored: true
                  }
                }
              }
            }
          )d")));
}

TEST_F(GeneratedColumns, CanParseAlterTableAlterStoredGeneratedColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          "ALTER TABLE T ALTER COLUMN G INT64 NOT NULL AS (K + V) STORED"),
      IsOkAndHolds(test::EqualsProto(
          R"d(
            alter_table {
              table_name: "T"
              alter_column {
                column {
                  column_name: "G"
                  type: INT64
                  not_null: true
                  generated_column {
                    expression: "(K + V)"
                    stored: true
                  }
                }
              }
            }
          )d")));
}

class ColumnDefaultValues : public ::testing::Test {
 public:
  ColumnDefaultValues()
      : feature_flags_({.enable_column_default_values = true}) {}

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(ColumnDefaultValues, CreateTableWithDefaultNonKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
                CREATE TABLE T (
                  K INT64 NOT NULL,
                  D INT64 DEFAULT (10),
                ) PRIMARY KEY (K))sql"),
              IsOkAndHolds(test::EqualsProto(R"d(
                create_table   {
                  table_name: "T"
                  column {
                    column_name: "K"
                    type: INT64
                    not_null: true
                  }
                  column {
                    column_name: "D"
                    type: INT64
                    column_default {
                      expression: "10"
                    }
                  }
                  primary_key {
                    key_name: "K"
                  }
                }
              )d")));
}

TEST_F(ColumnDefaultValues, CreateTableWithDefaultPrimaryKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
                CREATE TABLE T (
                  K INT64 NOT NULL DEFAULT (1),
                  V INT64,
                ) PRIMARY KEY (K))sql"),
              IsOkAndHolds(test::EqualsProto(R"d(
                create_table   {
                  table_name: "T"
                  column {
                    column_name: "K"
                    type: INT64
                    not_null: true
                    column_default {
                      expression: "1"
                    }
                  }
                  column {
                    column_name: "V"
                    type: INT64
                  }
                  primary_key {
                    key_name: "K"
                  }
                }
              )d")));
}

TEST_F(ColumnDefaultValues, CannotParseDefaultAndGeneratedColumn) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_column_default_values = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE TABLE T (
        K INT64,
        V INT64,
        G INT64 DEFAULT (1) AS (1) STORED,
       ) PRIMARY KEY (K)
    )sql"),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST_F(ColumnDefaultValues, CannotParseGeneratedAndDefaultColumn) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_column_default_values = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE TABLE T (
        K INT64,
        V INT64,
        G INT64 AS (1) STORED DEFAULT (1),
       ) PRIMARY KEY (K)
    )sql"),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST_F(ColumnDefaultValues, AlterTableAddDefaultColumn) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE T ADD COLUMN D INT64 DEFAULT (1)"),
              IsOkAndHolds(test::EqualsProto(
                  R"d(
                    alter_table   {
                      table_name: "T"
                      add_column {
                        column {
                          column_name: "D"
                          type: INT64
                          column_default {
                            expression: "1"
                          }
                        }
                      }
                    }
                )d")));
}

TEST_F(ColumnDefaultValues, AlterTableAlterDefaultColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  "ALTER TABLE T ALTER COLUMN D INT64 NOT NULL DEFAULT (1)"),
              IsOkAndHolds(test::EqualsProto(
                  R"d(
                    alter_table   {
                      table_name: "T"
                      alter_column {
                        column {
                          column_name: "D"
                          type: INT64
                          not_null: true
                          column_default {
                            expression: "1"
                          }
                        }
                      }
                    }
                )d")));
}

TEST_F(ColumnDefaultValues, AlterTableAlterDefaultColumnToNull) {
  EXPECT_THAT(ParseDDLStatement(
                  "ALTER TABLE T ALTER COLUMN D INT64 NOT NULL DEFAULT (NULL)"),
              IsOkAndHolds(test::EqualsProto(
                  R"d(
                    alter_table   {
                      table_name: "T"
                      alter_column {
                        column {
                          column_name: "D"
                          type: INT64
                          not_null: true
                          column_default {
                            expression: "NULL"
                          }
                        }
                      }
                    }
                )d")));
}

TEST_F(ColumnDefaultValues, AlterTableSetDefaultToColumn) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE T ALTER COLUMN D SET DEFAULT (1)"),
              IsOkAndHolds(test::EqualsProto(
                  R"d(
                    alter_table   {
                      table_name: "T"
                      alter_column {
                        column {
                          column_name: "D"
                          type: NONE
                          column_default {
                            expression: "1"
                          }
                        }
                        operation: SET_DEFAULT
                      }
                    }
              )d")));
}

TEST_F(ColumnDefaultValues, AlterTableDropDefaultToColumn) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE T ALTER COLUMN D DROP DEFAULT"),
              IsOkAndHolds(test::EqualsProto(
                  R"d(
              alter_table   {
                table_name: "T"
                alter_column {
                  column {
                    column_name: "D"
                    type: NONE
                  }
                  operation: DROP_DEFAULT
                }
              }
          )d")));
}

TEST_F(ColumnDefaultValues, InvalidDropDefault) {
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ALTER COLUMN D DROP DEFAULT (1)"),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST_F(ColumnDefaultValues, InvalidSetDefault) {
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ALTER COLUMN D SET DEFAULT"),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

class CheckConstraint : public ::testing::Test {
 public:
  CheckConstraint()
      : feature_flags_({.enable_stored_generated_columns = true}) {}

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(CheckConstraint, CanParseCreateTableWithCheckConstraint) {
  EXPECT_THAT(ParseDDLStatement("CREATE TABLE T ("
                                "  Id INT64,"
                                "  Value INT64,"
                                "  CHECK(Value > 0),"
                                "  CONSTRAINT value_gt_zero CHECK(Value > 0),"
                                "  CHECK(Value > 1),"
                                ") PRIMARY KEY(Id)"),
              IsOkAndHolds(test::EqualsProto(R"d(
                create_table   {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                  }
                  column {
                    column_name: "Value"
                    type: INT64
                  }
                  primary_key {
                    key_name: "Id"
                  }
                  check_constraint {
                    expression: "Value > 0"
                    enforced: true
                  }
                  check_constraint {
                    name: "value_gt_zero"
                    expression: "Value > 0"
                    enforced: true
                  }
                  check_constraint {
                    expression: "Value > 1"
                    enforced: true
                  }
                }
              )d")));
}

TEST_F(CheckConstraint, CanParseAlterTableAddCheckConstraint) {
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ADD CONSTRAINT B_GT_ZERO CHECK(B > 0)"),
      IsOkAndHolds(test::EqualsProto(R"d(
        alter_table {
          table_name: "T"
          add_check_constraint {
            check_constraint {
              name: "B_GT_ZERO"
              expression: "B > 0"
              enforced: true
            }
          }
        }
      )d")));
}

TEST_F(CheckConstraint, CanParseAlterTableAddUnamedCheckConstraint) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE T ADD CHECK(B > 0)"),
              IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "B > 0"
                      enforced: true
                    }
                  }
                }
              )d")));
}

TEST_F(CheckConstraint, CanParseEscapingCharsInCheckConstraint) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"d(ALTER TABLE T ADD CHECK(B > CONCAT(')\'"', ''''")''', "'\")", """'")""")))d"),
      IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "B > CONCAT(\')\\\'\"\', \'\'\'\'\")\'\'\', \"\'\\\")\", \"\"\"\'\")\"\"\")"
                      enforced: true
                    }
                  }
                }
              )d")));

  EXPECT_THAT(
      ParseDDLStatement(
          R"d(ALTER TABLE T ADD CHECK(B > CONCAT(b')\'"', b''''")''', b"'\")", b"""'")""")))d"),
      IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "B > CONCAT(b\')\\\'\"\', b\'\'\'\'\")\'\'\', b\"\'\\\")\", b\"\"\"\'\")\"\"\")"
                      enforced: true
                    }
                  }
                }
              )d")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(ALTER TABLE T ADD CHECK(B > '\a\b\r\n\t\\'))sql"),
      IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "B > \'\\a\\b\\r\\n\\t\\\\\'"
                      enforced: true
                    }
                  }
                }
              )d")));

  // The DDL statement indentation is intended for the two cases following.
  EXPECT_THAT(ParseDDLStatement(
                  R"d(ALTER TABLE T ADD CHECK(B > CONCAT('\n', ''''line 1
  line 2''', "\n", """line 11
  line22""")))d"),
              IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "B > CONCAT(\'\\n\', \'\'\'\'line 1\n  line 2\'\'\', \"\\n\", \"\"\"line 11\n  line22\"\"\")"
                      enforced: true
                    }
                  }
                }
              )d")));

  EXPECT_THAT(ParseDDLStatement(
                  R"d(ALTER TABLE T ADD CHECK(B > CONCAT(b'\n', b''''line 1
  line 2''', b"\n", b"""line 11
  line22""")))d"),
              IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "B > CONCAT(b\'\\n\', b\'\'\'\'line 1\n  line 2\'\'\', b\"\\n\", b\"\"\"line 11\n  line22\"\"\")"
                      enforced: true
                    }
                  }
                }
              )d")));
}

TEST_F(CheckConstraint, CanParseRegexContainsInCheckConstraint) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER TABLE T ADD CHECK(REGEXP_CONTAINS(B, r'f\(a,(.*),d\)')))sql"),
      IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "REGEXP_CONTAINS(B, r\'f\\(a,(.*),d\\)\')"
                      enforced: true
                    }
                  }
                }
              )d")));

  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER TABLE T ADD CHECK(REGEXP_CONTAINS(B, rb'f\(a,(.*),d\)')))sql"),
      IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "REGEXP_CONTAINS(B, rb\'f\\(a,(.*),d\\)\')"
                      enforced: true
                    }
                  }
                }
              )d")));
}

TEST_F(CheckConstraint, CanParseOctalNumberInCheckConstraint) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE T ADD CHECK(B > 05)"),
              IsOkAndHolds(test::EqualsProto(R"d(
                alter_table {
                  table_name: "T"
                  add_check_constraint {
                    check_constraint {
                      expression: "B > 05"
                      enforced: true
                    }
                  }
                }
              )d")));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ADD CHECK(B > 005 + 5 + 0.5 + .5e2)"),
      IsOkAndHolds(test::EqualsProto(R"d(
        alter_table {
          table_name: "T"
          add_check_constraint {
            check_constraint {
              expression: "B > 005 + 5 + 0.5 + .5e2"
              enforced: true
            }
          }
        }
      )d")));
}

TEST_F(CheckConstraint, ParseSyntaxErrorsInCheckConstraint) {
  EXPECT_THAT(
      ParseDDLStatement("CREATE TABLE T ("
                        "  Id INT64,"
                        "  Value INT64,"
                        "  CONSTRAINT ALL CHECK(Value > 0),"
                        ") PRIMARY KEY(Id)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'ALL' while parsing: column_type")));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ADD CHECK(B > '\\c')"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Expecting ')' but found Illegal escape sequence: \\c")));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ADD CONSTRAINT GROUPS CHECK(B > `A`))"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'GROUPS' while parsing")));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE T ADD CHECK(()"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Expecting ')' but found 'EOF'")));

  EXPECT_THAT(ParseDDLStatement(
                  "ALTER TABLE T ALTER CONSTRAINT col_a_gt_zero CHECK(A < 0);"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(ParseAnalyze, CanParseAnalyze) {
  EXPECT_THAT(ParseDDLStatement("ANALYZE"), IsOk());
}
}  // namespace

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
