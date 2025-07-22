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
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "common/feature_flags.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

namespace {

using absl::StatusCode;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

absl::StatusOr<DDLStatement> ParseDDLStatement(
    absl::string_view ddl,
    std::shared_ptr<const ProtoBundle> proto_bundle = nullptr) {
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
              StatusIs(StatusCode::kInvalidArgument));

  // Passes with backticks.
  EXPECT_THAT(ParseDDLStatement("CREATE DATABASE `mytestdb-1`"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_database { db_name: "mytestdb-1" }
              )pb")));
}

TEST(ParseCreateDatabase, CannotParseEmptyDatabaseName) {
  EXPECT_THAT(ParseDDLStatement("CREATE DATABASE"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterDatabase, ValidSetWitnessLocationToNonEmptyString) {
  absl::string_view ddl = R"(
    ALTER DATABASE db SET OPTIONS ( witness_location = 'us-east1' )
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(statement, test::EqualsProto(
                             R"pb(alter_database {
                                    set_options {
                                      options {
                                        option_name: "witness_location"
                                        string_value: "us-east1"
                                      }
                                    }
                                    db_name: "db"
                                  })pb"));
}

TEST(ParseAlterDatabase, ValidSetDefaultLeaderToNonEmptyString) {
  absl::string_view ddl = R"(
    ALTER DATABASE db SET OPTIONS ( default_leader = 'us-east1' )
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(statement, test::EqualsProto(
                             R"pb(alter_database {
                                    set_options {
                                      options {
                                        option_name: "default_leader"
                                        string_value: "us-east1"
                                      }
                                    }
                                    db_name: "db"
                                  })pb"));
}

TEST(ParseAlterDatabase, ValidSetVersionRetentionPeriodToNonEmptyString) {
  absl::string_view ddl = R"(
    ALTER DATABASE db SET OPTIONS (version_retention_period = '7d' )
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(statement, test::EqualsProto(
                             R"pb(alter_database {
                                    set_options {
                                      options {
                                        option_name: "version_retention_period"
                                        string_value: "7d"
                                      }
                                    }
                                    db_name: "db"
                                  })pb"));
}

TEST(ParseAlterDatabase, ValidSetVersionRetentionPeriodToNull) {
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (version_retention_period = NULL)
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(statement, test::EqualsProto(
                             R"pb(alter_database {
                                    set_options {
                                      options {
                                        option_name: "version_retention_period"
                                        null_value: true
                                      }
                                    }
                                    db_name: "db"
                                  })pb"));
}

TEST(ParseAlterDatabase, InvalidSetVersionRetentionPeriod) {
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (version_retention_period = 1)
  )";
  EXPECT_THAT(ParseDDLStatement(ddl),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unexpected value for option")));
}

TEST(ParseAlterDatabase, Invalid_NoOptionSet) {
  absl::string_view ddl = R"(
    ALTER DATABASE db SET OPTIONS ()
  )";
  EXPECT_THAT(ParseDDLStatement(ddl),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Encountered ')' while parsing: identifier")));
}

TEST(ParseAlterDatabase, SetDefaultSequenceKind) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_identity_columns = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (default_sequence_kind = 'bit_reversed_positive')
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(statement, test::EqualsProto(
                             R"pb(alter_database {
                                    set_options {
                                      options {
                                        option_name: "default_sequence_kind"
                                        string_value: "bit_reversed_positive"
                                      }
                                    }
                                    db_name: "db"
                                  })pb"));
}

TEST(ParseAlterDatabase, SetDefaultSequenceKindNull) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_identity_columns = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (default_sequence_kind = NULL)
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(statement, test::EqualsProto(
                             R"pb(alter_database {
                                    set_options {
                                      options {
                                        option_name: "default_sequence_kind"
                                        null_value: true
                                      }
                                    }
                                    db_name: "db"
                                  })pb"));
}

TEST(ParseAlterDatabase, InvalidDefaultSequenceKind) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_identity_columns = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (default_sequence_kind = 1)
  )";
  EXPECT_THAT(ParseDDLStatement(ddl),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unexpected value for option")));
}

TEST(ParseAlterDatabase, SetDefaultTimeZoneDisabled) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_default_time_zone = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (default_time_zone = 'UTC')
  )";
  EXPECT_THAT(ParseDDLStatement(ddl),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Option: default_time_zone is unknown")));
}

TEST(ParseAlterDatabase, SetDefaultTimeZone) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_default_time_zone = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (default_time_zone = 'UTC')
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(statement, test::EqualsProto(
                             R"pb(alter_database {
                                    set_options {
                                      options {
                                        option_name: "default_time_zone"
                                        string_value: "UTC"
                                      }
                                    }
                                    db_name: "db"
                                  })pb"));
}

TEST(ParseAlterDatabase, SetDefaultTimeZoneNull) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_default_time_zone = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (default_time_zone = NULL)
  )";
  DDLStatement statement;
  ZETASQL_EXPECT_OK(ParseDDLStatement(ddl, &statement));
  EXPECT_THAT(
      statement,
      test::EqualsProto(
          R"pb(alter_database {
                 set_options {
                   options { option_name: "default_time_zone" null_value: true }
                 }
                 db_name: "db"
               })pb"));
}

TEST(ParseAlterDatabase, InvalidDefaultTimeZone) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_default_time_zone = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  absl::string_view ddl = R"(
ALTER DATABASE db SET OPTIONS (default_time_zone = 1)
  )";
  EXPECT_THAT(ParseDDLStatement(ddl),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unexpected value for option")));
}

TEST(ParseAlterDatabase, Invalid_EmptyString) {
  absl::string_view ddl = R"(
    ALTER DATABASE db SET OPTIONS ( default_leader = '' )
  )";
  EXPECT_THAT(ParseDDLStatement(ddl),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Invalid string literal: ''")));
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
                    create_table { table_name: "Users" }
                  )pb")));
}

TEST(ParseCreateTable, CannotParseCreateTableWithoutName) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE (
                    ) PRIMARY KEY ()
                    )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseCreateTable, CannotParseCreateTableWithoutPrimaryKey) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL,
                      Name STRING(MAX)
                    )
                    )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Must specify either table or column PRIMARY KEY")));
}

TEST(ParseCreateTable, CanParseCreateTableWithPrimaryKeyOnAColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL PRIMARY KEY,
                      Name STRING(MAX)
                    )
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

TEST(ParseCreateTable, CannotParseCreateTablePrimaryKeyOnTwoColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL PRIMARY KEY,
                      Name STRING(MAX) PRIMARY KEY
                    )
                    )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Multiple columns declared as PRIMARY KEY")));
}

TEST(ParseCreateTable, CannotParseCreateTablePrimaryKeyInColumnAndInTable) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    CREATE TABLE Users (
                      UserId INT64 NOT NULL PRIMARY KEY,
                      Name STRING(MAX)
                    ) PRIMARY KEY (UserId)
                    )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Cannot specify both table and column PRIMARY KEY")));
}

TEST(ParseCreateTable, CannotParseAlterTableWithPrimaryKeyInAddColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE Users
                    ADD COLUMN UserId INT64 NOT NULL PRIMARY KEY
                    )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unexpected PRIMARY KEY clause")));
}

TEST(ParseCreateTable, CannotParseAlterTableWithPrimaryKeyInAlterColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE Users
                    ALTER COLUMN UserId INT64 NOT NULL PRIMARY KEY
                    )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Expecting 'EOF' but found 'PRIMARY'")));
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

TEST(ParseCreateTable, CanParseCreateNonParentInterleavedTableWithNoColumns) {
  // Note that the on_delete action is not specified and not set either.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Albums (
                    ) PRIMARY KEY (), INTERLEAVE IN Users
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Albums"
                      interleave_clause { table_name: "Users" type: IN }
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
     CanParseCreateNonParentInterleavedTableWithKeyAndNonKeyColumns) {
  // The clause ON DELETE CASCADE is accepted at the parser level, but should be
  // rejected at the schema validator level.
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                  CREATE TABLE Albums (
                    UserId INT64 NOT NULL,
                    AlbumId INT64 NOT NULL,
                    Name STRING(1024),
                    Description STRING(1024)
                  ) PRIMARY KEY (UserId, AlbumId),
                    INTERLEAVE IN Users ON DELETE CASCADE
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
              interleave_clause {
                table_name: "Users"
                type: IN
                on_delete: CASCADE
              }
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
     CanParseCreateNonParentInterleavedTableWithExplicitOnDeleteNoAction) {
  // The clause ON DELETE NO ACTION is accepted at the parser level, but should
  // be rejected at the schema validator level.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                 CREATE TABLE Albums (
                 ) PRIMARY KEY (), INTERLEAVE IN Users ON DELETE NO ACTION
               )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Albums"
                      interleave_clause {
                        table_name: "Users"
                        type: IN
                        on_delete: NO_ACTION
                      }
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

TEST(ParseCreateTable,
     CanParseCreateNonParentInterleavedTableWithImplicitOnDeleteNoAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
              CREATE TABLE Albums (
              ) PRIMARY KEY (), INTERLEAVE IN Users
            )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Albums"
                      interleave_clause { table_name: "Users" type: IN }
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

TEST(ParseCreateTable, CanParseCreateTableWithForeignKeyDeleteCascadeAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE T (
                      A INT64,
                      B STRING(MAX),
                      CONSTRAINT FK_UXY FOREIGN KEY (B, A)
                      REFERENCES U (X, Y) ON DELETE CASCADE,
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
                        constraint_name: "FK_UXY"
                        constrained_column_name: "B"
                        constrained_column_name: "A"
                        referenced_table_name: "U"
                        referenced_column_name: "X"
                        referenced_column_name: "Y"
                        enforced: true
                        on_delete: CASCADE
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseAlterTableAddForeignKeyWithDeleteCascadeAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD FOREIGN KEY (B, A)
                    REFERENCES U (X, Y) ON DELETE CASCADE
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
                          on_delete: CASCADE
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithForeignKeyDeleteNoAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE T (
                      A INT64,
                      B STRING(MAX),
                      CONSTRAINT FK_UXY FOREIGN KEY (B, A)
                      REFERENCES U (X, Y) ON DELETE NO ACTION,
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
                        constraint_name: "FK_UXY"
                        constrained_column_name: "B"
                        constrained_column_name: "A"
                        referenced_table_name: "U"
                        referenced_column_name: "X"
                        referenced_column_name: "Y"
                        enforced: true
                        on_delete: NO_ACTION
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithSynonym) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE People (
                      Name STRING(MAX),
                      SYNONYM (Folks)
                    ) PRIMARY KEY(Name)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "People"
                      column {
                        column_name: "Name"
                        type: STRING
                      }
                      primary_key {
                        key_name: "Name"
                      }
                      synonym: "Folks"
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseAlterTableAddForeignKeyWithDeleteNoAction) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD FOREIGN KEY (B, A)
                    REFERENCES U (X, Y) ON DELETE NO ACTION
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
                          on_delete: NO_ACTION
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseAlterTableAddForeignKeyEnforcementNoAction) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_fk_enforcement_option = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD FOREIGN KEY (B, A)
                    REFERENCES U (X, Y) ON DELETE NO ACTION NOT ENFORCED
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
                          enforced: false
                          on_delete: NO_ACTION
                        }
                      }
                    }
                  )pb")));
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE T ADD FOREIGN KEY (B, A)
                    REFERENCES U (X, Y) ENFORCED
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

  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE T ADD FOREIGN KEY (B, A)
    REFERENCES U (X, Y) ON DELETE CASCADE NOT ENFORCED
  )sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       R"(Error parsing Spanner DDL statement:
    ALTER TABLE T ADD FOREIGN KEY (B, A)
    REFERENCES U (X, Y) ON DELETE CASCADE NOT ENFORCED
   : ON DELETE actions are not supported for NOT ENFORCED foreign keys.)"));
}

TEST(ParseAlterTable, FlagGuardedCreateTableWithForeignKeyEnforcement) {
  absl::string_view ddl = R"sql(
    CREATE TABLE T (
      A INT64,
      B STRING(MAX),
      CONSTRAINT FK_UXY FOREIGN KEY (B, A)
      REFERENCES U (X, Y) ON DELETE CASCADE ENFORCED,
    ) PRIMARY KEY (A)
  )sql";
  EXPECT_THAT(ParseDDLStatement(
                  ddl),
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
                        constraint_name: "FK_UXY"
                        constrained_column_name: "B"
                        constrained_column_name: "A"
                        referenced_table_name: "U"
                        referenced_column_name: "X"
                        referenced_column_name: "Y"
                        enforced: true
                        on_delete: CASCADE
                      }
                    }
                  )pb")));

  EmulatorFeatureFlags::Flags flags;
  flags.enable_fk_enforcement_option = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  EXPECT_THAT(ParseDDLStatement(
                  ddl), StatusIs(absl::StatusCode::kInvalidArgument,
                       R"(Error parsing Spanner DDL statement:
    CREATE TABLE T (
      A INT64,
      B STRING(MAX),
      CONSTRAINT FK_UXY FOREIGN KEY (B, A)
      REFERENCES U (X, Y) ON DELETE CASCADE ENFORCED,
    ) PRIMARY KEY (A)
   : Foreign key enforcement is not supported.)"));
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

TEST(ParseCreateTable, CanParseCreateTableWithFloat32) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE T (
                      K INT64 NOT NULL,
                      FloatVal FLOAT32,
                      FloatArr ARRAY<FLOAT32>
                    ) PRIMARY KEY (K)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "T"
                      column { column_name: "K" type: INT64 not_null: true }
                      column { column_name: "FloatVal" type: FLOAT }
                      column {
                        column_name: "FloatArr"
                        type: ARRAY
                        array_subtype { type: FLOAT }
                      }
                      primary_key { key_name: "K" }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithTokenList) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE Albums (
      Id INT64 NOT NULL,
      Name STRING(MAX),
      Name_Token TOKENLIST,
    ) PRIMARY KEY (Id)
  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Albums"
                      column { column_name: "Id" type: INT64 not_null: true }
                      column { column_name: "Name" type: STRING }
                      column { column_name: "Name_Token" type: TOKENLIST }
                      primary_key { key_name: "Id" }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithVectorLength) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      K INT64,
      Arr ARRAY<FLOAT64>(vector_length=>10),
    ) PRIMARY KEY(K)
  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "T"
                      column { column_name: "K" type: INT64 }
                      column {
                        column_name: "Arr"
                        type: ARRAY
                        array_subtype { type: DOUBLE }
                        vector_length: 10
                      }
                      primary_key { key_name: "K" }
                    }
                  )pb")));
}

TEST(ParseCreateTable, CannotParseCreateTableWithInvalidColumnParameterName) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      K INT64,
      Arr ARRAY<FLOAT64>(invalid_param=>10),
    ) PRIMARY KEY(K)
  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseCreateTable, CannotParseCreateTableWithNegativeVectorLengthValue) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      K INT64,
      Arr ARRAY<FLOAT64>(vector_length=>-1),
    ) PRIMARY KEY(K)
  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseCreateTable, CannotParseCreateTableWithVectorLengthInNonArray) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      K INT64,
      Arr FLOAT64(vector_length=>10),
    ) PRIMARY KEY(K)
  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseCreateTable, CannotParseCreateTableWithMultipleVectorLength) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      K INT64,
      Arr ARRAY<FLOAT64>(vector_length=>10, vector_length=>20),
    ) PRIMARY KEY(K)
  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseCreateTable, CannotParseCreateTableWithVectorLengthInStructArray) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE T(
      K INT64,
      Arr STRUCT<arr ARRAY<INT64>(vector_length=>10)>,
    ) PRIMARY KEY(K)
  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument,
                       R"(Error parsing Spanner DDL statement:
    CREATE TABLE T(
      Key INT64,
      CreatedAt TIMESTAMP,
    ) PRIMARY KEY (Key), ROW DELETION POLICY (YOUNGER_THAN(CreatedAt, INTERVAL 7 DAY))
   : Only OLDER_THAN is supported.)"));
}

TEST(ParseCreateTable, CanParseCreateTableWithHiddenColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE Users (
                      Id INT64,
                      Name STRING(MAX) HIDDEN,
                    ) PRIMARY KEY (Id)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Users"
                      column { column_name: "Id" type: INT64 }
                      column { column_name: "Name" type: STRING hidden: true }
                      primary_key { key_name: "Id" }
                    }
                  )pb")));
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

TEST(ParseCreateIndex, CanUseWhereIsNotNull) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE INDEX Idx ON Users(UserId) WHERE UserId IS NOT NULL
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "Idx"
                      index_base_name: "Users"
                      key { key_name: "UserId" }
                      null_filtered_column: "UserId"
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanUseWhereIsNotNullMultipleColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE INDEX Idx ON Users(UserId, Name) WHERE UserId IS NOT NULL AND Name IS NOT NULL
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "Idx"
                      index_base_name: "Users"
                      key { key_name: "UserId" }
                      key { key_name: "Name" }
                      null_filtered_column: "UserId"
                      null_filtered_column: "Name"
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CanParseCreateIndexBasicWithLocalityGroupOption) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX GlobalAlbumsByName
                        ON Albums(Name) OPTIONS (locality_group = 'test_locality_group')
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "GlobalAlbumsByName"
                      index_base_name: "Albums"
                      key { key_name: "Name" }
                      null_filtered: true
                      set_options {
                        option_name: "locality_group"
                        string_value: "test_locality_group"
                      }
                    }
                  )pb")));
}

TEST(ParseCreateIndex,
     CanParseCreateIndexWithLocalityGroupOptionAndWhereClause) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE INDEX Idx ON Users(UserId, Name) WHERE UserId IS NOT NULL AND Name IS NOT NULL
                      OPTIONS (locality_group = 'test_locality_group')
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_index {
                      index_name: "Idx"
                      index_base_name: "Users"
                      key { key_name: "UserId" }
                      key { key_name: "Name" }
                      null_filtered_column: "UserId"
                      null_filtered_column: "Name"
                      set_options {
                        option_name: "locality_group"
                        string_value: "test_locality_group"
                      }
                    }
                  )pb")));
}

TEST(ParseCreateIndex, CannotParseCreateIndexWithOtherOption) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE NULL_FILTERED INDEX GlobalAlbumsByName
                        ON Albums(Name) OPTIONS (test_option = 'test_value')
                  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

// DROP TABLE

TEST(ParseDropTable, CanParseDropTableBasic) {
  EXPECT_THAT(
      ParseDDLStatement("DROP TABLE Users"),
      IsOkAndHolds(test::EqualsProto("drop_table { table_name: 'Users' }")));
}

TEST(ParseDropTable, CannotParseDropTableMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("DROP TABLE"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseDropTable, CannotParseDropTableInappropriateQuotes) {
  EXPECT_THAT(ParseDDLStatement("DROP `TABLE` Users"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseDropColumn, CannotParseDropColumnWithoutTable) {
  EXPECT_THAT(ParseDDLStatement("DROP COLUMN `TABLE`"),
              StatusIs(StatusCode::kInvalidArgument));
}

// DROP INDEX

TEST(ParseDropIndex, CanParseDropIndexBasic) {
  EXPECT_THAT(ParseDDLStatement("DROP INDEX LocalAlbumsByName"),
              IsOkAndHolds(test::EqualsProto(
                  "drop_index { index_name: 'LocalAlbumsByName' }")));
}

TEST(ParseDropIndex, CannotParseDropIndexMissingIndexName) {
  EXPECT_THAT(ParseDDLStatement("DROP INDEX"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseDropIndex, CannotParseDropIndexInappropriateQuotes) {
  EXPECT_THAT(ParseDDLStatement("DROP `INDEX` LocalAlbumsByName"),
              StatusIs(StatusCode::kInvalidArgument));
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

TEST(ParseAlterTable, CanParseAddTokenListColumn) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE Albums ADD COLUMN Name_TOKEN TOKENLIST
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "Albums"
                      add_column {
                        column { column_name: "Name_TOKEN" type: TOKENLIST }
                      }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseAddColumnNoColumnName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD COLUMN STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAddColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users ADD Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users ADD COLUMN Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAddColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ADD Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ADD COLUMN Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD Notes"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD COLUMN Notes"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ADD STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE Users ADD `COLUMN` Notes STRING(MAX)"),
      StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CanParseSetInterleaveInParentClauseImplicitNoAction) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER TABLE Albums SET INTERLEAVE IN PARENT Users )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Albums"
              set_interleave_clause {
                interleave_clause { table_name: "Users" on_delete: NO_ACTION }
              }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseSetInterleaveInParentClauseExplicitNoAction) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER TABLE Albums SET INTERLEAVE IN PARENT Users
                ON DELETE NO ACTION )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Albums"
              set_interleave_clause {
                interleave_clause { table_name: "Users" on_delete: NO_ACTION }
              }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseSetInterleaveInParentClauseOnDeleteCascade) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER TABLE Albums SET INTERLEAVE IN PARENT Users
                ON DELETE CASCADE )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Albums"
              set_interleave_clause {
                interleave_clause { table_name: "Users" on_delete: CASCADE }
              }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseSetInterleaveInClause) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(ALTER TABLE Albums SET INTERLEAVE IN Users )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            alter_table {
              table_name: "Albums"
              set_interleave_clause {
                interleave_clause { table_name: "Users" type: IN }
              }
            }
          )pb")));
}

TEST(ParseAlterTable, CanParseSetInterleaveInClauseExplicitNoAction) {
  // ON DELETE NO ACTION is accepted at parser level, but should be rejected at
  // schema level.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(ALTER TABLE Albums SET INTERLEAVE IN Users
                ON DELETE NO ACTION )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "Albums"
                      set_interleave_clause {
                        interleave_clause {
                          table_name: "Users"
                          type: IN
                          on_delete: NO_ACTION
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseSetInterleaveInClauseOnDeleteCascade) {
  // ON DELETE CASCADE is accepted at parser level, but should be rejected at
  // schema level.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(ALTER TABLE Albums SET INTERLEAVE IN Users
                ON DELETE CASCADE )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "Albums"
                      set_interleave_clause {
                        interleave_clause {
                          table_name: "Users"
                          type: IN
                          on_delete: CASCADE
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseAddSynonym) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE foo ADD SYNONYM bar
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "foo"
                      add_synonym { synonym: "bar" }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseDropSynonym) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE foo DROP SYNONYM bar
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "foo"
                      drop_synonym { synonym: "bar" }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CannotParseMalformedAddDropSynonym) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE foo ADD SYNONYM"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE foo ADD SYNONYM (bar)"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE foo SYNONYM bar"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE foo DROP SYNONYM (bar)"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CanParseRename) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE Users RENAME TO NewUsers
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "Users"
                      rename_to { name: "NewUsers" }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseRenameWithQuote) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE Users RENAME TO `TABLE`
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "Users"
                      rename_to { name: "TABLE" }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CanParseRenameWithSynonym) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    ALTER TABLE Users RENAME TO NewUsers, ADD SYNONYM Users
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_table {
                      table_name: "Users"
                      rename_to { name: "NewUsers" synonym: "Users" }
                    }
                  )pb")));
}

TEST(ParseAlterTable, CannotParseMalformedRename) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users RENAME NewUsers"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement(
                  "ALTER TABLE Users RENAME TO NewUsers ADD SYNONYM Users"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE Users RENAME TO NewUsers, SYNONYM "),
      StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseDropColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users DROP Notes"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users DROP COLUMN Notes"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseDropColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE DROP Notes"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE DROP COLUMN Notes"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users DROP `COLUMN` Notes"),
              StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingKeywordTable) {
  EXPECT_THAT(ParseDDLStatement("ALTER Users ALTER Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER Users ALTER COLUMN Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingTableName) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ALTER Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE ALTER COLUMN Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMissingColumnProperties) {
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER Notes"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER COLUMN Notes"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterTable, CannotParseAlterColumnMiscErrors) {
  // Missing column name.
  EXPECT_THAT(ParseDDLStatement("ALTER TABLE Users ALTER STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));

  // Multiple column names.
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE Users ALTER `COLUMN` Notes STRING(MAX)"),
      StatusIs(StatusCode::kInvalidArgument));

  // Missing table keyword.
  EXPECT_THAT(ParseDDLStatement("ALTER COLUMN Users.Notes STRING(MAX)"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(ParseAlterIndex, CanParseAddStoredColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
                    ALTER INDEX index ADD STORED COLUMN extra_column
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_index {
                      index_name: "index"
                      add_stored_column { column_name: "extra_column" }
                    }
                  )pb")));
}

TEST(ParseAlterIndex, CanParseSetLocalityGroupOption) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
                    ALTER INDEX index SET OPTIONS (locality_group = 'test_locality_group')
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_index {
                      index_name: "index"
                      set_options {
                        options {
                          option_name: "locality_group"
                          string_value: "test_locality_group"
                        }
                      }
                    }
                  )pb")));
}

TEST(ParseAlterIndex, CanParseDropStoredColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
                    ALTER INDEX index DROP STORED COLUMN extra_column
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    alter_index {
                      index_name: "index"
                      drop_stored_column: "extra_column"
                    }
                  )pb")));
}

TEST(ParseAlterIndex, CanNotParseUnknownAlterType) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
                    ALTER INDEX index UNKNOWN STORED COLUMN extra_column
                  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Syntax error on line 2, column 44: Expecting "
                                 "'EOF' but found '('")));
}

TEST(ParseRenameTable, CanParseRenameTable) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
                    RENAME TABLE Foo TO Bar
                  )sql"),
      IsOkAndHolds(test::EqualsProto(
          R"pb(
            rename_table { rename_op { from_name: "Foo" to_name: "Bar" } }
          )pb")));
}

TEST(ParseRenameTable, CanParseRenameTableChain) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    RENAME TABLE Bar TO Foobar, Foo TO Bar, Foobar TO Foo
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    rename_table {
                      rename_op { from_name: "Bar" to_name: "Foobar" }
                      rename_op { from_name: "Foo" to_name: "Bar" }
                      rename_op { from_name: "Foobar" to_name: "Foo" }
                    }
                  )pb")));
}

TEST(ParseRenameTable, CannotParseMalformedRenameTable) {
  EXPECT_THAT(ParseDDLStatement("RENAME TABLE Foo Bar"),
              StatusIs(StatusCode::kInvalidArgument));

  EXPECT_THAT(ParseDDLStatement("RENAME TABLE Bar TO Foo, Foo TO;"),
              StatusIs(StatusCode::kInvalidArgument));
}

// MISCELLANEOUS

TEST(Miscellaneous, CannotParseNonAsciiCharacters) {
  // The literal escape character is not considered a valid ascii character.
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE \x1b Users () PRIMARY KEY()
                  )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(Miscellaneous, CanParseExtraWhitespaceCharacters) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE   Users () PRIMARY KEY()
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table { table_name: "Users" }
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
              StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument));

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
              StatusIs(StatusCode::kInvalidArgument));
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
              StatusIs(StatusCode::kInvalidArgument));
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
      StatusIs(StatusCode::kInvalidArgument,
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
        StatusIs(StatusCode::kInvalidArgument,
                 HasSubstr("Encountered an unclosed triple quoted string")));
  }
}

TEST(ParseToken, CannotParseIllegalStringEscape) {
  EXPECT_THAT(
      ParseDDLStatement("\"\xc2\""),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered Structurally invalid UTF8 string")));
}

TEST(ParseToken, CannotParseIllegalBytesEscape) {
  EXPECT_THAT(
      ParseDDLStatement("b'''k\\u0030'''"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr(
              "Encountered Illegal escape sequence: Unicode escape sequence")));
}

class GeneratedColumns : public ::testing::Test {
 public:
  GeneratedColumns() : feature_flags_({}) {}

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
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
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
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
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
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST_F(ColumnDefaultValues, InvalidSetDefault) {
  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ALTER COLUMN D SET DEFAULT"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

class CheckConstraint : public ::testing::Test {
 public:
  CheckConstraint() : feature_flags_({}) {}

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
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'ALL' while parsing: column_type")));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ADD CHECK(B > '\\c')"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Expecting ')' but found Illegal escape sequence: \\c")));

  EXPECT_THAT(
      ParseDDLStatement("ALTER TABLE T ADD CONSTRAINT GROUPS CHECK(B > `A`))"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'GROUPS' while parsing")));

  EXPECT_THAT(ParseDDLStatement("ALTER TABLE T ADD CHECK(()"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Expecting ')' but found 'EOF'")));

  EXPECT_THAT(ParseDDLStatement(
                  "ALTER TABLE T ALTER CONSTRAINT col_a_gt_zero CHECK(A < 0);"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(ParseCreateProtoBundle, CanParseSingleProtoType) {
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  a.b.C"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_proto_bundle { insert_type { source_name: "a.b.C" } }
              )pb")));
}

TEST(ParseCreateProtoBundle, CanParseMultipleProtoTypes) {
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  a.b.C,"
                                "  package.name.User,"
                                "  package.name.Device,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_proto_bundle {
                  insert_type { source_name: "a.b.C" }
                  insert_type { source_name: "package.name.User" }
                  insert_type { source_name: "package.name.Device" }
                }
              )pb")));
}

TEST(ParseCreateProtoBundle, CanParseProtoTypesConflictingWithInbuiltTypes) {
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  BOOL,"
                                "  BYTES,"
                                "  DATE,"
                                "  FLOAT64,"
                                "  INT64,"
                                "  JSON,"
                                "  NUMERIC,"
                                "  STRING,"
                                "  TIMESTAMP,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_proto_bundle {
                  insert_type { source_name: "BOOL" }
                  insert_type { source_name: "BYTES" }
                  insert_type { source_name: "DATE" }
                  insert_type { source_name: "FLOAT64" }
                  insert_type { source_name: "INT64" }
                  insert_type { source_name: "JSON" }
                  insert_type { source_name: "NUMERIC" }
                  insert_type { source_name: "STRING" }
                  insert_type { source_name: "TIMESTAMP" }
                }
              )pb")));
}

TEST(ParseCreateProtoBundle,
     CanParseFloat32ProtoTypesConflictingWithInbuiltFloat32) {
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE (FLOAT32)"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_proto_bundle { insert_type { source_name: "FLOAT32" } }
              )pb")));
}

TEST(ParseDropProtoBundle, CanParseDDLStatement) {
  EXPECT_THAT(ParseDDLStatement("DROP PROTO BUNDLE"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_proto_bundle {}
              )pb")));
}

TEST(ParseAlterProtoBundle, CanParseInserts) {
  EXPECT_THAT(ParseDDLStatement("ALTER PROTO BUNDLE INSERT ("
                                "  a.b.C,"
                                "  package.name.User,"
                                "  package.name.Device,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_proto_bundle {
                  insert_type { source_name: "a.b.C" }
                  insert_type { source_name: "package.name.User" }
                  insert_type { source_name: "package.name.Device" }
                }
              )pb")));
}

TEST(ParseAlterProtoBundle, CanParseUpdates) {
  EXPECT_THAT(ParseDDLStatement("ALTER PROTO BUNDLE UPDATE ("
                                "  a.b.C,"
                                "  package.name.User,"
                                "  package.name.Device,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_proto_bundle {
                  update_type { source_name: "a.b.C" }
                  update_type { source_name: "package.name.User" }
                  update_type { source_name: "package.name.Device" }
                }
              )pb")));
}

TEST(ParseAlterProtoBundle, CanParseDeletes) {
  EXPECT_THAT(ParseDDLStatement("ALTER PROTO BUNDLE DELETE ("
                                "  a.b.C,"
                                "  package.name.User,"
                                "  package.name.Device,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_proto_bundle {
                  delete_type: "a.b.C"
                  delete_type: "package.name.User"
                  delete_type: "package.name.Device"
                }
              )pb")));
}

TEST(ParseAlterProtoBundle, CanParseMultipleOperations) {
  EXPECT_THAT(ParseDDLStatement("ALTER PROTO BUNDLE INSERT ("
                                "  a.b.C,"
                                ") UPDATE ("
                                "  package.name.User,"
                                ") DELETE ("
                                "  package.name.Device,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_proto_bundle {
                  insert_type { source_name: "a.b.C" }
                  update_type { source_name: "package.name.User" }
                  delete_type: "package.name.Device"
                }
              )pb")));
}

TEST(ParseProtoBundleStatements, FailsParsingInvalidIdentifiers) {
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  create.foo"
                                ")"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  'create'.foo"
                                ")"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  ''create'''.foo"
                                ")"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  foo-.bar"
                                ")"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  .foo.bar"
                                ")"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  foo,.bar"
                                ")"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(ParseProtoBundleStatements, CanParseEscapedIdentifiers) {
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  `create`.foo,"
                                "  `create.foo`,"
                                "  foo.`create`,"
                                "  foo.`create`.bar,"
                                "  foo.`create`.`table`,"
                                "  foo.`create`.`create`,"
                                "  foo.`create.create`,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_proto_bundle {
                  insert_type { source_name: "create.foo" }
                  insert_type { source_name: "create.foo" }
                  insert_type { source_name: "foo.create" }
                  insert_type { source_name: "foo.create.bar" }
                  insert_type { source_name: "foo.create.table" }
                  insert_type { source_name: "foo.create.create" }
                  insert_type { source_name: "foo.create.create" }
                }
              )pb")));
  EXPECT_THAT(ParseDDLStatement("CREATE PROTO BUNDLE ("
                                "  foo_.bar,"
                                "  `foo-`.bar,"
                                "  `foo,`.bar,"
                                ")"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_proto_bundle {
                  insert_type { source_name: "foo_.bar" }
                  insert_type { source_name: "foo-.bar" }
                  insert_type { source_name: "foo,.bar" }
                }
              )pb")));
}

class ProtoAndEnumColumns : public ::testing::Test {
 public:
  ProtoAndEnumColumns() = default;

  // Creates FileDescriptorProto for provided proto types.
  google::protobuf::FileDescriptorProto GenerateProtoDescriptor(std::string package,
                                                      std::string type_name) {
    return PARSE_TEXT_PROTO(absl::Substitute(R"pb(
                                               syntax: "proto2"
                                               name: "proto.$0.$1"
                                               package: "$0"
                                               message_type { name: "$1" }
                                             )pb",
                                             package, type_name));
  }

  // Creates FileDescriptorProto for provided enum types.
  google::protobuf::FileDescriptorProto GenerateEnumDescriptor(std::string package,
                                                     std::string type_name) {
    return PARSE_TEXT_PROTO(
        absl::Substitute(R"pb(
                           syntax: "proto2"
                           name: "enum.$0.$1"
                           package: "$0"
                           enum_type {
                             name: "$1"
                             value: { name: "UNSPECIFIED" number: 0 }
                           }
                         )pb",
                         package, type_name));
  }

  // Creates proto descriptors set as string for given proto and enum types.
  std::string GenerateDescriptorBytesAsString(
      std::string package, std::vector<std::string> &proto_types,
      std::vector<std::string> &enum_types) {
    google::protobuf::FileDescriptorSet file_descriptor_set;
    for (auto &type : proto_types) {
      *file_descriptor_set.add_file() = GenerateProtoDescriptor(package, type);
    }
    for (auto &type : enum_types) {
      *file_descriptor_set.add_file() = GenerateEnumDescriptor(package, type);
    }
    return file_descriptor_set.SerializeAsString();
  }

  // Populates the proto bundle for provided proto/enum types.
  absl::StatusOr<std::shared_ptr<const ProtoBundle>> SetUpBundle(
      std::string package, std::vector<std::string> &proto_types,
      std::vector<std::string> &enum_types) {
    auto insert_proto_types = std::vector<std::string>{};
    for (auto &type : proto_types) {
      std::string fullname = (!package.empty()) ? package + "." + type : type;
      insert_proto_types.push_back(fullname);
    }
    for (auto &type : enum_types) {
      std::string fullname = (!package.empty()) ? package + "." + type : type;
      insert_proto_types.push_back(fullname);
    }
    ZETASQL_ASSIGN_OR_RETURN(auto builder,
                     ProtoBundle::Builder::New(GenerateDescriptorBytesAsString(
                         package, proto_types, enum_types)));
    ZETASQL_RETURN_IF_ERROR(builder->InsertTypes(insert_proto_types));
    ZETASQL_ASSIGN_OR_RETURN(auto proto_bundle, builder->Build());
    return proto_bundle;
  }
};

TEST_F(ProtoAndEnumColumns, CanParseBasicCreateTable) {
  std::vector<std::string> proto_types{"UserInfo"};
  std::vector<std::string> enum_types{"UserState"};
  std::string package = "customer.app";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));

  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE Users(
      Id    INT64 NOT NULL,
      User customer.app.UserInfo,
      State customer.app.UserState
    ) PRIMARY KEY (Id)
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "Users"
                  column { column_name: "Id" type: INT64 not_null: true }
                  column {
                    column_name: "User"
                    type: NONE
                    proto_type_name: "customer.app.UserInfo"
                  }
                  column {
                    column_name: "State"
                    type: NONE
                    proto_type_name: "customer.app.UserState"
                  }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns, CanParseCreateTableWithNoPackageProtoPath) {
  std::vector<std::string> proto_types{"UserInfo"};
  std::vector<std::string> enum_types{"UserState"};
  std::string package = "";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE TABLE Users(
      Id    INT64 NOT NULL,
      User  UserInfo,
      State UserState
    ) PRIMARY KEY (Id)
  )sql",
                        proto_bundle),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_table {
          table_name: "Users"
          column { column_name: "Id" type: INT64 not_null: true }
          column { column_name: "User" type: NONE proto_type_name: "UserInfo" }
          column {
            column_name: "State"
            type: NONE
            proto_type_name: "UserState"
          }
          primary_key { key_name: "Id" }
        }
      )pb")));
}

TEST_F(ProtoAndEnumColumns, CanParseCreateTableWithArrayColumns) {
  std::vector<std::string> proto_types{"UserInfo"};
  std::vector<std::string> enum_types{"UserState"};
  std::string package = "customer.app";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE Users(
      Id    INT64 NOT NULL,
      Users  ARRAY<customer.app.UserInfo>,
      States ARRAY<customer.app.UserState>
    ) PRIMARY KEY (Id)
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "Users"
                  column { column_name: "Id" type: INT64 not_null: true }
                  column {
                    column_name: "Users"
                    type: ARRAY
                    array_subtype {
                      type: NONE
                      proto_type_name: "customer.app.UserInfo"
                    }
                  }
                  column {
                    column_name: "States"
                    type: ARRAY
                    array_subtype {
                      type: NONE
                      proto_type_name: "customer.app.UserState"
                    }
                  }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns, FailsParsingInvalidCreateTableSyntax) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE Users(
      Id    INT64 NOT NULL,
      User  PROTO<customer.app.UserInfo>(MAX),
      State PROTO<customer.app.UserState>(MAX)
    ) PRIMARY KEY (Id)
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE Users(
      Id    INT64 NOT NULL,
      User  PROTO<customer.app.UserInfo>(MAX),
      State PROTO<customer.app.UserState>(MAX)
    ) PRIMARY KEY (Id)
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST_F(ProtoAndEnumColumns, CanParseBasicAlterTableAddColumn) {
  std::vector<std::string> proto_types{};
  std::vector<std::string> enum_types{"UserState"};
  std::string package = "customer.app";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD COLUMN State customer.app.UserState
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  add_column {
                    column {
                      column_name: "State"
                      type: NONE
                      proto_type_name: "customer.app.UserState"
                    }
                  }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns, CanParseAlterTableAddColumnWithoutColumn) {
  std::vector<std::string> proto_types{};
  std::vector<std::string> enum_types{"UserState"};
  std::string package = "customer.app";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD State customer.app.UserState
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  add_column {
                    column {
                      column_name: "State"
                      type: NONE
                      proto_type_name: "customer.app.UserState"
                    }
                  }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns,
       FailsParsingAlterTableAddColumnWithAmbiguousColumnName) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD COLUMN customer.app.UserState
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST_F(ProtoAndEnumColumns,
       CanParseAlterTableAddColumnWithAmbiguousColumnName) {
  std::vector<std::string> enum_types{"UserState"};
  std::vector<std::string> proto_types{"UserInfo"};
  std::string package = "customer.app";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD `COLUMN` customer.app.UserState
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  add_column {
                    column {
                      column_name: "COLUMN"
                      type: NONE
                      proto_type_name: "customer.app.UserState"
                    }
                  }
                }
              )pb")));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD COLUMN COLUMN customer.app.UserInfo
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  add_column {
                    column {
                      column_name: "COLUMN"
                      type: NONE
                      proto_type_name: "customer.app.UserInfo"
                    }
                  }
                }
              )pb")));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD COLUMN `COLUMN` customer.app.UserState
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  add_column {
                    column {
                      column_name: "COLUMN"
                      type: NONE
                      proto_type_name: "customer.app.UserState"
                    }
                  }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns, CanParseBasicAlterColumn) {
  std::vector<std::string> proto_types{};
  std::vector<std::string> enum_types{"UserState"};
  std::string package = "customer.app";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER COLUMN State customer.app.UserState
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  alter_column {
                    column {
                      column_name: "State"
                      type: NONE
                      proto_type_name: "customer.app.UserState"
                    }
                  }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns, FailsToParseAlterColumnWithAmbiguousColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER COLUMN customer.app.UserState
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));

  // A column called COLUMN with the type COLUMN.
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER COLUMN COLUMN
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST_F(ProtoAndEnumColumns, CanParseAlterColumnWithAmbiguousColumn) {
  std::vector<std::string> proto_types{"UserInfo"};
  std::vector<std::string> enum_types{"UserState"};
  std::string package = "customer.app";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER `COLUMN` customer.app.UserInfo
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  alter_column {
                    column {
                      column_name: "COLUMN"
                      type: NONE
                      proto_type_name: "customer.app.UserInfo"
                    }
                  }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns, CanParseAlterColumnWithAmbiguousColumnNamedColumn) {
  // A column with the name COLUMN with type COLUMN.
  std::vector<std::string> proto_types{"COLUMN"};
  std::vector<std::string> enum_types{};
  std::string package = "";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER `COLUMN` `COLUMN`
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  alter_column {
                    column {
                      column_name: "COLUMN"
                      type: NONE
                      proto_type_name: "COLUMN"
                    }
                  }
                }
              )pb")));
}

TEST_F(ProtoAndEnumColumns, FailsToParseAlterColumnWithSetType) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER COLUMN SET
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST_F(ProtoAndEnumColumns, FailsToParseAlterColumnWithSetNotNull) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER COLUMN State SET NOT NULL
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST_F(ProtoAndEnumColumns, FailsToParseAlterColumnWithDropNotNull) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER COLUMN State DROP NOT NULL
  )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST_F(ProtoAndEnumColumns, CanParseAlterTableWithKeywordsAsTypes) {
  // Tests for wierd but legal proto columns due to pseudo reserved words.
  std::vector<std::string> proto_types{"DELETION", "FOREIGN", "KEY", "CHECK",
                                       "DROP"};
  std::vector<std::string> enum_types{};
  std::string package = "";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle,
                       SetUpBundle(package, proto_types, enum_types));
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD ROW DELETION
  )sql",
                        proto_bundle),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_table {
          table_name: "Users"
          add_column {
            column { column_name: "ROW" type: NONE proto_type_name: "DELETION" }
          }
        }
      )pb")));
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD FOREIGN KEY
  )sql",
                        proto_bundle),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_table {
          table_name: "Users"
          add_column {
            column { column_name: "FOREIGN" type: NONE proto_type_name: "KEY" }
          }
        }
      )pb")));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD CONSTRAINT FOREIGN
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  add_column {
                    column {
                      column_name: "CONSTRAINT"
                      type: NONE
                      proto_type_name: "FOREIGN"
                    }
                  }
                }
              )pb")));
  EXPECT_THAT(ParseDDLStatement(R"sql(
    ALTER TABLE Users ADD CONSTRAINT CHECK
  )sql",
                                proto_bundle),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "Users"
                  add_column {
                    column {
                      column_name: "CONSTRAINT"
                      type: NONE
                      proto_type_name: "CHECK"
                    }
                  }
                }
              )pb")));
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    ALTER TABLE Users ALTER State DROP
  )sql",
                        proto_bundle),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_table {
          table_name: "Users"
          alter_column {
            column { column_name: "State" type: NONE proto_type_name: "DROP" }
          }
        }
      )pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamForAll) {
  EXPECT_THAT(ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      ALL)sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStream"
                  for_clause { all: true }
                })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamForExplicitEntireTable) {
  EXPECT_THAT(ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      TestTable)sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStream"
                  for_clause {
                    tracked_tables {
                      table_entry { table_name: "TestTable" all_columns: true }
                    }
                  }
                })pb")));
}

TEST(CreatePlacement, CanParseCreatePlacement) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE PLACEMENT p1 OPTIONS (instance_partition = 'us-central1'))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_placement {
          placement_name: "p1"
          set_options {
            option_name: "instance_partition"
            string_value: "us-central1"
          }
        })pb")));
}

TEST(CreatePlacement, CanParseCreatePlacementWithDefaultLeader) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE PLACEMENT p1 OPTIONS (instance_partition = 'us-central1', default_leader = 'us-central1'))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_placement {
          placement_name: "p1"
          set_options {
            option_name: "instance_partition"
            string_value: "us-central1"
          }
          set_options {
            option_name: "default_leader"
            string_value: "us-central1"
          }
        })pb")));
}

TEST(CreatePlacement, CanParseAlterPlacement) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER PLACEMENT p1 SET OPTIONS (instance_partition = 'us-central1'))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_placement {
          placement_name: "p1"
          set_options {
            option_name: "instance_partition"
            string_value: "us-central1"
          }
        })pb")));
}

TEST(CreatePlacement, CanParseAlterPlacementWithDefaultLeader) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER PLACEMENT p1 SET OPTIONS (instance_partition = 'us-central1', default_leader = 'us-central1'))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_placement {
          placement_name: "p1"
          set_options {
            option_name: "instance_partition"
            string_value: "us-central1"
          }
          set_options {
            option_name: "default_leader"
            string_value: "us-central1"
          }
        })pb")));
}

TEST(CreatePlacement, CanParseDropPlacement) {
  EXPECT_THAT(ParseDDLStatement(R"sql(DROP PLACEMENT p1)sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_placement { placement_name: "p1" })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamForExplicitTablePkOnly) {
  EXPECT_THAT(ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      TestTable())sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStream"
                  for_clause {
                    tracked_tables {
                      table_entry {
                        table_name: "TestTable"
                        tracked_columns {}
                      }
                    }
                  }
                })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamForExplicitColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      TestTable(TestCol))sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStream"
                  for_clause {
                    tracked_tables {
                      table_entry {
                        table_name: "TestTable"
                        tracked_columns { column_name: "TestCol" }
                      }
                    }
                  }
                })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamForTableAndExplicitColumns) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamTableAndExplicitColumns FOR Users, Albums (Name, Description), Singers())sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "ChangeStreamTableAndExplicitColumns"
          for_clause {
            tracked_tables {
              table_entry { table_name: "Users" all_columns: true }
              table_entry {
                table_name: "Albums"
                tracked_columns {
                  column_name: "Name"
                  column_name: "Description"
                }
              }
              table_entry {
                table_name: "Singers"
                tracked_columns {}
              }
            }
          }
        })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamForTableNamedQuoteALL) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(CREATE CHANGE STREAM ChangeStreamForTableNamedQuoteALL
  FOR `ALL`)sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStreamForTableNamedQuoteALL"
                  for_clause {
                    tracked_tables {
                      table_entry { table_name: "ALL" all_columns: true }
                    }
                  }
                })pb")));
}

TEST(CreateChangeStream,
     CanParseCreateChangeStreamForTableNamedQuoteALLWithColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamForTableNamedQuoteALLWithColumn
  FOR `ALL`(SomeColumn))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "ChangeStreamForTableNamedQuoteALLWithColumn"
          for_clause {
            tracked_tables {
              table_entry {
                table_name: "ALL"
                tracked_columns { column_name: "SomeColumn" }
              }
            }
          }
        })pb")));
}

TEST(CreateChangeStream,
     CanParseCreateChangeStreamForTableNamedQuoteALLWithQuoteALLColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamForTableNamedQuoteALLWithQuoteALLColumn
  FOR `ALL`(`ALL`))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "ChangeStreamForTableNamedQuoteALLWithQuoteALLColumn"
          for_clause {
            tracked_tables {
              table_entry {
                table_name: "ALL"
                tracked_columns { column_name: "ALL" }
              }
            }
          }
        })pb")));
}

TEST(CreateChangeStream, ChangeStreamErrorForTableNamedALLWithColumn) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForTableNamedALLWithColumn
  FOR ALL(SomeColumn))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorForTableNamedALLWithPK) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForTableNamedALLWithPK FOR ALL())sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorForUsersAndALL) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForUsersAndALL FOR Users, ALL)sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorForUsersALLAlbums) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForUsersALLAlbums FOR Users(), ALL, Albums())sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorForUsersColumnALL) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForUsersColumnALL FOR Users(ALL))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamNoForClause) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(CREATE CHANGE STREAM ChangeStreamNoForClause)sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStreamNoForClause"
                })pb")));
}

TEST(CreateChangeStream, ChangeStreamErrorForClauseNothingFollowing) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing FOR)sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamMassivelyQuoted) {
  EXPECT_THAT(ParseDDLStatement(R"sql(CREATE CHANGE STREAM `ChangeStreamQuoted`
  FOR `Users`, `Albums`(`Name`), `Singers`())sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStreamQuoted"
                  for_clause {
                    tracked_tables {
                      table_entry { table_name: "Users" all_columns: true }
                      table_entry {
                        table_name: "Albums"
                        tracked_columns { column_name: "Name" }
                      }
                      table_entry {
                        table_name: "Singers"
                        tracked_columns {}
                      }
                    }
                  }
                })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamRepeatedTableColumns) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(CREATE CHANGE STREAM ChangeStreamRepeatedTableColumns
  FOR Users, Users, Albums(Name), Albums(Name, Description))sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStreamRepeatedTableColumns"
                  for_clause {
                    tracked_tables {
                      table_entry { table_name: "Users" all_columns: true }
                      table_entry { table_name: "Users" all_columns: true }
                      table_entry {
                        table_name: "Albums"
                        tracked_columns { column_name: "Name" }
                      }
                      table_entry {
                        table_name: "Albums"
                        tracked_columns {
                          column_name: "Name"
                          column_name: "Description"
                        }
                      }
                    }
                  }
                })pb")));
}

TEST(CreateChangeStream,
     CanParseCreateChangeStreamSetOptionsDataRetentionPeriod) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      ALL OPTIONS ( retention_period = '168h' ))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "ChangeStream"
          for_clause { all: true }
          set_options { option_name: "retention_period" string_value: "168h" }
        })pb")));
}

TEST(CreateChangeStream,
     CanParseCreateChangeStreamSetOptionsDataRetentionPeriodNull) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      ALL OPTIONS ( retention_period = null ))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "ChangeStream"
          for_clause { all: true }
          set_options { option_name: "retention_period" null_value: true }
        })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamSetOptionsValueCaptureType) {
  EXPECT_THAT(ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      ALL OPTIONS ( value_capture_type = 'NEW_ROW' ))sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_change_stream {
                  change_stream_name: "ChangeStream"
                  for_clause { all: true }
                  set_options {
                    option_name: "value_capture_type"
                    string_value: "NEW_ROW"
                  }
                })pb")));
}

TEST(CreateChangeStream,
     CanParseCreateChangeStreamSetOptionsValueCaptureTypeNull) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      ALL OPTIONS (value_capture_type = NULL))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "ChangeStream"
          for_clause { all: true }
          set_options { option_name: "value_capture_type" null_value: true }
        })pb")));
}

TEST(CreateChangeStream,
     CanParseCreateChangeStreamSetOptionsRetentionPeriodAndValueCaptureType) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM cs OPTIONS (retention_period='7d',value_capture_type='OLD_AND_NEW_VALUES'))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "cs"
          set_options { option_name: "retention_period" string_value: "7d" }
          set_options {
            option_name: "value_capture_type"
            string_value: "OLD_AND_NEW_VALUES"
          }
        })pb")));
}

TEST(CreateChangeStream,
     CanParseCreateChangeStreamSetValueCaptureTypeAndOptionsRetentionPeriod) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM cs OPTIONS (value_capture_type='OLD_AND_NEW_VALUES', retention_period='7d'))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "cs"
          set_options {
            option_name: "value_capture_type"
            string_value: "OLD_AND_NEW_VALUES"
          }
          set_options { option_name: "retention_period" string_value: "7d" }
        })pb")));
}

TEST(CreateChangeStream, CanParseCreateChangeStreamSetBooleanOptions) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(CREATE CHANGE STREAM ChangeStream FOR
      ALL OPTIONS ( exclude_insert = true, exclude_update=false, exclude_delete=true, exclude_ttl_deletes=null ))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_change_stream {
          change_stream_name: "ChangeStream"
          for_clause { all: true }
          set_options { option_name: "exclude_insert" bool_value: true }
          set_options { option_name: "exclude_update" bool_value: false }
          set_options { option_name: "exclude_delete" bool_value: true }
          set_options { option_name: "exclude_ttl_deletes" null_value: true }
        })pb")));
}

TEST(CreateChangeStream, ChangeStreamErrorEmptyOptions) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS ())sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorDuplicateOptions) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (retention_period = '7d', retention_period = '7d'))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorInvalidOptionsSyntax) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing SET OPTIONS (retention_period = '7d'))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorUnsupportedOptionName) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (allow_commit_timestamp = true))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateChangeStream, ChangeStreamErrorInvalidOptionType) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (retention_period = 1))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (retention_period = true))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (retention_period = ['list']))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (retention_period = [('key','val')]))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (value_capture_type = -1))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (value_capture_type = false))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (value_capture_type = ['list']))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (value_capture_type = [('key','val')]))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));

  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (exclude_insert = "false"))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));

  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (exclude_update = ['true']))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));

  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (exclude_delete = 1))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));

  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(CREATE CHANGE STREAM ChangeStreamErrorForClauseNothingFollowing OPTIONS (exclude_ttl_deletes = "null"))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(AlterChangeStream, CanParseAlterChangeStreamValidSetForClause) {
  EXPECT_THAT(ParseDDLStatement(R"sql(ALTER CHANGE STREAM cs SET FOR ALL)sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_change_stream {
                  change_stream_name: "cs"
                  set_for_clause { all: true }
                })pb")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET FOR Users, Albums (Name, Description), Singers())sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_change_stream {
          change_stream_name: "cs"
          set_for_clause {
            tracked_tables {
              table_entry { table_name: "Users" all_columns: true }
              table_entry {
                table_name: "Albums"
                tracked_columns {
                  column_name: "Name"
                  column_name: "Description"
                }
              }
              table_entry {
                table_name: "Singers"
                tracked_columns {}
              }
            }
          }
        })pb")));
}

TEST(AlterChangeStream,
     CanParseAlterChangeStreamSetOptionsDataRetentionPeriod) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM ChangeStream SET OPTIONS ( retention_period = '7d' ))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_change_stream {
          change_stream_name: "ChangeStream"
          set_options {
            options { option_name: "retention_period" string_value: "7d" }
          }
        })pb")));
}

TEST(AlterChangeStream, CanParseAlterChangeStreamSetOptionsValueCaptureType) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM ChangeStream SET OPTIONS ( value_capture_type = 'NEW_VALUES' ))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_change_stream {
          change_stream_name: "ChangeStream"
          set_options {
            options {
              option_name: "value_capture_type"
              string_value: "NEW_VALUES"
            }
          }
        })pb")));
}

TEST(AlterChangeStream,
     CanParseAlterChangeStreamSetOptionsRetentionPeriodAndValueCaptureType) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (
            retention_period = '7d',
            value_capture_type = 'OLD_AND_NEW_VALUES'
          ))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_change_stream {
          change_stream_name: "cs"
          set_options {
            options { option_name: "retention_period" string_value: "7d" }
            options {
              option_name: "value_capture_type"
              string_value: "OLD_AND_NEW_VALUES"
            }
          }
        })pb")));
}

TEST(AlterChangeStream, CanParseAlterChangeStreamSetOptionsNull) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (value_capture_type = NULL, retention_period = NULL))sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_change_stream {
          change_stream_name: "cs"
          set_options {
            options { option_name: "value_capture_type" null_value: true }
            options { option_name: "retention_period" null_value: true }
          }
        })pb")));
}

TEST(AlterChangeStream, CanParseAlterChangeStreamSuspend) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(ALTER CHANGE STREAM ChangeStream DROP FOR ALL )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_change_stream {
                  change_stream_name: "ChangeStream"
                  drop_for_clause { all: true }
                })pb")));
}

TEST(DropChangeStream, MissingAlterAction) {
  EXPECT_THAT(ParseDDLStatement(R"sql(ALTER CHANGE STREAM cs)sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, MissingChangeStreamName) {
  EXPECT_THAT(ParseDDLStatement(R"sql(ALTER CHANGE STREAM SET FOR ALL)sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM SET OPTIONS (retention_period = '7d'))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, MissingKeyWordSet) {
  EXPECT_THAT(ParseDDLStatement(R"sql(ALTER CHANGE STREAM cs FOR ALL)sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs OPTIONS (retention_period = '7d'))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, InvalidForClause) {
  EXPECT_THAT(ParseDDLStatement(R"sql(ALTER CHANGE STREAM cs SET FOR)sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(R"sql(ALTER CHANGE STREAM cs SET FOR ALL())sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(R"sql(ALTER CHANGE STREAM cs SET FOR ALL, Users)sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, EmptyOptions) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(ALTER CHANGE STREAM cs SET OPTIONS ())sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, DuplicateOptions) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (retention_period = '7d', retention_period = '5d'))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, UnsupportedOptionName) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (allow_commit_timestamp = true))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, InvalidOptionType) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (retention_period = 1))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (retention_period = true))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (value_capture_type = -1))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(ALTER CHANGE STREAM cs SET OPTIONS (value_capture_type = false))sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(DropChangeStream, CanParseDropChangeStream) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(DROP CHANGE STREAM ChangeStream)sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        drop_change_stream { change_stream_name: "ChangeStream" })pb")));
}

TEST(DropChangeStream, ErrorParseDropChangeStreams) {
  EXPECT_THAT(ParseDDLStatement(R"sql(DROP CHANGE STREAM)sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement(R"sql(DROP `CHANGE STREAM`)sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
  EXPECT_THAT(ParseDDLStatement(R"sql(DROP `CHANGE` `STREAM`)sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateSearchIndex, CanParseDDLStatement) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
          CREATE SEARCH INDEX SearchIndex ON Base(Token1)
          )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_search_index {
          index_name: "SearchIndex"
          index_base_name: "Base"
          token_column_definition { token_column { key_name: "Token1" } }
        })pb")));
}

TEST(CreateSearchIndex, CanParseDDLStatementWithOneOption) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
          CREATE SEARCH INDEX SearchIndex ON Base(Token1)
          OPTIONS (sort_order_sharding = true)
          )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_search_index {
          index_name: "SearchIndex"
          index_base_name: "Base"
          token_column_definition { token_column { key_name: "Token1" } }
          set_options { option_name: "sort_order_sharding" bool_value: true }
        })pb")));
}

TEST(CreateSearchIndex, CanParseDDLStatementWithTwoOptions) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
          CREATE SEARCH INDEX SearchIndex ON Base(Token1)
          OPTIONS (sort_order_sharding = true, disable_automatic_uid_column = false)
          )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_search_index {
          index_name: "SearchIndex"
          index_base_name: "Base"
          token_column_definition { token_column { key_name: "Token1" } }
          set_options { option_name: "sort_order_sharding" bool_value: true }
          set_options {
            option_name: "disable_automatic_uid_column"
            bool_value: false
          }
        })pb")));
}

TEST(CreateSearchIndex, CannotParseDDLStatementWithDuplicateOptions) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
          CREATE SEARCH INDEX SearchIndex ON Base(Token1)
          OPTIONS (sort_order_sharding = true, disable_automatic_uid_column = false,
                    sort_order_sharding = false)
          )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(CreateSearchIndex, CannotParseDDLStatementWithUnsupportedOptions) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
          CREATE SEARCH INDEX SearchIndex ON Base(Token1)
          OPTIONS (some_invalid_name = true)
          )sql"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(CreateSearchIndex, CanParseDDLStatementWithWhereIsNotNull) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
          CREATE SEARCH INDEX SearchIndex ON Base(Token1)
          ORDER BY Sort1, Sort2
          WHERE Sort1 IS NOT NULL
            AND Sort2 IS NOT NULL
          OPTIONS (sort_order_sharding = false)
          )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_search_index {
          index_name: "SearchIndex"
          index_base_name: "Base"
          token_column_definition { token_column { key_name: "Token1" } }
          order_by { key_name: "Sort1" }
          order_by { key_name: "Sort2" }
          null_filtered_column: "Sort1"
          null_filtered_column: "Sort2"
          set_options { option_name: "sort_order_sharding" bool_value: false }
        }
      )pb")));
}

TEST(CreateSearchIndex, CanParseComplicatedDDLStatement) {
  EXPECT_THAT(
      ParseDDLStatement(
          R"sql(
            CREATE SEARCH INDEX SearchIndex ON
            Base(Token1, Token2) STORING(Stored1, Stored2)
            PARTITION BY Partition1, Partition2
            ORDER BY Sort DESC, uid,
            INTERLEAVE IN Dir)sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_search_index {
          index_name: "SearchIndex"
          index_base_name: "Base"
          token_column_definition { token_column { key_name: "Token1" } }
          token_column_definition { token_column { key_name: "Token2" } }
          partition_by { key_name: "Partition1" }
          partition_by { key_name: "Partition2" }
          order_by { key_name: "Sort" order: DESC }
          order_by { key_name: "uid" }
          interleave_in_table: "Dir"
          stored_column_definition { name: "Stored1" }
          stored_column_definition { name: "Stored2" }
        }
      )pb")));
}

TEST(ParseDropSearchIndex, CanParseDropSearchIndexBasic) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
            DROP SEARCH INDEX SearchIndex
              )sql"),
              IsOkAndHolds(test::EqualsProto(
                  "drop_search_index { index_name: 'SearchIndex' }")));
}

TEST(ParseDropSearchIndex, CannotParseDropSearchIndexMissingIndexName) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
            DROP SEARCH INDEX
              )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(ParseDropSearchIndex, CannotParseDropSearchIndexInappropriateQuotes) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
            DROP `SEARCH INDEX` SearchIndex
              )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Error parsing Spanner DDL statement")));
}

TEST(CreateSequence, SequenceKindInSingleQuotes) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = 'bit_reversed_positive' )
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_sequence {
                  sequence_name: "seq"
                  set_options {
                    option_name: "sequence_kind"
                    string_value: "bit_reversed_positive"
                  }
                }
              )pb")));
}

TEST(CreateSequence, WithIfNotExists) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE IF NOT EXISTS seq OPTIONS (
        sequence_kind = "bit_reversed_positive" )
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_sequence {
                  sequence_name: "seq"
                  set_options {
                    option_name: "sequence_kind"
                    string_value: "bit_reversed_positive"
                  }
                  existence_modifier: IF_NOT_EXISTS
                }
              )pb")));
}

TEST(CreateSequence, SequenceKindInDoubleQuotes) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = "bit_reversed_positive" )
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_sequence {
                  sequence_name: "seq"
                  set_options {
                    option_name: "sequence_kind"
                    string_value: "bit_reversed_positive"
                  }
                }
              )pb")));
}

TEST(CreateSequence, WithNullOptions) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        skip_range_min = NULL,
        skip_range_max = NULL,
        start_with_counter = NULL )
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_sequence {
          sequence_name: "seq"
          set_options {
            option_name: "sequence_kind"
            string_value: "bit_reversed_positive"
          }
          set_options { option_name: "skip_range_min" null_value: true }
          set_options { option_name: "skip_range_max" null_value: true }
          set_options { option_name: "start_with_counter" null_value: true }
        }
      )pb")));
}

TEST(CreateSequence, CanParseCreateSequenceAllOptions) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          skip_range_min = 1,
          skip_range_max = 1000,
          start_with_counter = 1
      )
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_sequence {
          sequence_name: "seq"
          set_options {
            option_name: "sequence_kind"
            string_value: "bit_reversed_positive"
          }
          set_options { option_name: "skip_range_min" int64_value: 1 }
          set_options { option_name: "skip_range_max" int64_value: 1000 }
          set_options { option_name: "start_with_counter" int64_value: 1 }
        }
      )pb")));
}

TEST(CreateSequence, AllowNoSequenceKind) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
          skip_range_min = 1,
          skip_range_max = 1000,
          start_with_counter = 1
      )
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_sequence {
          sequence_name: "seq"
          set_options { option_name: "skip_range_min" int64_value: 1 }
          set_options { option_name: "skip_range_max" int64_value: 1000 }
          set_options { option_name: "start_with_counter" int64_value: 1 }
        }
      )pb")));
}

TEST(CreateSequence, Invalid_EmptyOptionList) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS ()
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Encountered ')' while parsing: identifier")));
}

TEST(CreateSequence, Invalid_NullSequenceKind) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = NULL
      )
      )sql"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr(
              "The only supported sequence kind is `bit_reversed_positive`")));
}

TEST(CreateSequence, Invalid_UnknownSequenceKind) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = "some_kind"
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unsupported sequence kind: some_kind")));
}

TEST(CreateSequence, Invalid_UnknownOption) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        start_with = 1
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Option: start_with is unknown")));
}

TEST(CreateSequence, Invalid_WrongOptionValue) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        start_with_counter = "hello"
      )
      )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Unexpected value for option: start_with_counter. "
                         "Supported option values are integers and NULL.")));
}

TEST(CreateSequence, Invalid_DuplicateOption) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq OPTIONS (
        sequence_kind = "bit_reversed_positive",
        sequence_kind = "bit_reversed_positive"
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Duplicate option: sequence_kind")));
}

TEST(CreateSequence, Invalid_SetOptionClause) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE SEQUENCE seq SET OPTIONS (
        sequence_kind = "bit_reversed_positive"
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Expecting 'EOF' but found 'SET'")));
}

TEST(AlterSequence, SetSequenceKind) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        sequence_kind = "bit_reversed_positive"
      )
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_sequence {
                  sequence_name: "seq"
                  set_options {
                    options {
                      option_name: "sequence_kind"
                      string_value: "bit_reversed_positive"
                    }
                  }
                }
              )pb")));
}

TEST(AlterSequence, AlterSequenceClauseDisabled) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_identity_columns = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SKIP RANGE 1, 1000
      )sql"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr(
              "SKIP RANGE is not supported in ALTER SEQUENCE statements")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq NO SKIP RANGE
      )sql"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr(
              "NO SKIP RANGE is not supported in ALTER SEQUENCE statements")));
}

TEST(AlterSequence, WithIfExists) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE IF EXISTS seq SET OPTIONS (
        start_with_counter = 1
      )
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_sequence {
                  sequence_name: "seq"
                  set_options {
                    options { option_name: "start_with_counter" int64_value: 1 }
                  }
                  existence_modifier: IF_EXISTS
                }
              )pb")));
}

TEST(AlterSequence, SetStartWithCounter) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        start_with_counter = 1
      )
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_sequence {
                  sequence_name: "seq"
                  set_options {
                    options { option_name: "start_with_counter" int64_value: 1 }
                  }
                }
              )pb")));
}

TEST(AlterSequence, SetSkipRange) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        skip_range_min = 1,
        skip_range_max = 1000
      )
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_sequence {
                  sequence_name: "seq"
                  set_options {
                    options { option_name: "skip_range_min" int64_value: 1 }
                    options { option_name: "skip_range_max" int64_value: 1000 }
                  }
                }
              )pb")));
}

TEST(AlterSequence, SetMultipleOptions) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        skip_range_min = 1,
        skip_range_max = 1000,
        start_with_counter = 100
      )
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_sequence {
          sequence_name: "seq"
          set_options {
            options { option_name: "skip_range_min" int64_value: 1 }
            options { option_name: "skip_range_max" int64_value: 1000 }
            options { option_name: "start_with_counter" int64_value: 100 }
          }
        }
      )pb")));
}

TEST(AlterSequence, Invalid_EmptySetOptionClause) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Encountered ')' while parsing: identifier")));
}

TEST(AlterSequence, Invalid_OptionClauseWithoutSetKeyword) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq OPTIONS (
        skip_range_min = 1
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Encountered 'OPTIONS' while parsing: "
                                 "alter_sequence_statement")));
}

TEST(AlterSequence, Invalid_SetSequenceKindToNull) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        sequence_kind = NULL
      )
      )sql"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr(
              "The only supported sequence kind is `bit_reversed_positive`")));
}

TEST(AlterSequence, Invalid_SetSequenceKindToOtherKind) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        sequence_kind = "other_kind"
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unsupported sequence kind: other_kind")));
}

TEST(AlterSequence, Invalid_UnknownOption) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        start_with = 1
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Option: start_with is unknown")));
}

TEST(AlterSequence, Invalid_WrongOptionValue) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        start_with_counter = "hello"
      )
      )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Unexpected value for option: start_with_counter. "
                         "Supported option values are integers and NULL.")));
}

TEST(AlterSequence, Invalid_DuplicateOption) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER SEQUENCE seq SET OPTIONS (
        start_with_counter = 1,
        start_with_counter = 1
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Duplicate option: start_with_counter")));
}

TEST(DropSequence, Invalid_WithOptionClause) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      DROP SEQUENCE seq OPTIONS (
        start_with_counter = 1
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Expecting 'EOF' but found 'OPTIONS'")));
}

TEST(DropSequence, Invalid_WithSetOptionClause) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      DROP SEQUENCE seq SET OPTIONS (
        start_with_counter = 1
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Expecting 'EOF' but found 'SET'")));
}

TEST(DropSequence, Basic) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      DROP SEQUENCE seq
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_sequence { sequence_name: "seq" }
              )pb")));
}

TEST(DropSequence, WithIfExists) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      DROP SEQUENCE IF EXISTS seq
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        drop_sequence { sequence_name: "seq" existence_modifier: IF_EXISTS }
      )pb")));
}

TEST(ParseViews, CreateViewNoSqlSecurity) {
  // The parser is able to parse this, but should be rejected during
  // schema update.
  EXPECT_THAT(ParseDDLStatement("CREATE VIEW `MyView` AS SELECT 1"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_function {
                  function_name: "MyView"
                  function_kind: VIEW
                  sql_body: "SELECT 1"
                  language: SQL
                })pb")));
}

TEST(ParseViews, CreateViewWithSqlSecurity) {
  EXPECT_THAT(ParseDDLStatement("CREATE VIEW MyView "
                                "SQL SECURITY INVOKER AS SELECT 1"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_function {
                  function_name: "MyView"
                  function_kind: VIEW
                  sql_body: "SELECT 1"
                  sql_security: INVOKER
                  language: SQL
                })pb")));
}

TEST(ParseViews, CreateViewWithEmptySqlBody) {
  EXPECT_THAT(ParseDDLStatement("CREATE VIEW MyView "
                                "SQL SECURITY INVOKER AS ()"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_function {
                  function_name: "MyView"
                  function_kind: VIEW
                  sql_body: "()"
                  sql_security: INVOKER
                  language: SQL
                })pb")));
}

TEST(ParseViews, CreateOrReplaceView) {
  EXPECT_THAT(ParseDDLStatement("CREATE OR REPLACE VIEW MyView "
                                "SQL SECURITY INVOKER AS SELECT 1"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_function {
                  function_name: "MyView"
                  function_kind: VIEW
                  sql_body: "SELECT 1"
                  sql_security: INVOKER
                  is_or_replace: true
                  language: SQL
                })pb")));
}

TEST(ParseViews, ParenthesizedViewDefinition) {
  EXPECT_THAT(ParseDDLStatement("CREATE OR REPLACE VIEW MyView "
                                "SQL SECURITY INVOKER AS (SELECT 1)"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_function {
                  function_name: "MyView"
                  function_kind: VIEW
                  is_or_replace: true
                  sql_security: INVOKER
                  sql_body: "(SELECT 1)"
                  language: SQL
                })pb")));
}

TEST(ParseViews, DropView) {
  EXPECT_THAT(
      ParseDDLStatement("DROP VIEW MyView "),
      IsOkAndHolds(test::EqualsProto(R"pb(
        drop_function { function_name: "MyView" function_kind: VIEW })pb")));
}

TEST(ParseCreateModel, ParseCreateModel) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE MODEL MyModel
    )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_model { model_name: "MyModel" })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE OR REPLACE MODEL MyModel
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_model { model_name: "MyModel" existence_modifier: OR_REPLACE }
      )pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE MODEL IF NOT EXISTS MyModel
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_model { model_name: "MyModel" existence_modifier: IF_NOT_EXISTS }
      )pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE MODEL MyModel
    INPUT (
      f1 INT64,
      f2 STRING(MAX)
    )
    OUTPUT (
      l1 BOOL,
      l2 ARRAY<FLOAT64>
    )
    OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e'
    )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_model {
          model_name: "MyModel"
          set_options {
            option_name: "endpoint"
            string_value: "//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e"
          }
          input { column_name: "f1" type: INT64 }
          input { column_name: "f2" type: STRING }
          output { column_name: "l1" type: BOOL }
          output {
            column_name: "l2"
            type: ARRAY
            array_subtype { type: DOUBLE }
          }
        })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE MODEL MyModel
    INPUT (
      f1 INT64,
      f2 STRING(MAX)
    )
    OUTPUT (
      l1 STRUCT<field1 BOOL>,
      l2 STRUCT<arr ARRAY<STRING(MAX)>, str STRUCT<bar DATE, foo BYTES(1024)>>
    )
    OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e'
    )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_model {
          model_name: "MyModel"
          set_options {
            option_name: "endpoint"
            string_value: "//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e"
          }
          input { column_name: "f1" type: INT64 }
          input { column_name: "f2" type: STRING }
          output {
            column_name: "l1"
            type: STRUCT
            type_definition {
              type: STRUCT
              struct_descriptor {
                field {
                  name: "field1"
                  type { type: BOOL }
                }
              }
            }
          }
          output {
            column_name: "l2"
            type: STRUCT
            type_definition {
              type: STRUCT
              struct_descriptor {
                field {
                  name: "arr"
                  type {
                    type: ARRAY
                    array_subtype { type: STRING }
                  }
                }
                field {
                  name: "str"
                  type {
                    type: STRUCT
                    struct_descriptor {
                      field {
                        name: "bar"
                        type { type: DATE }
                      }
                      field {
                        name: "foo"
                        type { type: BYTES length: 1024 }
                      }
                    }
                  }
                }
              }
            }
          }
        })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE MODEL MyModel
    INPUT (
      f1 INT64,
      f2 STRING(MAX)
    )
    OUTPUT (
      l1 STRUCT<field1 BOOL>,
      l2 STRUCT<ARRAY<STRING(MAX)>, str STRUCT<>>
    )
    OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e'
    )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_model {
          model_name: "MyModel"
          set_options {
            option_name: "endpoint"
            string_value: "//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e"
          }
          input { column_name: "f1" type: INT64 }
          input { column_name: "f2" type: STRING }
          output {
            column_name: "l1"
            type: STRUCT
            type_definition {
              type: STRUCT
              struct_descriptor {
                field {
                  name: "field1"
                  type { type: BOOL }
                }
              }
            }
          }
          output {
            column_name: "l2"
            type: STRUCT
            type_definition {
              type: STRUCT
              struct_descriptor {
                field {
                  type {
                    type: ARRAY
                    array_subtype { type: STRING }
                  }
                }
                field {
                  name: "str"
                  type {
                    type: STRUCT
                    struct_descriptor {}
                  }
                }
              }
            }
          }
        })pb")));

  // CREATE MODEL with column options
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE MODEL MyModel
    INPUT (
      f1 INT64 OPTIONS (required = false),
      f2 STRING(MAX)
    )
    OUTPUT (
      l1 BOOL OPTIONS (required = true),
      l2 ARRAY<FLOAT64>
    )
    OPTIONS (
      endpoint = '//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e'
    )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_model {
          model_name: "MyModel"
          set_options {
            option_name: "endpoint"
            string_value: "//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e"
          }
          input {
            column_name: "f1"
            type: INT64
            set_options { option_name: "required" bool_value: false }
          }
          input { column_name: "f2" type: STRING }
          output {
            column_name: "l1"
            type: BOOL
            set_options { option_name: "required" bool_value: true }
          }
          output {
            column_name: "l2"
            type: ARRAY
            array_subtype { type: DOUBLE }
          }
        })pb")));

  // CREATE MODEL with multiple endpoints
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE MODEL MyModel
    INPUT (
      f1 INT64,
      f2 STRING(MAX)
    )
    OUTPUT (
      l1 BOOL,
      l2 ARRAY<FLOAT64>
    )
    OPTIONS (
      endpoints = ['//aiplatform.googleapis.com/projects/p/locations/l/endpoints/1',
      '//aiplatform.googleapis.com/projects/p/locations/l/endpoints/2']
    )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_model {
          model_name: "MyModel"
          set_options {
            option_name: "endpoints"
            string_list_value: "//aiplatform.googleapis.com/projects/p/locations/l/endpoints/1"
            string_list_value: "//aiplatform.googleapis.com/projects/p/locations/l/endpoints/2"
          }
          input { column_name: "f1" type: INT64 }
          input { column_name: "f2" type: STRING }
          output { column_name: "l1" type: BOOL }
          output {
            column_name: "l2"
            type: ARRAY
            array_subtype { type: DOUBLE }
          }
        })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE MODEL
      )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'EOF' while parsing: identifier")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE MODEL MyModel OPTIONS ()
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Encountered ')' while parsing: identifier")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE MODEL MyModel OPTIONS (unknown_option = true)
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Option: unknown_option is unknown")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE MODEL MyModel OPTIONS (
        endpoint = 'test',
        endpoint = 'test'
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Duplicate option: endpoint")));

  // Model has a STRUCT column with missing types
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      CREATE MODEL m INPUT (f1 INT64) OUTPUT (l1 STRUCT<foo, bar>)
      )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered ',' while parsing: column_type")));

  // Model has an unexpected PRIMARY KEY in input column
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE MODEL m
      INPUT (
        f1 INT64,
        f2 STRING(MAX) PRIMARY KEY
      )
      OUTPUT (
        l1 BOOL,
        l2 ARRAY<FLOAT64>
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unexpected PRIMARY KEY clause")));

  // Model has an unexpected PRIMARY KEY in output column
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE MODEL m
      INPUT (
        f1 INT64,
        f2 STRING(MAX)
      )
      OUTPUT (
        l1 BOOL PRIMARY KEY,
        l2 ARRAY<FLOAT64>
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Unexpected PRIMARY KEY clause")));
}

TEST(ParseAlterModel, ParseAlterModel) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER MODEL MyModel
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_model { model_name: "MyModel" })pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER MODEL IF EXISTS MyModel
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_model { model_name: "MyModel" if_exists: true })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER MODEL MyModel SET OPTIONS (
        endpoint='//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e'
      )
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_model {
          model_name: "MyModel"
          set_options {
            options {
              option_name: "endpoint"
              string_value: "//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e"
            }
          }
        })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER MODEL MyModel SET OPTIONS (
        endpoint=NULL
      )
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_model {
          model_name: "MyModel"
          set_options { options { option_name: "endpoint" null_value: true } }
        })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      ALTER MODEL
      )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'EOF' while parsing: identifier")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER MODEL MyModel SET OPTIONS ()
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Encountered ')' while parsing: identifier")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER MODEL MyModel SET OPTIONS (unknown_option = true)
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Option: unknown_option is unknown")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      ALTER MODEL MyModel SET OPTIONS (
        endpoint='//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e',
        endpoint='//aiplatform.googleapis.com/projects/p/locations/l/endpoints/e'
      )
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Duplicate option: endpoint")));
}

TEST(ParseDropModel, ParseDropModel) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      DROP MODEL MyModel
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_model { model_name: "MyModel" })pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
      DROP MODEL IF EXISTS MyModel
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_model { model_name: "MyModel" if_exists: true })pb")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
      DROP MODEL
      )sql"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'EOF' while parsing: identifier")));
}

TEST(ParseViews, DropViewIfExists) {
  EXPECT_THAT(ParseDDLStatement("DROP VIEW IF EXISTS MyView"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_function {
                  function_name: "MyView"
                  function_kind: VIEW
                  existence_modifier: IF_EXISTS
                })pb")));
}
TEST(CreateSchema, Basic) {
  EXPECT_THAT(ParseDDLStatement("CREATE SCHEMA MySchema "),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_schema { schema_name: "MySchema" })pb")));
}

TEST(CreateSchema, IfExists) {
  EXPECT_THAT(ParseDDLStatement("CREATE SCHEMA IF NOT EXISTS MySchema"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_schema {
                  schema_name: "MySchema"
                  existence_modifier: IF_NOT_EXISTS
                })pb")));
}
TEST(AlterSchema, Basic) {
  EXPECT_THAT(ParseDDLStatement("ALTER SCHEMA MySchema SET OPTIONS (blah = 1)"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_schema { schema_name: "MySchema" })pb")));
}

TEST(AlterSchema, IfExists) {
  EXPECT_THAT(
      ParseDDLStatement(
          "ALTER SCHEMA IF EXISTS MySchema SET OPTIONS (blah = 1)"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_schema { schema_name: "MySchema" if_exists: true })pb")));
}

TEST(DropSchema, Basic) {
  EXPECT_THAT(ParseDDLStatement("DROP SCHEMA MySchema"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_schema { schema_name: "MySchema" })pb")));
}

TEST(DropSchema, IfExists) {
  EXPECT_THAT(ParseDDLStatement("DROP SCHEMA IF EXISTS MySchema"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_schema { schema_name: "MySchema" if_exists: true })pb")));
}

TEST(ParseAnalyze, CanParseAnalyze) {
  EXPECT_THAT(ParseDDLStatement("ANALYZE"), IsOk());
}

TEST(ParseFGAC, ParseCreateRole) {
  EXPECT_THAT(ParseDDLStatement("CREATE ROLE myrole"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_role { role_name: "myrole" })pb")));
}

TEST(ParseFGAC, ParseDropRole) {
  EXPECT_THAT(ParseDDLStatement("DROP ROLE myrole"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_role { role_name: "myrole" })pb")));
}

TEST(ParseFGAC, GrantPrivilege) {
  // Simple single privilege.
  EXPECT_THAT(ParseDDLStatement("GRANT INSERT ON TABLE MyTable TO ROLE MyRole"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                grant_privilege {
                  privilege { type: INSERT }
                  target { type: TABLE name: "MyTable" }
                  grantee { type: ROLE name: "MyRole" }
                })pb")));

  // Multiple privileges.
  EXPECT_THAT(
      ParseDDLStatement(
          "GRANT INSERT, SELECT, UPDATE ON TABLE MyTable TO ROLE MyRole"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        grant_privilege {
          privilege { type: INSERT }
          privilege { type: SELECT }
          privilege { type: UPDATE }
          target { type: TABLE name: "MyTable" }
          grantee { type: ROLE name: "MyRole" }
        })pb")));

  // Multiple grantees.
  EXPECT_THAT(ParseDDLStatement(
                  "GRANT INSERT ON TABLE MyTable TO ROLE MyRole1, MyRole2"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                grant_privilege {
                  privilege { type: INSERT }
                  target { type: TABLE name: "MyTable" }
                  grantee { type: ROLE name: "MyRole1" }
                  grantee { type: ROLE name: "MyRole2" }
                })pb")));

  // Single Invalid Privilege.
  EXPECT_THAT(
      ParseDDLStatement("GRANT DESTROY ON TABLE MyTable TO ROLE MyRole"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Encountered 'DESTROY' while parsing: grant_statement")));

  // Multiple Invalid Privileges.
  EXPECT_THAT(
      ParseDDLStatement(
          "GRANT DESTROY CRASH BURN ON TABLE MyTable TO ROLE MyRole"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Encountered 'DESTROY' while parsing: grant_statement")));

  // Valid and Invalid Privileges.
  EXPECT_THAT(
      ParseDDLStatement(
          "GRANT INSERT, UPDATE, DESTROY ON TABLE MyTable TO ROLE MyRole"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'DESTROY' while parsing: privilege")));
}

TEST(ParseFGAC, GrantMembership) {
  // Single role, single grantee.
  EXPECT_THAT(ParseDDLStatement("GRANT ROLE MyRole1 TO ROLE MyRole2"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                grant_membership {
                  role { type: ROLE name: "MyRole1" }
                  grantee { type: ROLE name: "MyRole2" }
                })pb")));

  // Multiple roles, single grantee.
  EXPECT_THAT(ParseDDLStatement("GRANT ROLE MyRole1, MyRole2 TO ROLE MyRole3"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                grant_membership {
                  role { type: ROLE name: "MyRole1" }
                  role { type: ROLE name: "MyRole2" }
                  grantee { type: ROLE name: "MyRole3" }
                })pb")));

  // Single role, mutiple grantees.
  EXPECT_THAT(ParseDDLStatement("GRANT ROLE MyRole1 TO ROLE MyRole2, MyRole3"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                grant_membership {
                  role { type: ROLE name: "MyRole1" }
                  grantee { type: ROLE name: "MyRole2" }
                  grantee { type: ROLE name: "MyRole3" }
                })pb")));

  // Multiple roles, mutiple grantees.
  EXPECT_THAT(ParseDDLStatement(R"sql(
    GRANT ROLE MyRole1, MyRole2 TO ROLE MyRole3, MyRole4
  )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                grant_membership {
                  role { type: ROLE name: "MyRole1" }
                  role { type: ROLE name: "MyRole2" }
                  grantee { type: ROLE name: "MyRole3" }
                  grantee { type: ROLE name: "MyRole4" }
                })pb")));
}

TEST(ParseFGAC, RevokePrivilege) {
  // Simple single privilege.
  EXPECT_THAT(
      ParseDDLStatement("REVOKE INSERT ON TABLE MyTable FROM ROLE MyRole"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        revoke_privilege {
          privilege { type: INSERT }
          target { type: TABLE name: "MyTable" }
          grantee { type: ROLE name: "MyRole" }
        })pb")));

  // Multiple privileges.
  EXPECT_THAT(
      ParseDDLStatement(
          "REVOKE INSERT, SELECT, UPDATE ON TABLE MyTable FROM ROLE MyRole"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        revoke_privilege {
          privilege { type: INSERT }
          privilege { type: SELECT }
          privilege { type: UPDATE }
          target { type: TABLE name: "MyTable" }
          grantee { type: ROLE name: "MyRole" }
        })pb")));

  // Multiple grantees.
  EXPECT_THAT(ParseDDLStatement(
                  "REVOKE INSERT ON TABLE MyTable FROM ROLE MyRole1, MyRole2"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                revoke_privilege {
                  privilege { type: INSERT }
                  target { type: TABLE name: "MyTable" }
                  grantee { type: ROLE name: "MyRole1" }
                  grantee { type: ROLE name: "MyRole2" }
                })pb")));

  // Single Invalid Privilege.
  EXPECT_THAT(
      ParseDDLStatement("REVOKE DESTROY ON TABLE MyTable FROM ROLE MyRole"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Encountered 'DESTROY' while parsing: revoke_statement")));

  // Multiple Invalid Privileges.
  EXPECT_THAT(
      ParseDDLStatement(
          "REVOKE DESTROY CRASH BURN FROM TABLE MyTable TO ROLE MyRole"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Encountered 'DESTROY' while parsing: revoke_statement")));

  // Valid and Invalid Privileges.
  EXPECT_THAT(
      ParseDDLStatement(
          "REVOKE INSERT, UPDATE, DESTROY FROM TABLE MyTable TO ROLE MyRole"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'DESTROY' while parsing: privilege")));
}

TEST(ParseFGAC, RevokeMembership) {
  // Single role, single grantee.
  EXPECT_THAT(ParseDDLStatement("REVOKE ROLE MyRole1 FROM ROLE MyRole2"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                revoke_membership {
                  role { type: ROLE name: "MyRole1" }
                  grantee { type: ROLE name: "MyRole2" }
                })pb")));

  // Multiple roles, single grantee.
  EXPECT_THAT(
      ParseDDLStatement("REVOKE ROLE MyRole1, MyRole2 FROM ROLE MyRole3"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        revoke_membership {
          role { type: ROLE name: "MyRole1" }
          role { type: ROLE name: "MyRole2" }
          grantee { type: ROLE name: "MyRole3" }
        })pb")));

  // Single role, mutiple grantees.
  EXPECT_THAT(
      ParseDDLStatement("REVOKE ROLE MyRole1 FROM ROLE MyRole2, MyRole3"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        revoke_membership {
          role { type: ROLE name: "MyRole1" }
          grantee { type: ROLE name: "MyRole2" }
          grantee { type: ROLE name: "MyRole3" }
        })pb")));

  // Multiple roles, mutiple grantees.
  EXPECT_THAT(ParseDDLStatement(R"sql(
    REVOKE ROLE MyRole1, MyRole2 FROM ROLE MyRole3, MyRole4
  )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                revoke_membership {
                  role { type: ROLE name: "MyRole1" }
                  role { type: ROLE name: "MyRole2" }
                  grantee { type: ROLE name: "MyRole3" }
                  grantee { type: ROLE name: "MyRole4" }
                })pb")));
}
class IdentityColumn : public ::testing::Test {
 public:
  IdentityColumn() : feature_flags_({.enable_identity_columns = true}) {}

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(IdentityColumn, DisabledFlag) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_identity_columns = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE TABLE T (
        Id INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
        Value STRING(MAX),
      ) PRIMARY KEY(Id)
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Identity columns are not supported.")));
}

TEST_F(IdentityColumn, CreateTableKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE TABLE T (
        Id INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
        Value STRING(MAX),
      ) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column { type: BIT_REVERSED_POSITIVE }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableNonKeyColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE TABLE T (
        Id INT64,
        Value INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
      ) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column { column_name: "Id" type: INT64 }
                  column {
                    column_name: "Value"
                    type: INT64
                    identity_column { type: BIT_REVERSED_POSITIVE }
                  }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableTwoColumns) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE TABLE T (
        Id INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
        Value INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
      ) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column { type: BIT_REVERSED_POSITIVE }
                  }
                  column {
                    column_name: "Value"
                    type: INT64
                    identity_column { type: BIT_REVERSED_POSITIVE }
                  }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableStartCounter) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
      CREATE TABLE T (
        Id INT64 GENERATED BY DEFAULT AS IDENTITY (
          BIT_REVERSED_POSITIVE START COUNTER WITH 1000),
        Value STRING(MAX),
      ) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      start_with_counter: 1000
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableSkipRange) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE SKIP RANGE 2000, 3000),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      skip_range_min: 2000
                      skip_range_max: 3000
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableSkipRangeDifferentOrder) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    SKIP RANGE 1, 1000 BIT_REVERSED_POSITIVE),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      skip_range_min: 1
                      skip_range_max: 1000
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableAllParams) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE START COUNTER WITH 1000 SKIP RANGE 2000, 3000),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      start_with_counter: 1000
                      skip_range_min: 2000
                      skip_range_max: 3000
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableAllParamsDifferentOrder) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE SKIP RANGE 2000, 3000 START COUNTER WITH 1000),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      start_with_counter: 1000
                      skip_range_min: 2000
                      skip_range_max: 3000
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableNamedSchema) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE NamedSchema.T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "NamedSchema.T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column { type: BIT_REVERSED_POSITIVE }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableNoSequenceKind) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY,
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {}
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableInvalidSyntax) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (UNKNOWN_SEQUENCE_KIND),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY
    (BIT_REVERSED_POSITIVE) (BIT_REVERSED_POSITIVE),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE BIT_REVERSED_POSITIVE),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("The sequence kind is set more than once")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE START SKIP RANGE 2000, 3000),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    START COUNTER WITH 1000 START COUNTER WITH 1000),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("START WITH COUNTER is set more than once")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    SKIP RANGE 1000, 2000 SKIP RANGE 1000, 2000),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("SKIP RANGE is set more than once")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED ALWAYS AS IDENTITY (BIT_REVERSED_POSITIVE),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE SKIP RANGE 1000),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST_F(IdentityColumn, CreateTableNegativeStartCounter) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE START COUNTER WITH -1),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      start_with_counter: -1
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableNegativeSkipRange) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE SKIP RANGE -1, -1),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      skip_range_min: -1
                      skip_range_max: -1
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, CreateTableSkipRangeMinLargerThanMax) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 GENERATED BY DEFAULT AS IDENTITY (
    BIT_REVERSED_POSITIVE SKIP RANGE 1000, 100),
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {
                      type: BIT_REVERSED_POSITIVE
                      skip_range_min: 1000
                      skip_range_max: 100
                    }
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterIdentityRestartCounter) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY RESTART COUNTER WITH 1000
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: NONE
                      identity_column { start_with_counter: 1000 }
                    }
                    operation: ALTER_IDENTITY
                    identity_alter_start_with_counter: true
                  }
                }
              )pb")));

  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER Id ALTER IDENTITY RESTART COUNTER WITH 1000
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: NONE
                      identity_column { start_with_counter: 1000 }
                    }
                    operation: ALTER_IDENTITY
                    identity_alter_start_with_counter: true
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterIdentitySkipRange) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE 2000, 3000
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_table {
          table_name: "T"
          alter_column {
            column {
              column_name: "Id"
              type: NONE
              identity_column { skip_range_min: 2000 skip_range_max: 3000 }
            }
            operation: ALTER_IDENTITY
            identity_alter_skip_range: true
          }
        }
      )pb")));
}

TEST_F(IdentityColumn, AlterIdentityNoSkipRange) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET NO SKIP RANGE
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: NONE
                      identity_column {}
                    }
                    operation: ALTER_IDENTITY
                    identity_alter_skip_range: true
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterIdentityInvalidSyntax) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id RESTART COUNTER WITH 1000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id SET SKIP RANGE 2000, 3000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id
  ALTER IDENTITY RESTART COUNTER WITH 1000 RESTART COUNTER WITH 2000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id
  ALTER IDENTITY RESTART COUNTER WITH 1000 SET SKIP RANGE 2000, 3000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id
  ALTER IDENTITY SET SKIP RANGE 2000, 3000 SET NO SKIP RANGE
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE 2000 3000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE 2000: 3000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET UNKNOWN_SEQUENCE_KIND
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE 1000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE ,1000
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE 1, NULL
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE NULL, 100
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST_F(IdentityColumn, AlterIdentityNegativeRestartCounter) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY RESTART COUNTER WITH -1
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: NONE
                      identity_column { start_with_counter: -1 }
                    }
                    operation: ALTER_IDENTITY
                    identity_alter_start_with_counter: true
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterIdentityNegativeSkipRange) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE -1, -1
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: NONE
                      identity_column { skip_range_min: -1 skip_range_max: -1 }
                    }
                    operation: ALTER_IDENTITY
                    identity_alter_skip_range: true
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterIdentityMinLargerThanMax) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id ALTER IDENTITY SET SKIP RANGE 1000, 100
      )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        alter_table {
          table_name: "T"
          alter_column {
            column {
              column_name: "Id"
              type: NONE
              identity_column { skip_range_min: 1000 skip_range_max: 100 }
            }
            operation: ALTER_IDENTITY
            identity_alter_skip_range: true
          }
        }
      )pb")));
}

TEST_F(IdentityColumn, AlterColumnNoSequenceKind) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id INT64 GENERATED BY DEFAULT AS IDENTITY
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: INT64
                      identity_column {}
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: INT64
                      identity_column { type: BIT_REVERSED_POSITIVE }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterColumnStartCounter) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE START COUNTER WITH 1000)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: INT64
                      identity_column {
                        type: BIT_REVERSED_POSITIVE
                        start_with_counter: 1000
                      }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterColumnSkipRange) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE SKIP RANGE 2000, 3000)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: INT64
                      identity_column {
                        type: BIT_REVERSED_POSITIVE
                        skip_range_min: 2000
                        skip_range_max: 3000
                      }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AlterColumnAllParams) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ALTER COLUMN Id INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE START COUNTER WITH 1000 SKIP RANGE 2000, 3000)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  alter_column {
                    column {
                      column_name: "Id"
                      type: INT64
                      identity_column {
                        type: BIT_REVERSED_POSITIVE
                        start_with_counter: 1000
                        skip_range_min: 2000
                        skip_range_max: 3000
                      }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AddColumn) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ADD COLUMN Value INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  add_column {
                    column {
                      column_name: "Value"
                      type: INT64
                      identity_column { type: BIT_REVERSED_POSITIVE }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AddColumnStartCounter) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ADD COLUMN Value INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE START COUNTER WITH 1000)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  add_column {
                    column {
                      column_name: "Value"
                      type: INT64
                      identity_column {
                        type: BIT_REVERSED_POSITIVE
                        start_with_counter: 1000
                      }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AddColumnSkipRange) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ADD COLUMN Value INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE SKIP RANGE 3000, 4000)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  add_column {
                    column {
                      column_name: "Value"
                      type: INT64
                      identity_column {
                        type: BIT_REVERSED_POSITIVE
                        skip_range_min: 3000
                        skip_range_max: 4000
                      }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AddColumnAllParams) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ADD COLUMN Value INT64 GENERATED BY DEFAULT AS IDENTITY (
  BIT_REVERSED_POSITIVE START COUNTER WITH 1000 SKIP RANGE 3000, 4000)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  add_column {
                    column {
                      column_name: "Value"
                      type: INT64
                      identity_column {
                        type: BIT_REVERSED_POSITIVE
                        start_with_counter: 1000
                        skip_range_min: 3000
                        skip_range_max: 4000
                      }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AddColumnNoSequenceKind) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ADD COLUMN Value INT64 GENERATED BY DEFAULT AS IDENTITY
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  add_column {
                    column {
                      column_name: "Value"
                      type: INT64
                      identity_column {}
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AddColumnInvalidSyntax) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
ALTER TABLE T ADD COLUMN Value INT64 GENERATED BY DEFAULT AS IDENTITY ()
      )sql"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST_F(IdentityColumn, AddColumnStringType) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
ALTER TABLE T ADD COLUMN Value STRING(MAX) GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_table {
                  table_name: "T"
                  add_column {
                    column {
                      column_name: "Value"
                      type: STRING
                      identity_column { type: BIT_REVERSED_POSITIVE }
                    }
                  }
                }
              )pb")));
}

TEST_F(IdentityColumn, AutoIncrement) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_serial_auto_increment = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(ParseDDLStatement(R"sql(
CREATE TABLE T (
  Id INT64 AUTO_INCREMENT,
  Value STRING(MAX),
) PRIMARY KEY(Id)
      )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_table {
                  table_name: "T"
                  column {
                    column_name: "Id"
                    type: INT64
                    identity_column {}
                  }
                  column { column_name: "Value" type: STRING }
                  primary_key { key_name: "Id" }
                }
              )pb")));
}

TEST_F(IdentityColumn, AutoIncrementFlagDisabled) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_serial_auto_increment = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(ParseDDLStatement("CREATE TABLE T (Id INT64 AUTO_INCREMENT, "
                                "Value STRING(MAX)) PRIMARY KEY(Id)"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("AUTO_INCREMENT is not supported")));
}

TEST(UserDefinedFunction, CreateFunctionBasicFlagDisabled) {
  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL "
                        "SECURITY INVOKER AS (x+1)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("User defined functions are not supported.")));
}

TEST(UserDefinedFunction, CreateFunctionBasic) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL "
                        "SECURITY INVOKER AS (x+1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "x" param_typename: "INT64" }
                  return_typename: "INT64"
                  sql_body: "x+1"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, ParameterizedTypes) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement("CREATE FUNCTION T.func(a STRING(10)) "
                        "RETURNS STRING(MAX) SQL SECURITY INVOKER AS (NULL)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "T.func"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "a" param_typename: "STRING(10)" }
                  return_typename: "STRING(MAX)"
                  sql_body: "NULL"
                  language: SQL
                }
              )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      statement,
      ParseDDLStatement("CREATE FUNCTION T.func(a STRING) "
                        "RETURNS STRING(10) SQL SECURITY INVOKER AS (NULL)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "T.func"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "a" param_typename: "STRING" }
                  return_typename: "STRING(10)"
                  sql_body: "NULL"
                  language: SQL
                }
              )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      statement,
      ParseDDLStatement("CREATE FUNCTION T.func(a ARRAY<STRING>) RETURNS "
                        "STRING SQL SECURITY INVOKER AS (NULL)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "T.func"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "a" param_typename: "ARRAY<STRING>" }
                  return_typename: "STRING"
                  sql_body: "NULL"
                  language: SQL
                }
              )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      statement, ParseDDLStatement(
                     "CREATE FUNCTION T.func(a STRING) "
                     "RETURNS ARRAY<STRING> SQL SECURITY INVOKER  AS (NULL)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "T.func"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "a" param_typename: "STRING" }
                  return_typename: "ARRAY<STRING>"
                  sql_body: "NULL"
                  language: SQL
                }
              )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      statement,
      ParseDDLStatement("CREATE FUNCTION T.func(a ARRAY<STRING(10)>) RETURNS "
                        "STRING SQL SECURITY INVOKER AS (NULL)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "T.func"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "a" param_typename: "ARRAY<STRING(10)>" }
                  return_typename: "STRING"
                  sql_body: "NULL"
                  language: SQL
                }
              )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      statement,
      ParseDDLStatement("CREATE FUNCTION T.func(a STRING) RETURNS "
                        "ARRAY<STRING(10)> SQL SECURITY INVOKER AS (a)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "T.func"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "a" param_typename: "STRING" }
                  return_typename: "ARRAY<STRING(10)>"
                  sql_body: "a"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithAnyType) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION T.func(a ANY TYPE, b INT64) RETURNS "
                        "INT64 AS (a + b)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'ANY' while parsing")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION T.func(a ARRAY<ANY>) RETURNS INT64 AS "
                        "(NULL)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'ANY' while parsing")));
}

TEST(UserDefinedFunction, CreateFunctionWithInvalidDeterminism) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo() RETURNS STRING IMMUTABLE AS "
                        "(GENERATE_UUID())"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'AS' but found 'IMMUTABLE'")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo() RETURNS STRING DETERMINISTIC AS "
                        "(GENERATE_UUID())"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'AS' but found 'DETERMINISTIC'")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo() RETURNS STRING STABLE AS "
                        "(GENERATE_UUID())"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'AS' but found 'STABLE'")));

  EXPECT_THAT(
      ParseDDLStatement(
          "CREATE FUNCTION foo(a INT64) RETURNS STRING NOT DETERMINISTIC AS "
          "(GENERATE_UUID())"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'AS' but found 'NOT'")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo() RETURNS STRING VOLATILE AS "
                        "(GENERATE_UUID())"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'AS' but found 'VOLATILE'")));

  EXPECT_THAT(
      ParseDDLStatement(
          "CREATE FUNCTION foo(a INT64) RETURNS STRING INVALID_DETERMINISM AS "
          "(GENERATE_UUID())"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'AS' but found 'INVALID_DETERMINISM'")));
}

TEST(UserDefinedFunction, CreateFunctionLowercase) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement("create function udf_1(x INT64) returns INT64 "
                        "sql security invoker as (x+1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "x" param_typename: "INT64" }
                  return_typename: "INT64"
                  sql_body: "x+1"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionComplexExpression) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement(
          "CREATE FUNCTION udf_1(x INT64, y INT64, z INT64) RETURNS INT64 SQL "
          "SECURITY INVOKER AS (x + y + (z / x))"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "x" param_typename: "INT64" }
                  param { name: "y" param_typename: "INT64" }
                  param { name: "z" param_typename: "INT64" }
                  return_typename: "INT64"
                  sql_body: "x + y + (z / x)"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithNoParams) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement("CREATE FUNCTION udf_1() RETURNS INT64 SQL SECURITY "
                        "INVOKER AS (1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  return_typename: "INT64"
                  sql_security: INVOKER
                  sql_body: "1"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithNoSqlSecurity) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement(
          "CREATE FUNCTION udf_1(x INT64) RETURNS INT64 AS (x+1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  param { name: "x" param_typename: "INT64" }
                  return_typename: "INT64"
                  sql_body: "x+1"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithNoReturns) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) AS (x+1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  param { name: "x" param_typename: "INT64" }
                  sql_body: "x+1"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithNoExpressionFails) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) "
                        "RETURNS INT64 SQL SECURITY INVOKER"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST(UserDefinedFunction, CreateFunctionWithInvalidSecurityFails) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) "
                        "RETURNS INT64 SQL SECURITY DEFINER"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST(UserDefinedFunction, CreateFunctionWithRemoteFails) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  // Remote functions are not supported in the emulator.
  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) "
                        "RETURNS INT64 REMOTE SECURITY DEFINER"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST(UserDefinedFunction, CreateFunctionWithNoFunctionNameFails) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION RETURNS INT64 SQL SECURITY INVOKER"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST(UserDefinedFunction, CreateOrReplaceFunction) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement(
          "CREATE OR REPLACE FUNCTION udf_1(x INT64) RETURNS INT64 SQL "
          "SECURITY INVOKER AS (x+1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  is_or_replace: true
                  sql_security: INVOKER
                  param { name: "x" param_typename: "INT64" }
                  return_typename: "INT64"
                  sql_body: "x+1"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithScalarSubquery) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL "
                        "SECURITY INVOKER AS ((SELECT 1) + x)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "x" param_typename: "INT64" }
                  return_typename: "INT64"
                  sql_body: "(SELECT 1) + x"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithDefaultsPositional) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto statement,
      ParseDDLStatement(
          "CREATE FUNCTION udf_1(x INT64 DEFAULT 1) RETURNS INT64 SQL "
          "SECURITY INVOKER AS (x+1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "x" param_typename: "INT64" default_value: "1" }
                  return_typename: "INT64"
                  sql_body: "x+1"
                  language: SQL
                }
              )pb"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      statement,
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64 DEFAULT 1, y INT64 "
                        "DEFAULT 2) RETURNS INT64 SQL "
                        "SECURITY INVOKER AS (x+1)"));
  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param { name: "x" param_typename: "INT64" default_value: "1" }
                  param { name: "y" param_typename: "INT64" default_value: "2" }
                  return_typename: "INT64"
                  sql_body: "x+1"
                  language: SQL
                }
              )pb"));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64 DEFAULT 1, y INT64, z "
                        "INT64 DEFAULT 3) RETURNS INT64 SQL "
                        "SECURITY INVOKER AS (x+1)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Function parameters must have default values if any "
                         "previous parameter has a default value.")));
}

TEST(UserDefinedFunction, CreateFunctionWithManyFunctionLiterals) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto statement, ParseDDLStatement(
                                           R"(
CREATE FUNCTION udf_many_literals(
  param_int64 INT64 DEFAULT 1,
  param_numeric NUMERIC DEFAULT NUMERIC '123.456',
  param_string STRING DEFAULT 'default_string',
  param_bool BOOL DEFAULT TRUE,
  param_timestamp TIMESTAMP DEFAULT TIMESTAMP '2021-01-01 00:00:00+00',
  param_struct STRUCT<field1 INT64> DEFAULT (1),
  param_json JSON DEFAULT JSON '{"key": "value"}',
  param_null INT64 DEFAULT NULL
) RETURNS INT64 SQL SECURITY INVOKER AS (param_int64 + 1)
)"));

  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_many_literals"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param {
                    name: "param_int64"
                    param_typename: "INT64"
                    default_value: "1"
                  }
                  param {
                    name: "param_numeric"
                    param_typename: "NUMERIC"
                    default_value: "NUMERIC \'123.456\'"
                  }
                  param {
                    name: "param_string"
                    param_typename: "STRING"
                    default_value: "\'default_string\'"
                  }
                  param {
                    name: "param_bool"
                    param_typename: "BOOL"
                    default_value: "TRUE"
                  }
                  param {
                    name: "param_timestamp"
                    param_typename: "TIMESTAMP"
                    default_value: "TIMESTAMP \'2021-01-01 00:00:00+00\'"
                  }
                  param {
                    name: "param_struct"
                    param_typename: "STRUCT<field1 INT64>"
                    default_value: "(1)"
                  }
                  param {
                    name: "param_json"
                    param_typename: "JSON"
                    default_value: "JSON \'{\"key\": \"value\"}\'"
                  }
                  param {
                    name: "param_null"
                    param_typename: "INT64"
                    default_value: "NULL"
                  }
                  return_typename: "INT64"
                  sql_body: "param_int64 + 1"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionWithArrayLiterals) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto statement, ParseDDLStatement(
                                           R"(
CREATE FUNCTION udf_array_literals(
  param_int_array ARRAY<INT64> DEFAULT [1, 2, 3],
  param_string_array ARRAY<STRING> DEFAULT ['x', 'y', 'xy'],
  param_empty_int_array ARRAY<INT64> DEFAULT ARRAY<INT64>[],
  param_empty_string_array ARRAY<STRING> DEFAULT ARRAY<STRING>[],
  param_untyped_array ARRAY<STRING> DEFAULT ARRAY['a', 'b', 'c'],
  param_nested_array ARRAY<ARRAY<INT64>> DEFAULT [[1, 2], [3, 4]]
) RETURNS INT64 SQL SECURITY INVOKER AS (ARRAY_LENGTH(param_int_array))
)"));

  EXPECT_THAT(statement, test::EqualsProto(R"pb(
                create_function {
                  function_name: "udf_array_literals"
                  function_kind: FUNCTION
                  sql_security: INVOKER
                  param {
                    name: "param_int_array"
                    param_typename: "ARRAY<INT64>"
                    default_value: "[1, 2, 3]"
                  }
                  param {
                    name: "param_string_array"
                    param_typename: "ARRAY<STRING>"
                    default_value: "['x', 'y', 'xy']"
                  }
                  param {
                    name: "param_empty_int_array"
                    param_typename: "ARRAY<INT64>"
                    default_value: "ARRAY<INT64>[]"
                  }
                  param {
                    name: "param_empty_string_array"
                    param_typename: "ARRAY<STRING>"
                    default_value: "ARRAY<STRING>[]"
                  }
                  param {
                    name: "param_untyped_array"
                    param_typename: "ARRAY<STRING>"
                    default_value: "ARRAY['a', 'b', 'c']"
                  }
                  param {
                    name: "param_nested_array"
                    param_typename: "ARRAY<ARRAY<INT64>>"
                    default_value: "[[1, 2], [3, 4]]"
                  }
                  return_typename: "INT64"
                  sql_body: "ARRAY_LENGTH(param_int_array)"
                  language: SQL
                }
              )pb"));
}

TEST(UserDefinedFunction, CreateFunctionInvalidSyntax) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  EXPECT_THAT(
      ParseDDLStatement(
          "CREATE FUNCTION T.func(a ARRAY<ANY>) RETURNS INT64 AS (NULL)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered 'ANY' while parsing")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo (,) RETURNS INT64 AS (NULL)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting ')' but found ','")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo (a,b) RETURNS INT64 AS (NULL)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered ',' while parsing")));

  EXPECT_THAT(ParseDDLStatement(
                  "CREATE FUNCTION foo (int64_t,int64) RETURNS INT64 AS (NULL)"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Encountered ',' while parsing")));

  EXPECT_THAT(ParseDDLStatement("CREATE FUNCTION foo () RETURNS INT64 AS NULL"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("Expecting '(' but found 'NULL'")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo () RETURNS INT64 AS (NULL;"),
      StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Expecting ')' but found an unknown character (';').")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION foo () RETURNS INT64 AS NULL);"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting '(' but found 'NULL'")));

  EXPECT_THAT(
      ParseDDLStatement(
          "CREATE FUNCTION foo () RETURNS INT64 SQL SECURITY FOOBAR AS (NULL)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'INVOKER' but found 'FOOBAR'")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION udf_1()) RETURNS INT64 SQL SECURITY "
                        "INVOKER AS (x+1)"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Expecting 'AS' but found ')'")));

  EXPECT_THAT(
      ParseDDLStatement("CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL "
                        "SECURITY INVOKER AS ()"),
      StatusIs(StatusCode::kInvalidArgument,
               HasSubstr("Encountered ')' while parsing: expression")));
}

TEST(UserDefinedFunction, CreateTVFFailures) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  EXPECT_THAT(
      ParseDDLStatement("CREATE TABLE FUNCTION foo(X INT64) AS ((SELECT X));"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement(
          "CREATE OR REPLACE TABLE FUNCTION foo(X INT64) AS ((SELECT X));"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST(UserDefinedFunction, DropFunctionBasic) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(ParseDDLStatement("DROP FUNCTION udf_1"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_function { function_name: "udf_1" function_kind: FUNCTION }
              )pb")));
}

TEST(UserDefinedFunction, DropFunctionIfExists) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(ParseDDLStatement("DROP FUNCTION IF EXISTS udf_1"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_function {
                  function_name: "udf_1"
                  function_kind: FUNCTION
                  existence_modifier: IF_EXISTS
                }
              )pb")));
}

TEST(UserDefinedFunction, DropFunctionInvalidSyntax) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = true;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(
      ParseDDLStatement("DROP FUNCTION udf_1()"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement("DROP FUNCTION foo()"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement("DROP FUNCTION foo(a)"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement("DROP FUNCTION foo(INT64)"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement("DROP FUNCTION foo(a INT64)"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement("DROP FUNCTION foo IF EXISTS"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));

  EXPECT_THAT(
      ParseDDLStatement("DROP TABLE FUNCTION foo"),
      StatusIs(StatusCode::kInvalidArgument, HasSubstr("Syntax error")));
}

TEST(UserDefinedFunction, DropFunctionNotSupportedWhenFlagOff) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_user_defined_functions = false;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  EXPECT_THAT(ParseDDLStatement("DROP FUNCTION foo"),
              StatusIs(StatusCode::kInvalidArgument,
                       HasSubstr("User defined functions are not supported")));
}

TEST(CreateLocalityGroup, CanParseCreateLocalityGroup) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(CREATE LOCALITY GROUP LocalityGroup
          OPTIONS ( storage = 'ssd', ssd_to_hdd_spill_timespan = '10d' ))sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_locality_group {
                  locality_group_name: "LocalityGroup"
                  set_options { option_name: "inflash" bool_value: true }
                  set_options {
                    option_name: "age_based_spill_policy"
                    string_list_value: "disk:10d"
                  }
                })pb")));
}

TEST(CreateLocalityGroup, CanParseCreateDefaultLocalityGroup) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(CREATE LOCALITY GROUP default
          OPTIONS ( storage = 'ssd', ssd_to_hdd_spill_timespan = '10d' ))sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                create_locality_group {
                  locality_group_name: "default"
                  set_options { option_name: "inflash" bool_value: true }
                  set_options {
                    option_name: "age_based_spill_policy"
                    string_list_value: "disk:10d"
                  }
                })pb")));
}

TEST(AlterLocalityGroup, CanParseAlterLocalityGroupSetOptions) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(ALTER LOCALITY GROUP IF EXISTS LocalityGroup
          SET OPTIONS ( storage = 'hdd', ssd_to_hdd_spill_timespan = '1d' ))sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_locality_group {
                  locality_group_name: "LocalityGroup"
                  set_options {
                    options { option_name: "inflash" bool_value: false }
                    options {
                      option_name: "age_based_spill_policy"
                      string_list_value: "disk:1d"
                    }
                  }
                  existence_modifier: IF_EXISTS
                })pb")));
}

TEST(AlterLocalityGroup, CanParseAlterDefaultLocalityGroup) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(ALTER LOCALITY GROUP IF EXISTS default
          SET OPTIONS ( storage = 'hdd', ssd_to_hdd_spill_timespan = '1d' ))sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                alter_locality_group {
                  locality_group_name: "default"
                  set_options {
                    options { option_name: "inflash" bool_value: false }
                    options {
                      option_name: "age_based_spill_policy"
                      string_list_value: "disk:1d"
                    }
                  }
                  existence_modifier: IF_EXISTS
                })pb")));
}

TEST(DropLocalityGroup, CanParseDropLocalityGroup) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(DROP LOCALITY GROUP LocalityGroup)sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        drop_locality_group { locality_group_name: "LocalityGroup" })pb")));
}

TEST(DropLocalityGroup, CanParseDropLocalityGroupIfExists) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(DROP LOCALITY GROUP IF EXISTS LocalityGroup)sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        drop_locality_group {
          locality_group_name: "LocalityGroup"
          existence_modifier: IF_EXISTS
        })pb")));
}

TEST(DropLocalityGroup, CanParseDropDefaultLocalityGroup) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(DROP LOCALITY GROUP IF EXISTS default)sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        drop_locality_group {
          locality_group_name: "default"
          existence_modifier: IF_EXISTS
        })pb")));
}

TEST(ParseCreatePropertyGraph, ParseCreatePropertyGraph) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE PROPERTY GRAPH graph
      NODE TABLES (nodes)
      EDGE TABLES (edges KEY (from_id, to_id)
        SOURCE KEY (from_id) REFERENCES nodes (id)
        DESTINATION KEY (to_id) REFERENCES nodes (id)
      )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_property_graph {
          name: "graph"
          ddl_body: "CREATE PROPERTY GRAPH graph\n"
                    "      NODE TABLES (nodes)\n"
                    "      EDGE TABLES (edges KEY (from_id, to_id)\n"
                    "        SOURCE KEY (from_id) REFERENCES nodes (id)\n"
                    "        DESTINATION KEY (to_id) REFERENCES nodes (id)\n"
                    "      )"
        })pb")));
}

TEST(ParseCreatePropertyGraph, ParseCreateOrReplacePropertyGraph) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE OR REPLACE PROPERTY GRAPH graph
      NODE TABLES (nodes)
      EDGE TABLES (edges KEY (from_id, to_id)
        SOURCE KEY (from_id) REFERENCES nodes (id)
        DESTINATION KEY (to_id) REFERENCES nodes (id)
      )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_property_graph {
          name: "graph"
          existence_modifier: OR_REPLACE
          ddl_body: "CREATE OR REPLACE PROPERTY GRAPH graph\n"
                    "      NODE TABLES (nodes)\n"
                    "      EDGE TABLES (edges KEY (from_id, to_id)\n"
                    "        SOURCE KEY (from_id) REFERENCES nodes (id)\n"
                    "        DESTINATION KEY (to_id) REFERENCES nodes (id)\n"
                    "      )"
        })pb")));
}

TEST(ParseCreatePropertyGraph, ParseCreateIfNotExistsPropertyGraph) {
  EXPECT_THAT(
      ParseDDLStatement(R"sql(
    CREATE PROPERTY GRAPH IF NOT EXISTS graph
      NODE TABLES (nodes)
      EDGE TABLES (edges KEY (from_id, to_id)
        SOURCE KEY (from_id) REFERENCES nodes (id)
        DESTINATION KEY (to_id) REFERENCES nodes (id)
      )
    )sql"),
      IsOkAndHolds(test::EqualsProto(R"pb(
        create_property_graph {
          name: "graph"
          existence_modifier: IF_NOT_EXISTS
          ddl_body: "CREATE PROPERTY GRAPH IF NOT EXISTS graph\n"
                    "      NODE TABLES (nodes)\n"
                    "      EDGE TABLES (edges KEY (from_id, to_id)\n"
                    "        SOURCE KEY (from_id) REFERENCES nodes (id)\n"
                    "        DESTINATION KEY (to_id) REFERENCES nodes (id)\n"
                    "      )"
        })pb")));
}

TEST(ParseDropPropertyGraph, ParseDropPropertyGraph) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    DROP PROPERTY GRAPH graph
    )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_property_graph { name: "graph" })pb")));
}

TEST(ParseDropPropertyGraph, ParseDropIfExistsPropertyGraph) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    DROP PROPERTY GRAPH IF EXISTS graph
    )sql"),
              IsOkAndHolds(test::EqualsProto(R"pb(
                drop_property_graph {
                  name: "graph"
                  existence_modifier: IF_EXISTS
                })pb")));
}

TEST(ParseCreateTable, CanParseCreateTableWithInterval) {
  EXPECT_THAT(ParseDDLStatement(
                  R"sql(
                    CREATE TABLE T (
                      K INT64 NOT NULL,
                      IntervalVal INTERVAL,
                      IntervalArr ARRAY<INTERVAL>,
                      IntervalStruct STRUCT<IntervalVal INTERVAL>
                    ) PRIMARY KEY (K)
                  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "T"
                      column { column_name: "K" type: INT64 not_null: true }
                      column { column_name: "IntervalVal" type: INTERVAL }
                      column {
                        column_name: "IntervalArr"
                        type: ARRAY
                        array_subtype { type: INTERVAL }
                      }
                      column {
                        column_name: "IntervalStruct"
                        type: STRUCT
                        type_definition {
                          type: STRUCT
                          struct_descriptor {
                            field {
                              name: "IntervalVal"
                              type { type: INTERVAL }
                            }
                          }
                        }
                      }
                      primary_key { key_name: "K" }
                    }
                  )pb")));
}

TEST(Comments, SupportComments) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE Albums (
      Id INT64 NOT NULL, /* block
comment */ Value Int64,
      Age Int64, -- inline comment
      Height Int64 # inline comment
    ) PRIMARY KEY (Id)
  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Albums"
                      column { column_name: "Id" type: INT64 not_null: true }
                      column { column_name: "Value" type: INT64 }
                      column { column_name: "Age" type: INT64 }
                      column { column_name: "Height" type: INT64 }
                      primary_key { key_name: "Id" }
                    }
                  )pb")));
}

TEST(CassandraType, SupportCassandraType) {
  EXPECT_THAT(ParseDDLStatement(R"sql(
    CREATE TABLE Albums (
      Id INT64 NOT NULL OPTIONS (cassandra_type = 'bigint'),
      Value STRING(MAX) OPTIONS (cassandra_type = 'varchar')
    ) PRIMARY KEY (Id)
  )sql"),
              IsOkAndHolds(test::EqualsProto(
                  R"pb(
                    create_table {
                      table_name: "Albums"
                      column { column_name: "Id" type: INT64 not_null: true }
                      column { column_name: "Value" type: STRING }
                      primary_key { key_name: "Id" }
                    }
                  )pb")));
}

}  // namespace

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
