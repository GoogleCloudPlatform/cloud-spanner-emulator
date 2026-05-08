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

#include "backend/schema/updater/global_schema_names.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "common/errors.h"
#include "common/limits.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using testing::Eq;
using testing::Not;
using googlesql_base::testing::IsOk;

TEST(GlobalSchemaNames, AddName) {
  GlobalSchemaNames names;
  GOOGLESQL_EXPECT_OK(names.AddName("Table", "Albums"));
  GOOGLESQL_EXPECT_OK(names.AddName("Table", "Singers"));
  EXPECT_TRUE(names.HasName("Albums"));
  EXPECT_TRUE(names.HasName("Singers"));

  // Names must be unique regardless of the type of schema object.
  EXPECT_THAT(names.AddName("Index", "Albums"),
              Eq(error::SchemaObjectAlreadyExists("Index", "Albums")));

  // Names are case-insensitive.
  EXPECT_THAT(names.AddName("Table", "albums"),
              Eq(error::SchemaObjectAlreadyExists("Table", "albums")));
}

TEST(GlobalSchemaNames, RemoveName) {
  GlobalSchemaNames names;
  GOOGLESQL_EXPECT_OK(names.AddName("Table", "Albums"));
  EXPECT_TRUE(names.HasName("Albums"));
  names.RemoveName("albums");  // Case-insensitive.
  EXPECT_FALSE(names.HasName("Albums"));
  GOOGLESQL_EXPECT_OK(names.AddName("Table", "Albums"));
  EXPECT_TRUE(names.HasName("Albums"));
}

TEST(GlobalSchemaNames, GenerateForeignKeyName) {
  GlobalSchemaNames names;
  auto status = names.GenerateForeignKeyName("Albums", "Singers");
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_TRUE(names.HasName(status.value()));
  EXPECT_THAT(status.value(), Eq("FK_Albums_Singers_5FB395005BB87272_1"));

  // Same tables.
  status = names.GenerateForeignKeyName("Albums", "Singers");
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(), Eq("FK_Albums_Singers_5FB395005BB87272_2"));

  // Different tables.
  status = names.GenerateForeignKeyName("Albums", "Songs");
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(), Eq("FK_Albums_Songs_42ABDA0A1D54791A_1"));

  // Truncate long names.
  std::string long_referencing_name(limits::kMaxSchemaIdentifierLength / 4,
                                    'x');
  std::string long_referenced_name(limits::kMaxSchemaIdentifierLength, 'y');
  status = names.GenerateForeignKeyName(
      std::string(limits::kMaxSchemaIdentifierLength / 4, 'x'),
      std::string(limits::kMaxSchemaIdentifierLength, 'y'));
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("FK_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_"
                 "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
                 "yyyyyyyyyyyy_F02578DFC8500A04_1"));
  EXPECT_THAT(status.value().size(), Eq(limits::kMaxSchemaIdentifierLength));

  // Empty tables names.
  EXPECT_THAT(names.GenerateForeignKeyName("", "Songs"), Not(IsOk()));
  EXPECT_THAT(names.GenerateForeignKeyName("Albums", ""), Not(IsOk()));

  // Remove schemas for the base name. Append the schema for the full name.
  status = names.GenerateForeignKeyName("Schema1.Albums", "Schema2.Songs");
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(), Eq("Schema1.FK_Albums_Songs_04F0A18F8F9162C0_1"));
}

TEST(GlobalSchemaNames, GenerateManagedIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/false,
                                     /*unique=*/false);
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_09F682A0D8AF2F47"));

  status =
      names.GenerateManagedIndexName("Schema1.Albums", {"Songs", "Artists"},
                                     /*null_filtered=*/false, /*unique=*/false);
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("Schema1.IDX_Albums_Songs_Artists_013336CDE9D3087F"));
}

TEST(GlobalSchemaNames, GenerateManagedNullFilteredIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/true,
                                     /*unique=*/false);
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_N_5849069C505A683F"));
}

TEST(GlobalSchemaNames, GenerateManagedUniqueIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/false,
                                     /*unique=*/true);
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_U_E3AF278F4A7F7E44"));
}

TEST(GlobalSchemaNames, GenerateManagedNullFilteredUniqueIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/true,
                                     /*unique=*/true);
  GOOGLESQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_U_E3AF278F4A7F7E44"));
}

TEST(GlobalSchemaNames, ValidateSchemaName) {
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Table", "Albums"));

  EXPECT_THAT(GlobalSchemaNames::ValidateSchemaName("Table", ""), Not(IsOk()));
  EXPECT_THAT(GlobalSchemaNames::ValidateSchemaName("Table", "_Albums"),
              Eq(error::InvalidSchemaName("Table", "_Albums")));

  std::string max_name(limits::kMaxSchemaIdentifierLength, 'x');
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Table", max_name));

  std::string long_name(limits::kMaxSchemaIdentifierLength + 1, 'x');
  EXPECT_THAT(GlobalSchemaNames::ValidateSchemaName("Table", long_name),
              Eq(error::InvalidSchemaName("Table", long_name)));

  EXPECT_THAT(
      GlobalSchemaNames::ValidateSchemaName("Table", "hte_isdirtytest$y"),
      Eq(error::InvalidSchemaName("Table", "hte_isdirtytest$y")));
}

TEST(GlobalSchemaNames, ValidateConstraintName) {
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateConstraintName("Albums", "Foreign Key",
                                                      "FK_C"));
  EXPECT_THAT(GlobalSchemaNames::ValidateConstraintName("Albums", "Foreign Key",
                                                        "PK_C"),
              Eq(error::InvalidConstraintName("Foreign Key", "PK_C", "PK_")));
  EXPECT_THAT(GlobalSchemaNames::ValidateConstraintName("Albums", "Foreign Key",
                                                        "CK_IS_NOT_NULL_C"),
              Eq(error::InvalidConstraintName("Foreign Key", "CK_IS_NOT_NULL_C",
                                              "CK_IS_NOT_NULL_")));
}

TEST(GlobalSchemaNames, ValidateNamedSchemaName) {
  // Valid named schema names.
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateNamedSchemaName("Albums"));
  EXPECT_THAT(GlobalSchemaNames::ValidateNamedSchemaName("_Albums"),
              Eq(error::InvalidSchemaName("Schema", "_Albums")));
  EXPECT_THAT(GlobalSchemaNames::ValidateNamedSchemaName("pg_catalog"),
              Eq(error::InvalidSchemaName("Schema", "pg_catalog")));

  // Validate names of SDL types in named schemas.
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Table", "Albums.Songs"));
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Synonym", "Albums.Songs"));
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Sequence", "Albums.Seq"));
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("View", "Albums.Songs"));
  GOOGLESQL_EXPECT_OK(
      GlobalSchemaNames::ValidateSchemaName("Index", "Albums.SongsIndex"));
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Udf", "Albums.SongsUdf"));
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Foreign Key",
                                                  "Albums.FK_Songs_Artists"));
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Check Constraint",
                                                  "Albums.CK_Songs_Artists"));
  GOOGLESQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Property Graph",
                                                  "Albums.ArtistsGraph"));

  EXPECT_THAT(GlobalSchemaNames::ValidateSchemaName("Schema", "Albums.Artists"),
              Eq(error::SchemaObjectTypeUnsupportedInNamedSchema(
                  "Schema", "Albums.Artists")));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
