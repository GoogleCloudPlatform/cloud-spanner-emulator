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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "common/errors.h"
#include "common/limits.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using testing::Eq;
using testing::Not;
using zetasql_base::testing::IsOk;

TEST(GlobalSchemaNames, AddName) {
  GlobalSchemaNames names;
  ZETASQL_EXPECT_OK(names.AddName("Table", "Albums"));
  ZETASQL_EXPECT_OK(names.AddName("Table", "Singers"));
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
  ZETASQL_EXPECT_OK(names.AddName("Table", "Albums"));
  EXPECT_TRUE(names.HasName("Albums"));
  names.RemoveName("albums");  // Case-insensitive.
  EXPECT_FALSE(names.HasName("Albums"));
  ZETASQL_EXPECT_OK(names.AddName("Table", "Albums"));
  EXPECT_TRUE(names.HasName("Albums"));
}

TEST(GlobalSchemaNames, GenerateForeignKeyName) {
  GlobalSchemaNames names;
  auto status = names.GenerateForeignKeyName("Albums", "Singers");
  ZETASQL_EXPECT_OK(status);
  EXPECT_TRUE(names.HasName(status.value()));
  EXPECT_THAT(status.value(), Eq("FK_Albums_Singers_5FB395005BB87272_1"));

  // Same tables.
  status = names.GenerateForeignKeyName("Albums", "Singers");
  ZETASQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(), Eq("FK_Albums_Singers_5FB395005BB87272_2"));

  // Different tables.
  status = names.GenerateForeignKeyName("Albums", "Songs");
  ZETASQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(), Eq("FK_Albums_Songs_42ABDA0A1D54791A_1"));

  // Truncate long names.
  std::string long_referencing_name(limits::kMaxSchemaIdentifierLength / 4,
                                    'x');
  std::string long_referenced_name(limits::kMaxSchemaIdentifierLength, 'y');
  status = names.GenerateForeignKeyName(
      std::string(limits::kMaxSchemaIdentifierLength / 4, 'x'),
      std::string(limits::kMaxSchemaIdentifierLength, 'y'));
  ZETASQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("FK_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_"
                 "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
                 "yyyyyyyyyyyy_F02578DFC8500A04_1"));
  EXPECT_THAT(status.value().size(), Eq(limits::kMaxSchemaIdentifierLength));

  // Empty tables names.
  EXPECT_THAT(names.GenerateForeignKeyName("", "Songs"), Not(IsOk()));
  EXPECT_THAT(names.GenerateForeignKeyName("Albums", ""), Not(IsOk()));
}

TEST(GlobalSchemaNames, GenerateManagedIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/false,
                                     /*unique=*/false);
  ZETASQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_09F682A0D8AF2F47"));
}

TEST(GlobalSchemaNames, GenerateManagedNullFilteredIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/true,
                                     /*unique=*/false);
  ZETASQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_N_5849069C505A683F"));
}

TEST(GlobalSchemaNames, GenerateManagedUniqueIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/false,
                                     /*unique=*/true);
  ZETASQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_U_E3AF278F4A7F7E44"));
}

TEST(GlobalSchemaNames, GenerateManagedNullFilteredUniqueIndexName) {
  GlobalSchemaNames names;
  auto status =
      names.GenerateManagedIndexName("Songs", {"FirstName", "LastName"},
                                     /*null_filtered=*/true,
                                     /*unique=*/true);
  ZETASQL_EXPECT_OK(status);
  EXPECT_THAT(status.value(),
              Eq("IDX_Songs_FirstName_LastName_U_E3AF278F4A7F7E44"));
}

TEST(GlobalSchemaNames, ValidateSchemaName) {
  ZETASQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Table", "Albums"));

  EXPECT_THAT(GlobalSchemaNames::ValidateSchemaName("Table", ""), Not(IsOk()));
  EXPECT_THAT(GlobalSchemaNames::ValidateSchemaName("Table", "_Albums"),
              Eq(error::InvalidSchemaName("Table", "_Albums")));

  std::string max_name(limits::kMaxSchemaIdentifierLength, 'x');
  ZETASQL_EXPECT_OK(GlobalSchemaNames::ValidateSchemaName("Table", max_name));

  std::string long_name(limits::kMaxSchemaIdentifierLength + 1, 'x');
  EXPECT_THAT(GlobalSchemaNames::ValidateSchemaName("Table", long_name),
              Eq(error::InvalidSchemaName("Table", long_name)));
}

TEST(GlobalSchemaNames, ValidateConstraintName) {
  ZETASQL_EXPECT_OK(GlobalSchemaNames::ValidateConstraintName("Albums", "Foreign Key",
                                                      "FK_C"));
  EXPECT_THAT(GlobalSchemaNames::ValidateConstraintName("Albums", "Foreign Key",
                                                        "PK_C"),
              Eq(error::InvalidConstraintName("Foreign Key", "PK_C", "PK_")));
  EXPECT_THAT(GlobalSchemaNames::ValidateConstraintName("Albums", "Foreign Key",
                                                        "CK_IS_NOT_NULL_C"),
              Eq(error::InvalidConstraintName("Foreign Key", "CK_IS_NOT_NULL_C",
                                              "CK_IS_NOT_NULL_")));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
