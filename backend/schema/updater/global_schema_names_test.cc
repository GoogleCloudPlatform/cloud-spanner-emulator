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
#include "zetasql/base/status.h"

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
  names.RemoveName("albums");  // Case-insensitive.
  ZETASQL_EXPECT_OK(names.AddName("Table", "Albums"));
}

TEST(GlobalSchemaNames, GenerateForeignKeyName) {
  GlobalSchemaNames names;
  auto status = names.GenerateForeignKeyName("Albums", "Singers");
  ZETASQL_EXPECT_OK(status);
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

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
