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

#include "backend/datamodel/types.h"

#include <vector>

#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"

using ::postgres_translator::spangres::datatypes::GetPgJsonbType;
using ::postgres_translator::spangres::datatypes::GetPgNumericType;

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

class TypesTest : public ::testing::Test {
 public:
  TypesTest() = default;

  std::vector<const zetasql::Type*> supported_types() {
    return {
        type_factory_.get_int64(),     type_factory_.get_bool(),
        type_factory_.get_double(),    type_factory_.get_string(),
        type_factory_.get_bytes(),     type_factory_.get_date(),
        type_factory_.get_timestamp(), type_factory_.get_numeric(),
        type_factory_.get_json(),      type_factory_.get_float(),
        type_factory_.get_uuid(),
    };
  }

  std::vector<const zetasql::Type*> unsupported_types() {
    std::vector<const zetasql::Type*> unsupported_types = {
        type_factory_.get_int32(),
        type_factory_.get_interval(),
    };

    return unsupported_types;
  }

  std::vector<const zetasql::Type*> supported_key_types() {
    return {
        type_factory_.get_int64(),     type_factory_.get_bool(),
        type_factory_.get_double(),    type_factory_.get_string(),
        type_factory_.get_bytes(),     type_factory_.get_date(),
        type_factory_.get_timestamp(), type_factory_.get_numeric(),
        type_factory_.get_uuid(),
    };
  }

  std::vector<const zetasql::Type*> unsupported_key_types() {
    return {
        type_factory_.get_float(), type_factory_.get_tokenlist(),
        type_factory_.get_json(),  GetPgJsonbType(),
        GetPgNumericType(),
    };
  }

 protected:
  zetasql::TypeFactory type_factory_;
};

TEST_F(TypesTest, SupportedColumnType) {
  for (const zetasql::Type* type : supported_types()) {
    EXPECT_TRUE(IsSupportedColumnType(type));
    const zetasql::ArrayType* array_type;
    ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(type, &array_type));
    EXPECT_TRUE(IsSupportedColumnType(array_type));
  }

  for (const zetasql::Type* type : unsupported_types()) {
    EXPECT_FALSE(IsSupportedColumnType(type));
  }
}

void TestArrayTypesForSupportedKeyColumnType(
    zetasql::TypeFactory* type_factory, const zetasql::Type* type) {
  const zetasql::ArrayType* array_type;
  ZETASQL_ASSERT_OK(type_factory->MakeArrayType(type, &array_type));
  EXPECT_FALSE(IsSupportedKeyColumnType(array_type));
}

TEST_F(TypesTest, SupportedKeyColumnType) {
  for (const zetasql::Type* type : supported_key_types()) {
    EXPECT_TRUE(IsSupportedKeyColumnType(type));
    TestArrayTypesForSupportedKeyColumnType(&type_factory_, type);
  }

  for (const zetasql::Type* type : unsupported_key_types()) {
    EXPECT_FALSE(IsSupportedKeyColumnType(type));
    TestArrayTypesForSupportedKeyColumnType(&type_factory_, type);
  }
}
}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
