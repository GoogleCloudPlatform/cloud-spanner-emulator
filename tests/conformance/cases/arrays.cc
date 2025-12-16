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

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/json.h"
#include "google/cloud/spanner/numeric.h"
#include "tests/conformance/common/database_test_base.h"

namespace google::spanner::emulator::test {

namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

class ArraysTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("arrays.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectArraysTest, ArraysTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ArraysTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ArraysTest, InsertBasicArraysSucceed) {
  Array<std::string> string_arr{"test", optional<std::string>()};
  Array<Bytes> bytes_arr{Bytes("1234"), optional<Bytes>()};
  Array<int64_t> num_arr{1, 2, 3};
  Array<Timestamp> timestamp_arr{Timestamp(), Timestamp(), Timestamp()};
  Array<Date> date_arr{Date(/*year=*/1, /*month=*/1, /*day=*/1)};

  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "string_array"}, {1, string_arr}));
  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "bytes_array"}, {2, bytes_arr}));
  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "num_array"}, {3, num_arr}));
  ZETASQL_EXPECT_OK(
      Insert("test_table", {"id", "timestamp_array"}, {4, timestamp_arr}));
  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "date_array"}, {5, date_arr}));

  EXPECT_THAT(
      ReadAll("test_table", {"id", "string_array", "bytes_array", "num_array",
                             "timestamp_array", "date_array"}),
      IsOkAndHoldsRows({
          {1, string_arr, Null<Array<Bytes>>(), Null<Array<std::int64_t>>(),
           Null<Array<Timestamp>>(), Null<Array<Date>>()},
          {2, Null<Array<std::string>>(), bytes_arr,
           Null<Array<std::int64_t>>(), Null<Array<Timestamp>>(),
           Null<Array<Date>>()},
          {3, Null<Array<std::string>>(), Null<Array<Bytes>>(), num_arr,
           Null<Array<Timestamp>>(), Null<Array<Date>>()},
          {4, Null<Array<std::string>>(), Null<Array<Bytes>>(),
           Null<Array<std::int64_t>>(), timestamp_arr, Null<Array<Date>>()},
          {5, Null<Array<std::string>>(), Null<Array<Bytes>>(),
           Null<Array<std::int64_t>>(), Null<Array<Timestamp>>(), date_arr},
      }));
}

TEST_P(ArraysTest, InsertJsonArraysSucceed) {
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    Array<Json> json_arr{Json("{\"intkey\":123}"), Json("{\"boolkey\":true}"),
                         Json("{\"strkey\":\"strval\"}")};

    ZETASQL_EXPECT_OK(Insert("test_table", {"id", "json_array"}, {1, json_arr}));
    EXPECT_THAT(ReadAll("test_table", {"id", "json_array"}),
                IsOkAndHoldsRows({{1, json_arr}}));
  } else {
    Array<cloud::spanner::JsonB> json_arr{
        cloud::spanner::JsonB("{\"intkey\": 123}"),
        cloud::spanner::JsonB("{\"boolkey\": true}"),
        cloud::spanner::JsonB("{\"strkey\": \"strval\"}")};

    ZETASQL_EXPECT_OK(Insert("test_table", {"id", "json_array"}, {1, json_arr}));
    EXPECT_THAT(ReadAll("test_table", {"id", "json_array"}),
                IsOkAndHoldsRows({{1, json_arr}}));
  }
}

TEST_P(ArraysTest, InsertNumericArraysSucceed) {
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    Array<Numeric> numeric_arr{
        cloud::spanner::MakeNumeric("-124523523.235235").value(),
        cloud::spanner::MakeNumeric("0.000000001").value(),
        cloud::spanner::MakeNumeric("99999999999999999999999999999.999999999")
            .value()};

    ZETASQL_EXPECT_OK(Insert("test_table", {"id", "numeric_array"}, {1, numeric_arr}));
    EXPECT_THAT(ReadAll("test_table", {"id", "numeric_array"}),
                IsOkAndHoldsRows({{1, numeric_arr}}));
  } else {
    Array<cloud::spanner::PgNumeric> numeric_arr{
        cloud::spanner::MakePgNumeric("-124523523.235235").value(),
        cloud::spanner::MakePgNumeric("0.000000001").value(),
        cloud::spanner::MakePgNumeric("99999999999999999999999999999.999999999")
            .value()};

    ZETASQL_EXPECT_OK(Insert("test_table", {"id", "numeric_array"}, {1, numeric_arr}));
    EXPECT_THAT(ReadAll("test_table", {"id", "numeric_array"}),
                IsOkAndHoldsRows({{1, numeric_arr}}));
  }
}

TEST_P(ArraysTest, InsertOrUpdateArraySucceeds) {
  Array<std::string> old_string_arr{"value1", "value2", "value3"};
  Array<std::string> new_string_arr{"new-value1", "new-value2", "new-value3"};
  Array<std::int64_t> num_arr{1, 2, 3};

  ZETASQL_EXPECT_OK(InsertOrUpdate("test_table", {"id", "string_array"},
                           {1, old_string_arr}));
  EXPECT_THAT(ReadAll("test_table", {"id", "string_array"}),
              IsOkAndHoldsRows({{1, old_string_arr}}));
  ZETASQL_EXPECT_OK(InsertOrUpdate("test_table", {"id", "string_array", "num_array"},
                           {1, new_string_arr, num_arr}));
  EXPECT_THAT(ReadAll("test_table", {"id", "string_array", "num_array"}),
              IsOkAndHoldsRows({{1, new_string_arr, num_arr}}));
}

TEST_P(ArraysTest, UpdateArraySucceeds) {
  Array<std::string> old_string_arr{"value1", "value2", "value3"};
  Array<std::string> new_string_arr{"new-value1", "new-value2", "new-value3"};
  Array<std::int64_t> num_arr{1, 2, 3};

  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "string_array", "num_array"},
                   {1, old_string_arr, num_arr}));
  EXPECT_THAT(ReadAll("test_table", {"id", "string_array", "num_array"}),
              IsOkAndHoldsRows({{1, old_string_arr, num_arr}}));
  ZETASQL_EXPECT_OK(Update("test_table", {"id", "string_array"}, {1, new_string_arr}));
  EXPECT_THAT(ReadAll("test_table", {"id", "string_array", "num_array"}),
              IsOkAndHoldsRows({{1, new_string_arr, num_arr}}));
}

TEST_P(ArraysTest, ReplaceArraySucceeds) {
  Array<std::string> old_string_arr{"value1", "value2", "value3"};
  Array<std::string> new_string_arr{"new-value1", "new-value2", "new-value3"};
  Array<std::int64_t> num_arr{1, 2, 3};

  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "string_array", "num_array"},
                   {1, old_string_arr, num_arr}));
  EXPECT_THAT(ReadAll("test_table", {"id", "string_array", "num_array"}),
              IsOkAndHoldsRows({{1, old_string_arr, num_arr}}));
  ZETASQL_EXPECT_OK(Replace("test_table", {"id", "string_array"}, {1, new_string_arr}));
  EXPECT_THAT(
      ReadAll("test_table", {"id", "string_array", "num_array"}),
      IsOkAndHoldsRows({{1, new_string_arr, Null<Array<std::int64_t>>()}}));
}

TEST_P(ArraysTest, InsertArrayThatExceedsSizeLimitFails) {
  Array<std::string> string_arr{"1234567890abcdefghijklmnopqrstuvwxyz"};

  EXPECT_THAT(Insert("test_table", {"id", "string_array"}, {1, string_arr}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ArraysTest, InsertArrayWithMaxDataPerColumnSucceeds) {
  // Attempt to insert an array that is exactly 10MB.
  std::string str(250000, 'a');
  Array<std::string> string_arr{str, str, str, str};
  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "max_string_array"}, {1, string_arr}));
}

TEST_P(ArraysTest, InsertArrayThatExceedsMaxDataPerColumnFails) {
  // Attempt to insert an array larger than 10MB.
  // TODO: Add in test for an insert that exceeds 10MB after we have
  // correct value size validator.
}

TEST_P(ArraysTest, InsertInvalidDateArrayFails) {
  Array<Date> date_arr{Date(/*year=*/0, /*month=*/0, /*day=*/0)};

  EXPECT_THAT(Insert("test_table", {"id", "date_array"}, {1, date_arr}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(ArraysTest, InsertEmptyArraysSucceed) {
  Array<std::string> string_arr{};
  Array<Bytes> bytes_arr{};
  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "string_array"}, {1, string_arr}));
  ZETASQL_EXPECT_OK(Insert("test_table", {"id", "bytes_array"}, {2, bytes_arr}));

  // Read back all rows.
  EXPECT_THAT(ReadAll("test_table", {"id", "string_array", "bytes_array"}),
              IsOkAndHoldsRows({{1, string_arr, Null<Array<Bytes>>()},
                                {2, Null<Array<std::string>>(), bytes_arr}}));
}

TEST_P(ArraysTest, InsertArraysWithVectorLengthSucceed) {
  Array<double> double_arr{1.1, 1.2};
  Array<float> float_arr{2.1, 2.2};
  ZETASQL_EXPECT_OK(Insert("vector_length_limits_table", {"pk", "arr_double"},
                   {2, std::move(double_arr)}));
  ZETASQL_EXPECT_OK(Insert("vector_length_limits_table", {"pk", "arr_float"},
                   {3, std::move(float_arr)}));
}

TEST_P(ArraysTest, InsertArraysWithVectorLengthWithNullFail) {
  Array<double> double_arr{1.1, std::nullopt};
  Array<float> float_arr{2.1, std::nullopt};
  EXPECT_THAT(
      Insert("vector_length_limits_table", {"pk", "arr_double"},
             {2, std::move(double_arr)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(" has `vector_length`, and Null is not allowed")));
  EXPECT_THAT(
      Insert("vector_length_limits_table", {"pk", "arr_float"},
             {3, std::move(float_arr)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(" has `vector_length`, and Null is not allowed")));
}

TEST_P(ArraysTest, InsertArraysWithVectorLengthLessThanVectorLengthFail) {
  Array<double> double_arr{1.1};
  Array<float> float_arr{2.1};
  EXPECT_THAT(Insert("vector_length_limits_table", {"pk", "arr_double"},
                     {2, std::move(double_arr)}),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(Insert("vector_length_limits_table", {"pk", "arr_float"},
                     {3, std::move(float_arr)}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(ArraysTest, InsertArraysWithVectorLengthExceedsVectorLengthFail) {
  Array<double> double_arr{1.1, 1.2, 1.3};
  Array<float> float_arr{2.1, 2.2, 2.3, 2.4};
  EXPECT_THAT(Insert("vector_length_limits_table", {"pk", "arr_double"},
                     {2, std::move(double_arr)}),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(Insert("vector_length_limits_table", {"pk", "arr_float"},
                     {3, std::move(float_arr)}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(ArraysTest, InsertInvalidUTFStringArrayFails) {
  // TODO: Find a way to test this. Currently it fails on the
  // client library, so the emulator doesn't even see it.
}

TEST_P(ArraysTest, ArrayTransform) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_EXPECT_OK(Query("SELECT ARRAY_TRANSFORM([1, 2, 3], e -> e * 2) AS x"));
}

}  // namespace

}  // namespace google::spanner::emulator::test
