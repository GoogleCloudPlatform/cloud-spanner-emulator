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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class ArraysTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE TestTable(
        ID             INT64 NOT NULL,
        StringArray    ARRAY<STRING(30)>,
        BytesArray     ARRAY<BYTES(30)>,
        NumArray       ARRAY<INT64>,
        MaxStringArray ARRAY<STRING(MAX)>,
        TimestampArray ARRAY<TIMESTAMP>,
        NumericArray   ARRAY<NUMERIC>,
        DateArray      ARRAY<DATE>,
      ) PRIMARY KEY (ID)
    )"});
  }
};

TEST_F(ArraysTest, InsertBasicArraysSucceed) {
  Array<std::string> string_arr{"test", optional<std::string>()};
  Array<Bytes> bytes_arr{Bytes("1234"), optional<Bytes>()};
  Array<std::int64_t> num_arr{1, 2, 3};
  Array<Timestamp> timestamp_arr{Timestamp(), Timestamp(), Timestamp()};
  Array<Date> date_arr{Date(/*year=*/1, /*month=*/1, /*day=*/1)};
  Array<Numeric> numeric_arr{
      cloud::spanner::MakeNumeric("-124523523.235235").value(),
      cloud::spanner::MakeNumeric("0.000000001").value(),
      cloud::spanner::MakeNumeric("99999999999999999999999999999.999999999")
          .value()};

  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "StringArray"}, {1, string_arr}));
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "BytesArray"}, {2, bytes_arr}));
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "NumArray"}, {3, num_arr}));
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "TimestampArray"}, {4, timestamp_arr}));
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "DateArray"}, {5, date_arr}));
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "NumericArray"}, {6, numeric_arr}));

  EXPECT_THAT(
      ReadAll("TestTable", {"ID", "StringArray", "BytesArray", "NumArray",
                            "TimestampArray", "DateArray", "NumericArray"}),
      IsOkAndHoldsRows({
          {1, string_arr, Null<Array<Bytes>>(), Null<Array<std::int64_t>>(),
           Null<Array<Timestamp>>(), Null<Array<Date>>(),
           Null<Array<Numeric>>()},
          {2, Null<Array<std::string>>(), bytes_arr,
           Null<Array<std::int64_t>>(), Null<Array<Timestamp>>(),
           Null<Array<Date>>(), Null<Array<Numeric>>()},
          {3, Null<Array<std::string>>(), Null<Array<Bytes>>(), num_arr,
           Null<Array<Timestamp>>(), Null<Array<Date>>(),
           Null<Array<Numeric>>()},
          {4, Null<Array<std::string>>(), Null<Array<Bytes>>(),
           Null<Array<std::int64_t>>(), timestamp_arr, Null<Array<Date>>(),
           Null<Array<Numeric>>()},
          {5, Null<Array<std::string>>(), Null<Array<Bytes>>(),
           Null<Array<std::int64_t>>(), Null<Array<Timestamp>>(), date_arr,
           Null<Array<Numeric>>()},
          {6, Null<Array<std::string>>(), Null<Array<Bytes>>(),
           Null<Array<std::int64_t>>(), Null<Array<Timestamp>>(),
           Null<Array<Date>>(), numeric_arr},
      }));
}

TEST_F(ArraysTest, InsertOrUpdateArraySucceeds) {
  Array<std::string> old_string_arr{"value1", "value2", "value3"};
  Array<std::string> new_string_arr{"new-value1", "new-value2", "new-value3"};
  Array<std::int64_t> num_arr{1, 2, 3};

  ZETASQL_EXPECT_OK(
      InsertOrUpdate("TestTable", {"ID", "StringArray"}, {1, old_string_arr}));
  EXPECT_THAT(ReadAll("TestTable", {"ID", "StringArray"}),
              IsOkAndHoldsRows({{1, old_string_arr}}));
  ZETASQL_EXPECT_OK(InsertOrUpdate("TestTable", {"ID", "StringArray", "NumArray"},
                           {1, new_string_arr, num_arr}));
  EXPECT_THAT(ReadAll("TestTable", {"ID", "StringArray", "NumArray"}),
              IsOkAndHoldsRows({{1, new_string_arr, num_arr}}));
}

TEST_F(ArraysTest, UpdateArraySucceeds) {
  Array<std::string> old_string_arr{"value1", "value2", "value3"};
  Array<std::string> new_string_arr{"new-value1", "new-value2", "new-value3"};
  Array<std::int64_t> num_arr{1, 2, 3};

  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "StringArray", "NumArray"},
                   {1, old_string_arr, num_arr}));
  EXPECT_THAT(ReadAll("TestTable", {"ID", "StringArray", "NumArray"}),
              IsOkAndHoldsRows({{1, old_string_arr, num_arr}}));
  ZETASQL_EXPECT_OK(Update("TestTable", {"ID", "StringArray"}, {1, new_string_arr}));
  EXPECT_THAT(ReadAll("TestTable", {"ID", "StringArray", "NumArray"}),
              IsOkAndHoldsRows({{1, new_string_arr, num_arr}}));
}

TEST_F(ArraysTest, ReplaceArraySucceeds) {
  Array<std::string> old_string_arr{"value1", "value2", "value3"};
  Array<std::string> new_string_arr{"new-value1", "new-value2", "new-value3"};
  Array<std::int64_t> num_arr{1, 2, 3};

  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "StringArray", "NumArray"},
                   {1, old_string_arr, num_arr}));
  EXPECT_THAT(ReadAll("TestTable", {"ID", "StringArray", "NumArray"}),
              IsOkAndHoldsRows({{1, old_string_arr, num_arr}}));
  ZETASQL_EXPECT_OK(Replace("TestTable", {"ID", "StringArray"}, {1, new_string_arr}));
  EXPECT_THAT(
      ReadAll("TestTable", {"ID", "StringArray", "NumArray"}),
      IsOkAndHoldsRows({{1, new_string_arr, Null<Array<std::int64_t>>()}}));
}

TEST_F(ArraysTest, InsertArrayThatExceedsSizeLimitFails) {
  Array<std::string> string_arr{"1234567890abcdefghijklmnopqrstuvwxyz"};
  Array<Bytes> bytes_arr{Bytes("1234567890abcdefghijklmnopqrstuvwxyz")};

  EXPECT_THAT(Insert("TestTable", {"ID", "StringArray"}, {1, string_arr}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_THAT(Insert("TestTable", {"ID", "BytesArray"}, {2, bytes_arr}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ArraysTest, InsertArrayWithMaxDataPerColumnSucceeds) {
  // Attempt to insert an array that is exactly 10MB.
  std::string str(250000, 'a');
  Array<std::string> string_arr{str, str, str, str};
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "MaxStringArray"}, {1, string_arr}));
}

TEST_F(ArraysTest, InsertArrayThatExceedsMaxDataPerColumnFails) {
  // Attempt to insert an array larger than 10MB.
  // TODO: Add in test for an insert that exceeds 10MB after we have
  // correct value size validator.
}

TEST_F(ArraysTest, InsertInvalidDateArrayFails) {
  Array<Date> date_arr{Date(/*year=*/0, /*month=*/0, /*day=*/0)};

  EXPECT_THAT(Insert("TestTable", {"ID", "DateArray"}, {1, date_arr}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(ArraysTest, InsertEmptyArraysSucceed) {
  Array<std::string> string_arr{};
  Array<Bytes> bytes_arr{};
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "StringArray"}, {1, string_arr}));
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID", "BytesArray"}, {2, bytes_arr}));

  // Read back all rows.
  EXPECT_THAT(ReadAll("TestTable", {"ID", "StringArray", "BytesArray"}),
              IsOkAndHoldsRows({{1, string_arr, Null<Array<Bytes>>()},
                                {2, Null<Array<std::string>>(), bytes_arr}}));
}

TEST_F(ArraysTest, InsertInvalidUTFStringArrayFails) {
  // TODO: Find a way to test this. Currently it fails on the
  // client library, so the emulator doesn't even see it.
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
