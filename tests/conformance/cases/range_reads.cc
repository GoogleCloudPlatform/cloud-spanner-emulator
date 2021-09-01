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
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class RangeReadsTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    ZETASQL_RETURN_IF_ERROR(SetSchema({R"(
      CREATE TABLE Users(
        ID   INT64,
        Name STRING(MAX),
        Age  INT64
      ) PRIMARY KEY (ID)
    )",
                               R"(
      CREATE TABLE NumericTable(
        key   NUMERIC,
        val STRING(MAX)
      ) PRIMARY KEY (key)
    )"}));
    return absl::OkStatus();
  }

 protected:
  void PopulateDatabase() {
    // Write fixure data to use in reads.
    ZETASQL_EXPECT_OK(MultiInsert("Users", {"ID", "Name", "Age"},
                          {{Null<std::int64_t>(), "Adam", 20},
                           {1, "John", 22},
                           {2, "Peter", 41},
                           {4, "Matthew", 33},
                           {5, Null<std::string>(), 18}}));
  }

  void PopulateNumericTable() {
    ZETASQL_EXPECT_OK(MultiInsert("NumericTable", {"key", "val"},
                          {
                              {Null<Numeric>(), "null"},
                              {minNumeric(), "min"},
                              {negativeNumeric(), "neg"},
                              {zeroNumeric(), "zero"},
                              {positiveNumeric(), "pos"},
                              {maxNumeric(), "max"},
                          }));
  }

  Numeric minNumeric() {
    return cloud::spanner::MakeNumeric(
               "-99999999999999999999999999999.999999999")
        .value();
  }

  Numeric negativeNumeric() {
    return cloud::spanner::MakeNumeric("-1.23").value();
  }

  Numeric maxNumeric() {
    return cloud::spanner::MakeNumeric(
               "99999999999999999999999999999.999999999")
        .value();
  }

  Numeric zeroNumeric() { return cloud::spanner::MakeNumeric("0").value(); }

  Numeric positiveNumeric() {
    return cloud::spanner::MakeNumeric("1.23").value();
  }
};

TEST_F(RangeReadsTest, CanReadAllKeyrange) {
  PopulateDatabase();

  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20},
                                {1, "John", 22},
                                {2, "Peter", 41},
                                {4, "Matthew", 33},
                                {5, Null<std::string>(), 18}}));
}

TEST_F(RangeReadsTest, CanReadPointKey) {
  PopulateDatabase();

  KeySet key_set;
  key_set.AddKey(Key(1));
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, key_set),
              IsOkAndHoldsRows({{1, "John", 22}}));
}

TEST_F(RangeReadsTest, CanReadUsingKeyBounds) {
  PopulateDatabase();

  // Can read using a closed closed range.
  EXPECT_THAT(
      Read("Users", {"ID", "Name", "Age"},
           ClosedClosed(Key(Null<std::int64_t>()), Key(1))),
      IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20}, {1, "John", 22}}));

  // Can read using a closed open range.
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"},
                   ClosedOpen(Key(Null<std::int64_t>()), Key(1))),
              IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20}}));

  // Can read using an open closed range.
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"},
                   OpenClosed(Key(Null<std::int64_t>()), Key(1))),
              IsOkAndHoldsRows({{1, "John", 22}}));

  // Can read using an open open range.
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"},
                   OpenOpen(Key(Null<std::int64_t>()), Key(2))),
              IsOkAndHoldsRows({{1, "John", 22}}));
}

TEST_F(RangeReadsTest, CanReadUsingEmptyKeyBounds) {
  PopulateDatabase();

  // Empty range bound corresponds to match all for closed bound and match none
  // for open bound.

  // Can read using a closed closed range with empty start key.
  EXPECT_THAT(
      Read("Users", {"ID", "Name", "Age"}, ClosedClosed(Key(), Key(1))),
      IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20}, {1, "John", 22}}));

  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, OpenClosed(Key(), Key(1))),
              IsOkAndHoldsRows({}));

  // Can read using a open closed range with empty end key.
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, OpenClosed(Key(1), Key())),
              IsOkAndHoldsRows({{2, "Peter", 41},
                                {4, "Matthew", 33},
                                {5, Null<std::string>(), 18}}));

  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, ClosedOpen(Key(1), Key())),
              IsOkAndHoldsRows({}));

  // Can read using a closed open range with empty start key.
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, ClosedOpen(Key(), Key(1))),
              IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20}}));

  // Can read using an closed closed range with both ends being empty.
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, ClosedClosed(Key(), Key())),
              IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20},
                                {1, "John", 22},
                                {2, "Peter", 41},
                                {4, "Matthew", 33},
                                {5, Null<std::string>(), 18}}));
}

TEST_F(RangeReadsTest, CanReadNumericAllKeyrange) {
  PopulateNumericTable();

  EXPECT_THAT(Read("NumericTable", {"key", "val"}, KeySet::All()),
              IsOkAndHoldsRows({
                  {Null<Numeric>(), "null"},
                  {minNumeric(), "min"},
                  {negativeNumeric(), "neg"},
                  {zeroNumeric(), "zero"},
                  {positiveNumeric(), "pos"},
                  {maxNumeric(), "max"},
              }));
}

TEST_F(RangeReadsTest, CanReadNumericPointKey) {
  PopulateNumericTable();

  KeySet key_set;
  key_set.AddKey(Key(positiveNumeric()));
  EXPECT_THAT(Read("NumericTable", {"key", "val"}, key_set),
              IsOkAndHoldsRows({{positiveNumeric(), "pos"}}));
}

TEST_F(RangeReadsTest, CanReadNumericUsingKeyBounds) {
  PopulateNumericTable();

  // Can read using a closed closed range.
  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   ClosedClosed(Key(Null<Numeric>()), Key(positiveNumeric()))),
              IsOkAndHoldsRows({{Null<Numeric>(), "null"},
                                {minNumeric(), "min"},
                                {negativeNumeric(), "neg"},
                                {zeroNumeric(), "zero"},
                                {positiveNumeric(), "pos"}}));

  // Can read using a closed open range.
  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   ClosedOpen(Key(Null<Numeric>()), Key(positiveNumeric()))),
              IsOkAndHoldsRows({{Null<Numeric>(), "null"},
                                {minNumeric(), "min"},
                                {negativeNumeric(), "neg"},
                                {zeroNumeric(), "zero"}}));

  // Can read using an open closed range.
  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   OpenClosed(Key(Null<Numeric>()), Key(positiveNumeric()))),
              IsOkAndHoldsRows({{minNumeric(), "min"},
                                {negativeNumeric(), "neg"},
                                {zeroNumeric(), "zero"},
                                {positiveNumeric(), "pos"}}));

  // Can read using an open open range.
  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   OpenOpen(Key(Null<Numeric>()), Key(positiveNumeric()))),
              IsOkAndHoldsRows({{minNumeric(), "min"},
                                {negativeNumeric(), "neg"},
                                {zeroNumeric(), "zero"}}));

  // Read using an closed closed range where the two endpoints are
  // not-null/non-empty.
  EXPECT_THAT(
      Read("NumericTable", {"key", "val"},
           ClosedClosed(Key(negativeNumeric()), Key(positiveNumeric()))),
      IsOkAndHoldsRows({{negativeNumeric(), "neg"},
                        {zeroNumeric(), "zero"},
                        {positiveNumeric(), "pos"}}));

  // Read using an closed open range where the two endpoints are
  // not-null/non-empty.
  EXPECT_THAT(
      Read("NumericTable", {"key", "val"},
           ClosedOpen(Key(negativeNumeric()), Key(positiveNumeric()))),
      IsOkAndHoldsRows({{negativeNumeric(), "neg"}, {zeroNumeric(), "zero"}}));

  // Read using an open closed range where the two endpoints are
  // not-null/non-empty.
  EXPECT_THAT(
      Read("NumericTable", {"key", "val"},
           OpenClosed(Key(negativeNumeric()), Key(positiveNumeric()))),
      IsOkAndHoldsRows({{zeroNumeric(), "zero"}, {positiveNumeric(), "pos"}}));

  // Read using an open open range where the two endpoints are
  // not-null/non-empty.
  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   OpenOpen(Key(negativeNumeric()), Key(positiveNumeric()))),
              IsOkAndHoldsRows({{zeroNumeric(), "zero"}}));
}

TEST_F(RangeReadsTest, CanReadNumericUsingEmptyKeyBounds) {
  PopulateNumericTable();

  // Can read using a closed closed range with empty start key.
  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   ClosedClosed(Key(), Key(zeroNumeric()))),
              IsOkAndHoldsRows({{Null<Numeric>(), "null"},
                                {minNumeric(), "min"},
                                {negativeNumeric(), "neg"},
                                {zeroNumeric(), "zero"}}));

  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   OpenClosed(Key(), Key(zeroNumeric()))),
              IsOkAndHoldsRows({}));

  // Can read using a open closed range with empty end key.
  EXPECT_THAT(
      Read("NumericTable", {"key", "val"},
           OpenClosed(Key(zeroNumeric()), Key())),
      IsOkAndHoldsRows({{positiveNumeric(), "pos"}, {maxNumeric(), "max"}}));

  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   ClosedOpen(Key(zeroNumeric()), Key())),
              IsOkAndHoldsRows({}));

  // Can read using a closed open range with empty start key.
  EXPECT_THAT(Read("NumericTable", {"key", "val"},
                   ClosedOpen(Key(), Key(zeroNumeric()))),
              IsOkAndHoldsRows({{Null<Numeric>(), "null"},
                                {minNumeric(), "min"},
                                {negativeNumeric(), "neg"}}));

  // Can read using an closed closed range with both ends being empty.
  EXPECT_THAT(Read("NumericTable", {"key", "val"}, ClosedClosed(Key(), Key())),
              IsOkAndHoldsRows({
                  {Null<Numeric>(), "null"},
                  {minNumeric(), "min"},
                  {negativeNumeric(), "neg"},
                  {zeroNumeric(), "zero"},
                  {positiveNumeric(), "pos"},
                  {maxNumeric(), "max"},
              }));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
