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
#include <string>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/numeric.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

class RangeReadsTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchemaFromFile("range_reads.test");
  }

 protected:
  void PopulateDatabase() {
    // Write fixure data to use in reads.
    if (dialect_ == database_api::POSTGRESQL) {
      // PostgreSQL does not support NULL primary keys.
      GOOGLESQL_EXPECT_OK(MultiInsert("Users", {"ID", "Name", "Age"},
                            {{1, "John", 22},
                             {2, "Peter", 41},
                             {4, "Matthew", 33},
                             {5, Null<std::string>(), 18}}));
    } else {
      GOOGLESQL_EXPECT_OK(MultiInsert("Users", {"ID", "Name", "Age"},
                            {{Null<std::int64_t>(), "Adam", 20},
                             {1, "John", 22},
                             {2, "Peter", 41},
                             {4, "Matthew", 33},
                             {5, Null<std::string>(), 18}}));
    }
  }

  void PopulateNumericTable() {
    GOOGLESQL_EXPECT_OK(MultiInsert("NumericTable", {"key", "val"},
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

INSTANTIATE_TEST_SUITE_P(
    PerDialectRangeReadsTest, RangeReadsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<RangeReadsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(RangeReadsTest, CanReadAllKeyrange) {
  PopulateDatabase();

  if (dialect_ == database_api::POSTGRESQL) {
    EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, KeySet::All()),
                IsOkAndHoldsRows({{1, "John", 22},
                                  {2, "Peter", 41},
                                  {4, "Matthew", 33},
                                  {5, Null<std::string>(), 18}}));
  } else {
    EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, KeySet::All()),
                IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20},
                                  {1, "John", 22},
                                  {2, "Peter", 41},
                                  {4, "Matthew", 33},
                                  {5, Null<std::string>(), 18}}));
  }
}

TEST_P(RangeReadsTest, CanReadPointKey) {
  PopulateDatabase();

  KeySet key_set;
  key_set.AddKey(Key(1));
  EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, key_set),
              IsOkAndHoldsRows({{1, "John", 22}}));
}

TEST_P(RangeReadsTest, CanReadUsingKeyBounds) {
  PopulateDatabase();

  if (dialect_ == database_api::POSTGRESQL) {
    // Can read using a closed closed range.
    EXPECT_THAT(
        Read("Users", {"ID", "Name", "Age"}, ClosedClosed(Key(1), Key(2))),
        IsOkAndHoldsRows({{1, "John", 22}, {2, "Peter", 41}}));

    // Can read using a closed open range.
    EXPECT_THAT(
        Read("Users", {"ID", "Name", "Age"}, ClosedOpen(Key(1), Key(2))),
        IsOkAndHoldsRows({{1, "John", 22}}));

    // Can read using an open closed range.
    EXPECT_THAT(
        Read("Users", {"ID", "Name", "Age"}, OpenClosed(Key(1), Key(2))),
        IsOkAndHoldsRows({{2, "Peter", 41}}));

    // Can read using an open open range.
    EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, OpenOpen(Key(1), Key(3))),
                IsOkAndHoldsRows({{2, "Peter", 41}}));
  } else {
    // Can read using a closed closed range.
    EXPECT_THAT(Read("Users", {"ID", "Name", "Age"},
                     ClosedClosed(Key(Null<std::int64_t>()), Key(1))),
                IsOkAndHoldsRows(
                    {{Null<std::int64_t>(), "Adam", 20}, {1, "John", 22}}));

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
}

TEST_P(RangeReadsTest, CanReadUsingEmptyKeyBounds) {
  PopulateDatabase();

  // Empty range bound corresponds to match all for closed bound and match none
  // for open bound.

  // Can read using a closed closed range with empty start key.
  if (dialect_ == database_api::POSTGRESQL) {
    EXPECT_THAT(
        Read("Users", {"ID", "Name", "Age"}, ClosedClosed(Key(), Key(2))),
        IsOkAndHoldsRows({{1, "John", 22}, {2, "Peter", 41}}));

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
    EXPECT_THAT(Read("Users", {"ID", "Name", "Age"}, ClosedOpen(Key(), Key(2))),
                IsOkAndHoldsRows({{1, "John", 22}}));

    // Can read using an closed closed range with both ends being empty.
    EXPECT_THAT(
        Read("Users", {"ID", "Name", "Age"}, ClosedClosed(Key(), Key())),
        IsOkAndHoldsRows({{1, "John", 22},
                          {2, "Peter", 41},
                          {4, "Matthew", 33},
                          {5, Null<std::string>(), 18}}));
  } else {
    EXPECT_THAT(
        Read("Users", {"ID", "Name", "Age"}, ClosedClosed(Key(), Key(1))),
        IsOkAndHoldsRows(
            {{Null<std::int64_t>(), "Adam", 20}, {1, "John", 22}}));

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
    EXPECT_THAT(
        Read("Users", {"ID", "Name", "Age"}, ClosedClosed(Key(), Key())),
        IsOkAndHoldsRows({{Null<std::int64_t>(), "Adam", 20},
                          {1, "John", 22},
                          {2, "Peter", 41},
                          {4, "Matthew", 33},
                          {5, Null<std::string>(), 18}}));
  }
}

TEST_P(RangeReadsTest, CanReadNumericAllKeyrange) {
  if (dialect_ == database_api::POSTGRESQL) {
    GTEST_SKIP() << "PostgreSQL dialect does not support numeric primary keys";
  }

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

TEST_P(RangeReadsTest, CanReadNumericPointKey) {
  if (dialect_ == database_api::POSTGRESQL) {
    GTEST_SKIP() << "PostgreSQL dialect does not support numeric primary keys";
  }

  PopulateNumericTable();

  KeySet key_set;
  key_set.AddKey(Key(positiveNumeric()));
  EXPECT_THAT(Read("NumericTable", {"key", "val"}, key_set),
              IsOkAndHoldsRows({{positiveNumeric(), "pos"}}));
}

TEST_P(RangeReadsTest, CanReadNumericUsingKeyBounds) {
  if (dialect_ == database_api::POSTGRESQL) {
    GTEST_SKIP() << "PostgreSQL dialect does not support numeric primary keys";
  }

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

TEST_P(RangeReadsTest, CanReadNumericUsingEmptyKeyBounds) {
  if (dialect_ == database_api::POSTGRESQL) {
    GTEST_SKIP() << "PostgreSQL dialect does not support numeric primary keys";
  }

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
