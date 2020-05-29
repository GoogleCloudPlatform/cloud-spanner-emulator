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

#include "backend/storage/in_memory_storage.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/datamodel/key_range.h"
#include "backend/storage/iterator.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Bool;
using zetasql::values::Int64;
using zetasql::values::String;

class InMemoryStorageTest : public testing::Test {
 protected:
  const TableID kTableId0 = "test_table:0";
  const TableID kTableId1 = "test_table:1";
  const ColumnID kColumnID = "test_column:0";
  const KeyRange kKeyRange0To5 =
      KeyRange::ClosedOpen(Key({Int64(0)}), Key({Int64(5)}));
  InMemoryStorage storage_;
  std::unique_ptr<StorageIterator> itr_;
};

TEST_F(InMemoryStorageTest, LookupByTable) {
  absl::Time t0 = absl::Now();

  // Write into 2 tables.
  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId1, Key({Int64(10)}), {kColumnID},
                           {String("value-10")}));

  // Read from the 2 tables.
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(
      storage_.Lookup(t0, kTableId0, Key({Int64(1)}), {kColumnID}, &values));
  EXPECT_THAT(values, testing::ElementsAre(String("value-1")));
  ZETASQL_EXPECT_OK(
      storage_.Lookup(t0, kTableId1, Key({Int64(10)}), {kColumnID}, &values));
  EXPECT_THAT(values, testing::ElementsAre(String("value-10")));
}

TEST_F(InMemoryStorageTest, ReadByTable) {
  absl::Time t0 = absl::Now();

  // Write into 2 tables.
  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId1, Key({Int64(4)}), {kColumnID},
                           {String("value-10")}));

  // Read from the 2 table.
  ZETASQL_EXPECT_OK(storage_.Read(t0, kTableId0, kKeyRange0To5, {kColumnID}, &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->NumColumns(), 1);
  EXPECT_EQ(itr_->ColumnValue(0), String("value-1"));
  EXPECT_EQ(itr_->Key(), Key({Int64(1)}));
  EXPECT_FALSE(itr_->Next());

  ZETASQL_EXPECT_OK(storage_.Read(t0, kTableId1, kKeyRange0To5, {kColumnID}, &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->NumColumns(), 1);
  EXPECT_EQ(itr_->ColumnValue(0), String("value-10"));
  EXPECT_EQ(itr_->Key(), Key({Int64(4)}));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, ReadRangeFromSingleTable) {
  absl::Time t0 = absl::Now();

  // Write into table.
  for (int i = 0; i < 5; ++i) {
    ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(i)}), {kColumnID},
                             {String(absl::StrCat("value-", i))}));
  }

  // Read from the table.
  ZETASQL_EXPECT_OK(storage_.Read(t0, kTableId0, kKeyRange0To5, {kColumnID}, &itr_));
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(itr_->Next());
    EXPECT_EQ(itr_->NumColumns(), 1);
    EXPECT_EQ(itr_->ColumnValue(0), String(absl::StrCat("value-", i)));
    EXPECT_EQ(itr_->Key(), Key({Int64(i)}));
  }
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, LookupByTimestamp) {
  absl::Time write_ts = absl::Now();

  ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));

  // Lookup key at exact timestamp it was written.
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_.Lookup(write_ts, kTableId0, Key({Int64(1)}), {kColumnID},
                            &values));
  EXPECT_THAT(values, testing::ElementsAre(String("value-1")));

  // Lookup key at a future timestamp.
  absl::Time lookup_in_future_ts = write_ts + absl::Nanoseconds(24);
  ZETASQL_EXPECT_OK(storage_.Lookup(lookup_in_future_ts, kTableId0, Key({Int64(1)}),
                            {kColumnID}, &values));
  EXPECT_THAT(values, testing::ElementsAre(String("value-1")));

  // Lookup key at timestamp before the first time it was written.
  absl::Time lookup_before_write_ts = write_ts - absl::Nanoseconds(1);
  EXPECT_THAT(storage_.Lookup(lookup_before_write_ts, kTableId0,
                              Key({Int64(1)}), {kColumnID}, &values),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  EXPECT_TRUE(values.empty());
}

TEST_F(InMemoryStorageTest, ReadByTimestamp) {
  absl::Time write_ts = absl::Now();

  // Write into table.
  ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));

  // Read key at the exact timestamp it was written.
  ZETASQL_EXPECT_OK(
      storage_.Read(write_ts, kTableId0, kKeyRange0To5, {kColumnID}, &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->ColumnValue(0), String("value-1"));
  EXPECT_EQ(itr_->Key(), Key({Int64(1)}));

  // Read key at a future timestamp.
  absl::Time read_in_future_ts = write_ts + absl::Nanoseconds(24);
  ZETASQL_EXPECT_OK(storage_.Read(read_in_future_ts, kTableId0, kKeyRange0To5,
                          {kColumnID}, &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->ColumnValue(0), String("value-1"));
  EXPECT_EQ(itr_->Key(), Key({Int64(1)}));

  // Read key at timestamp before the first time it was written.
  absl::Time read_before_write_ts = write_ts - absl::Nanoseconds(1);
  ZETASQL_EXPECT_OK(storage_.Read(read_before_write_ts, kTableId0, kKeyRange0To5,
                          {kColumnID}, &itr_));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, LookupInvalidTableReturnsNotFoundError) {
  absl::Time t0 = absl::Now();

  // Lookup a table_id in empty storage.
  std::vector<zetasql::Value> values;
  EXPECT_THAT(
      storage_.Lookup(t0, kTableId0, Key({Int64(1)}), {kColumnID}, &values),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));

  // Lookup invalid table_id in storage.
  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  EXPECT_THAT(storage_.Lookup(t0, "invalid-table_id_", Key({Int64(1)}),
                              {kColumnID}, &values),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InMemoryStorageTest, ReadInvalidTableReturnsEmptyResult) {
  absl::Time t0 = absl::Now();

  // Read a table_id in empty storage.
  ZETASQL_EXPECT_OK(storage_.Read(t0, kTableId0, kKeyRange0To5, {kColumnID}, &itr_));
  EXPECT_FALSE(itr_->Next());

  // Read invalid table_id in storage.
  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  ZETASQL_EXPECT_OK(storage_.Read(t0, "invalid-table_id_", kKeyRange0To5, {kColumnID},
                          &itr_));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, LookupMissingKeyReturnsNotFound) {
  absl::Time t0 = absl::Now();

  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  EXPECT_THAT(
      storage_.Lookup(t0, kTableId0, Key({Int64(100)}), {kColumnID}, &values),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InMemoryStorageTest, ReadMissingKeyReturnsEmptyItr) {
  absl::Time t0 = absl::Now();
  KeyRange key_range = KeyRange::ClosedOpen(Key({Int64(10)}), Key({Int64(50)}));

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  ZETASQL_EXPECT_OK(storage_.Read(t0, kTableId0, key_range, {kColumnID}, &itr_));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, ReadEmptyKeyRangeReturnsEmptyItr) {
  absl::Time t0 = absl::Now();
  KeyRange key_range = KeyRange::Empty();

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  ZETASQL_EXPECT_OK(storage_.Read(t0, kTableId0, key_range, {kColumnID}, &itr_));
  EXPECT_FALSE(itr_->Next());

  ZETASQL_EXPECT_OK(storage_.Read(
      t0, kTableId0,
      KeyRange(EndpointType::kClosed, Key(), EndpointType::kOpen, Key()), {},
      &itr_));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, LookupByMissingColumnReturnsInvalidValues) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));

  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_.Lookup(t0 + absl::Nanoseconds(5), kTableId0,
                            Key({Int64(1)}), {"invalid_kColumnID"}, &values));
  EXPECT_FALSE(values.empty());
  EXPECT_FALSE(values[0].is_valid());
}

TEST_F(InMemoryStorageTest, ReadByMissingColumnReturnsInvalidValues) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));

  ZETASQL_EXPECT_OK(storage_.Read(t0, kTableId0, kKeyRange0To5, {"invalid_kColumnID"},
                          &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_FALSE(itr_->ColumnValue(0).is_valid());
  EXPECT_EQ(itr_->Key(), Key({Int64(1)}));
}

TEST_F(InMemoryStorageTest, LookupWithoutColumns) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_.Lookup(t0, kTableId0, Key({Int64(1)}), {}, &values));
  EXPECT_TRUE(values.empty());
}

TEST_F(InMemoryStorageTest, ReadWithoutColumns) {
  absl::Time write_ts = absl::Now();
  absl::Time lookup_ts = write_ts + absl::Seconds(1);
  Key key({String("key"), Int64(1)});

  for (int i = 0; i < 5; i++) {
    Key key({String("key"), Int64(i)});
    ZETASQL_EXPECT_OK(
        storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  }

  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(
      storage_.Read(lookup_ts, kTableId0, KeyRange::Point(key), {}, &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->NumColumns(), 0);
  EXPECT_EQ(itr_->Key(), key);
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, LookupWithNullValuesReturnsInternalError) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                           {String("value-1")}));
  EXPECT_THAT(storage_.Lookup(t0, kTableId0, Key({Int64(1)}), {kColumnID},
                              /*values =*/nullptr),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(InMemoryStorageTest, WriteWithEmptyKeyAndColumns) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key(), {}, {}));
}

TEST_F(InMemoryStorageTest, WriteWithEmptyColumns) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, Key({Int64(1)}), {}, {}));
}

TEST_F(InMemoryStorageTest, DeleteFromNonExistentTable) {
  absl::Time t0 = absl::Now();
  Key key({Int64(1)});

  ZETASQL_EXPECT_OK(storage_.Delete(t0, kTableId0, KeyRange::Point(key)));
}

TEST_F(InMemoryStorageTest, DuplicateDeleteReturnsOk) {
  Key key({Int64(1)});
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);

  ZETASQL_EXPECT_OK(
      storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange::Point(key)));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange::Point(key)));
}

TEST_F(InMemoryStorageTest, DeleteEmptyRangeWillDeleteNothing) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = absl::Now();
  absl::Time lookup_ts = delete_ts + absl::Seconds(1);

  ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, Key({Int64(1)}), {kColumnID},
                           {Bool(true)}));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange()));
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_.Lookup(lookup_ts, kTableId0, Key({Int64(1)}), {kColumnID},
                            &values));
  EXPECT_THAT(values, testing::ElementsAre(Bool(true)));
}

TEST_F(InMemoryStorageTest, DeleteLargeRangeFromSparseTable) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time lookup_ts = delete_ts + absl::Seconds(1);

  // Write sparse keys.
  for (int j = 0; j < 5; j++) {
    ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, Key({Int64(5 * j)}),
                             {kColumnID}, {Bool(true)}));
  }

  // Delete key range [0, 50).
  KeyRange key_range(KeyRange::ClosedOpen(Key({Int64(0)}), Key({Int64(50)})));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, key_range));

  for (int j = 0; j < 5; j++) {
    std::vector<zetasql::Value> values;
    EXPECT_THAT(storage_.Lookup(lookup_ts, kTableId0, Key({Int64(5 * j)}),
                                {kColumnID}, &values),
                zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  }
}

TEST_F(InMemoryStorageTest, DeleteRangeNotInTableWillDeleteNothing) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time lookup_ts = delete_ts + absl::Seconds(1);

  // Write key range [0, 5).
  for (int i = 0; i < 5; i++) {
    ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, Key({Int64(i)}), {kColumnID},
                             {Bool(true)}));
  }

  // Delete key range [10, 100).
  KeyRange key_range(KeyRange::ClosedOpen(Key({Int64(10)}), Key({Int64(100)})));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, key_range));

  // Lookup for all existing keys [0, 5) should succeed.
  for (int i = 0; i < 5; i++) {
    std::vector<zetasql::Value> values;
    ZETASQL_EXPECT_OK(storage_.Lookup(lookup_ts, kTableId0, Key({Int64(i)}),
                              {kColumnID}, &values));
    EXPECT_THAT(values, testing::ElementsAre(Bool(true)));
    values.clear();
  }
}

TEST_F(InMemoryStorageTest, DeletePartialKeyRangeFromTable) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time lookup_ts = delete_ts + absl::Seconds(1);

  // Write keys in the range [10, 15].
  for (int i = 10; i <= 15; i++) {
    ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, Key({Int64(i)}), {kColumnID},
                             {Bool(true)}));
  }

  // Delete keys in the range [6, 12).
  KeyRange key_range(KeyRange::ClosedOpen(Key({Int64(6)}), Key({Int64(12)})));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, key_range));

  // Lookup after delete should return invalid values for keys range [10, 12).
  std::vector<zetasql::Value> values;
  for (int i = 10; i < 12; i++) {
    EXPECT_THAT(storage_.Lookup(lookup_ts, kTableId0, Key({Int64(i)}),
                                {kColumnID}, &values),
                zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  }
  // Lookup for range [12, 15] should return valid values.
  for (int i = 12; i <= 15; i++) {
    std::vector<zetasql::Value> values;
    ZETASQL_EXPECT_OK(storage_.Lookup(lookup_ts, kTableId0, Key({Int64(i)}),
                              {kColumnID}, &values));
    EXPECT_THAT(values, testing::ElementsAre(Bool(true)));
    values.clear();
  }
}

TEST_F(InMemoryStorageTest,
       DeleteUsingInvalidKeyRangeEndpointsReturnsInternalError) {
  absl::Time t0 = absl::Now();
  Key key({Int64(1)});

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, key, {}, {}));
  EXPECT_THAT(storage_.Delete(t0, kTableId0,
                              KeyRange::OpenClosed(Key({Int64(0)}), key)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(
      storage_.Delete(t0, kTableId0,
                      KeyRange::OpenOpen(Key({Int64(0)}), Key({Int64(2)}))),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(storage_.Delete(t0, kTableId0,
                              KeyRange::ClosedClosed(Key({Int64(0)}), key)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(InMemoryStorageTest, DeleteUsingEmptyKeyRangeDeletesNothing) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time lookup_ts = delete_ts + absl::Seconds(1);

  for (int i = 0; i < 5; i++) {
    Key key({Int64(i)});
    ZETASQL_EXPECT_OK(
        storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  }
  // Explicit Empty KeyRange.
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange::Empty()));

  // StartKey <= EndKey is considered an empty range.
  ZETASQL_EXPECT_OK(
      storage_.Delete(delete_ts, kTableId0,
                      KeyRange::ClosedOpen(Key({Int64(5)}), Key({Int64(0)}))));
  // Lookup keys in the range.
  for (int i = 0; i < 5; i++) {
    std::vector<zetasql::Value> values;
    ZETASQL_EXPECT_OK(storage_.Lookup(lookup_ts, kTableId0, Key({Int64(i)}),
                              {kColumnID}, &values));
    EXPECT_THAT(values, testing::ElementsAre(Bool(true)));
    values.clear();
  }
  // Read
  ZETASQL_EXPECT_OK(
      storage_.Read(lookup_ts, kTableId0, KeyRange::All(), {kColumnID}, &itr_));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(itr_->Next());
    EXPECT_EQ(itr_->NumColumns(), 1);
    EXPECT_EQ(itr_->Key(), Key({Int64(i)}));
  }
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, DeleteUsingAllKeyRangeDeletesEverything) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time lookup_ts = delete_ts + absl::Seconds(1);

  for (int i = 0; i < 5; i++) {
    Key key({Int64(i)});
    ZETASQL_EXPECT_OK(
        storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  }
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange::All()));
  // Lookup
  for (int i = 0; i < 5; i++) {
    std::vector<zetasql::Value> values;
    EXPECT_THAT(storage_.Lookup(lookup_ts, kTableId0, Key({Int64(i)}),
                                {kColumnID}, &values),
                zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  }
  // Read
  ZETASQL_EXPECT_OK(
      storage_.Read(lookup_ts, kTableId0, KeyRange::All(), {kColumnID}, &itr_));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, DeleteUsingPrefixKeyRange) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time lookup_ts = delete_ts + absl::Seconds(1);

  for (int i = 0; i < 5; i++) {
    Key key({String("key"), Int64(i)});
    ZETASQL_EXPECT_OK(
        storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  }
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0,
                            KeyRange::Prefix(Key({String("key")}))));
  // Lookup
  for (int i = 0; i < 5; i++) {
    std::vector<zetasql::Value> values;
    EXPECT_THAT(
        storage_.Lookup(lookup_ts, kTableId0, Key({String("key"), Int64(i)}),
                        {kColumnID}, &values),
        zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  }
  // Read
  ZETASQL_EXPECT_OK(storage_.Read(lookup_ts, kTableId0,
                          KeyRange::ClosedOpen(Key({String("key"), Int64(0)}),
                                               Key({String("key"), Int64(5)})),
                          {kColumnID}, &itr_));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, LookupShouldReturnMostRecentColumnValues) {
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time write_after_delete_ts = delete_ts + absl::Seconds(1);
  absl::Time lookup_after_second_write_ts =
      write_after_delete_ts + absl::Seconds(1);
  Key key({Int64(1)});

  // Write key with column value = true.
  ZETASQL_EXPECT_OK(
      storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  // Delete key.
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange::Point(key)));
  // Write key without column value.
  ZETASQL_EXPECT_OK(storage_.Write(write_after_delete_ts, kTableId0, key, {}, {}));
  // Lookup should return empty column value.
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_.Lookup(lookup_after_second_write_ts, kTableId0, key,
                            {kColumnID}, &values));
  EXPECT_THAT(values, testing::ElementsAre(zetasql::Value()));
  // Read should return empty column values.
  ZETASQL_EXPECT_OK(storage_.Read(lookup_after_second_write_ts, kTableId0,
                          kKeyRange0To5, {kColumnID}, &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->NumColumns(), 1);
  EXPECT_FALSE(itr_->ColumnValue(0).is_valid());
  EXPECT_EQ(itr_->Key(), Key({Int64(1)}));
}

TEST_F(InMemoryStorageTest, LookupAtOrAfterDeleteTimestampReturnsInvalidValue) {
  Key key({Int64(1)});
  absl::Time write_ts = absl::Now();
  absl::Time delete_ts = write_ts + absl::Seconds(1);
  absl::Time after_delete_ts = delete_ts + absl::Seconds(1);

  ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, key, {kColumnID},
                           {String("value-10")}));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange::Point(key)));

  // Lookup at delete timestamp.
  std::vector<zetasql::Value> values;
  EXPECT_THAT(storage_.Lookup(delete_ts, kTableId0, key, {kColumnID}, &values),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));

  // Lookup after delete timestamp.
  values.clear();
  EXPECT_THAT(
      storage_.Lookup(after_delete_ts, kTableId0, key, {kColumnID}, &values),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InMemoryStorageTest, LookupBeforeDeleteTimestampReturnsValidValue) {
  absl::Time write_ts = absl::Now();
  absl::Time before_delete_ts = write_ts + absl::Seconds(1);
  absl::Time delete_ts = before_delete_ts + absl::Seconds(1);
  Key key({Int64(1)});

  ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, key, {kColumnID},
                           {String("value-10")}));
  ZETASQL_EXPECT_OK(storage_.Delete(delete_ts, kTableId0, KeyRange::Point(key)));
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(
      storage_.Lookup(before_delete_ts, kTableId0, key, {kColumnID}, &values));
  EXPECT_THAT(values, testing::ElementsAre(String("value-10")));
}

TEST_F(InMemoryStorageTest, SnapshotRead) {
  absl::Time write_ts = absl::Now();
  absl::Time snapshot_read_ts = write_ts + absl::Seconds(1);
  absl::Time second_write_ts = snapshot_read_ts + absl::Seconds(1);
  Key key({Int64(1)});

  ZETASQL_EXPECT_OK(storage_.Write(write_ts, kTableId0, key, {kColumnID},
                           {String("value-10")}));
  ZETASQL_EXPECT_OK(storage_.Write(second_write_ts, kTableId0, key, {kColumnID},
                           {String("value-20")}));

  // Snapshot Read.
  ZETASQL_EXPECT_OK(storage_.Read(snapshot_read_ts, kTableId0, kKeyRange0To5,
                          {kColumnID}, &itr_));
  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->ColumnValue(0), String("value-10"));
}

TEST_F(InMemoryStorageTest, ReadUsingKeyRangeAll) {
  absl::Time write_ts = absl::Now();
  absl::Time read_ts = write_ts + absl::Seconds(1);

  for (int i = 0; i < 5; i++) {
    Key key({Int64(i)});
    ZETASQL_EXPECT_OK(
        storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  }

  // Read using KeyRange::All()
  ZETASQL_EXPECT_OK(
      storage_.Read(read_ts, kTableId0, KeyRange::All(), {kColumnID}, &itr_));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(itr_->Next());
    EXPECT_EQ(itr_->NumColumns(), 1);
    EXPECT_EQ(itr_->Key(), Key({Int64(i)}));
  }
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, ReadUsingPointKeyRange) {
  absl::Time write_ts = absl::Now();
  absl::Time read_ts = write_ts + absl::Seconds(1);

  for (int i = 0; i < 5; i++) {
    Key key({Int64(i)});
    ZETASQL_EXPECT_OK(
        storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  }

  ZETASQL_EXPECT_OK(storage_.Read(read_ts, kTableId0, KeyRange::Point(Key({Int64(0)})),
                          {kColumnID}, &itr_));

  EXPECT_TRUE(itr_->Next());
  EXPECT_EQ(itr_->NumColumns(), 1);
  EXPECT_EQ(itr_->Key(), Key({Int64(0)}));
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest, ReadUsingPrefixKeyRange) {
  absl::Time write_ts = absl::Now();
  absl::Time read_ts = write_ts + absl::Seconds(1);

  for (int i = 0; i < 5; i++) {
    Key key({String("key"), Int64(i)});
    ZETASQL_EXPECT_OK(
        storage_.Write(write_ts, kTableId0, key, {kColumnID}, {Bool(true)}));
  }

  // Read
  ZETASQL_EXPECT_OK(storage_.Read(read_ts, kTableId0,
                          KeyRange::Prefix(Key({String("key")})), {kColumnID},
                          &itr_));
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(itr_->Next());
    EXPECT_EQ(itr_->NumColumns(), 1);
    EXPECT_EQ(itr_->Key(), Key({String("key"), Int64(i)}));
  }
  EXPECT_FALSE(itr_->Next());
}

TEST_F(InMemoryStorageTest,
       ReadUsingInvalidKeyRangeEndpointsReturnsInternalError) {
  absl::Time t0 = absl::Now();
  Key key({Int64(1)});

  ZETASQL_EXPECT_OK(storage_.Write(t0, kTableId0, key, {}, {}));

  EXPECT_THAT(
      storage_.Read(t0, kTableId0, KeyRange::OpenClosed(Key({Int64(0)}), key),
                    {}, &itr_),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(
      storage_.Read(t0, kTableId0,
                    KeyRange::OpenOpen(Key({Int64(0)}), Key({Int64(2)})), {},
                    &itr_),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(
      storage_.Read(t0, kTableId0, KeyRange::ClosedClosed(Key({Int64(0)}), key),
                    {}, &itr_),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
