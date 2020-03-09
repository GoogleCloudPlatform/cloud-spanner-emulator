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

#include "backend/storage/in_memory_iterator.h"

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/datamodel/key.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::Value;
using zetasql::values::Int64;

TEST(FixedRowStorageIterator, CreateIterator) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(2)}), {Int64(20)}}};

  FixedRowStorageIterator itr(std::move(row_values));
  while (itr.Next()) {
    EXPECT_EQ(Key({Int64(2)}), itr.Key());
    EXPECT_EQ(1, itr.NumColumns());
    for (int i = 0; i < itr.NumColumns(); ++i) {
      EXPECT_EQ(Int64(20), itr.ColumnValue(i));
    }
  }
  EXPECT_FALSE(itr.Next());
}

TEST(FixedRowStorageIterator, CreateIteratorWithNoRows) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {};

  FixedRowStorageIterator itr(std::move(row_values));
  EXPECT_FALSE(itr.Next());
}

TEST(FixedRowStorageIterator, CreateIteratorWithCompositeKeys) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(1), Int64(2)}), {Int64(20)}}};

  FixedRowStorageIterator itr(std::move(row_values));
  while (itr.Next()) {
    EXPECT_EQ(Key({Int64(1), Int64(2)}), itr.Key());
    EXPECT_EQ(1, itr.NumColumns());
    EXPECT_EQ(Int64(20), itr.ColumnValue(0));
  }
}

TEST(FixedRowStorageIterator, CreateIteratorWithManyRows) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(0)}), {Int64(20)}},
      {Key({Int64(1)}), {Int64(20)}},
      {Key({Int64(2)}), {Int64(20)}}};

  FixedRowStorageIterator itr(std::move(row_values));
  int i = 0;
  while (itr.Next()) {
    EXPECT_EQ(Key({Int64(i)}), itr.Key());
    EXPECT_EQ(1, itr.NumColumns());
    EXPECT_EQ(Int64(20), itr.ColumnValue(0));
    ++i;
  }
}

TEST(FixedRowStorageIterator, CreateIteratorWithEmptyColumns) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(2)}), {}}};

  FixedRowStorageIterator itr(std::move(row_values));
  while (itr.Next()) {
    EXPECT_EQ(Key({Int64(2)}), itr.Key());
    EXPECT_EQ(0, itr.NumColumns());
  }
}

TEST(FixedRowStorageIterator, CreateIteratorWithInvalidColumnValues) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(2)}), {Value()}}};

  FixedRowStorageIterator itr(std::move(row_values));
  while (itr.Next()) {
    EXPECT_EQ(Key({Int64(2)}), itr.Key());
    EXPECT_EQ(1, itr.NumColumns());
    EXPECT_FALSE(itr.ColumnValue(0).is_valid());
  }
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
