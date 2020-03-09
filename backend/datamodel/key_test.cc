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

#include "backend/datamodel/key.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/datamodel/value.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql::types::Int64Type;
using zetasql::values::Int64;
using zetasql::values::Null;
using zetasql::values::String;

TEST(Key, ReturnsNoColumnsForEmptyKeys) {
  Key empty_key;
  EXPECT_EQ(0, empty_key.NumColumns());
}

TEST(Key, SupportsBasicOperationsOnSingleColumnKeys) {
  Key key({String("One")});

  EXPECT_EQ(1, key.NumColumns());
  EXPECT_EQ(String("One"), key.ColumnValue(0));
  EXPECT_FALSE(key.IsColumnDescending(0));
}

TEST(Key, SupportsBasicOperationsOnMultiColumnKeys) {
  Key key;
  key.AddColumn({String("One")});
  key.AddColumn({Int64(2)});
  key.SetColumnDescending(1, true);

  EXPECT_EQ(2, key.NumColumns());
  EXPECT_EQ(String("One"), key.ColumnValue(0));
  EXPECT_EQ(Int64(2), key.ColumnValue(1));
  EXPECT_FALSE(key.IsColumnDescending(0));
  EXPECT_TRUE(key.IsColumnDescending(1));
}

TEST(Key, DoesNotNeedBeginEndParts) {
  Key start({Int64(6)});
  Key end = Key({Int64(6)}).ToPrefixLimit();
  Key b_eq_25({Int64(6), Int64(25)});
  Key b_lt_25({Int64(6), Int64(10)});
  Key b_gt_25({Int64(6), Int64(30)});

  // Check order with ascending columns.
  std::vector<Key> with_column_b_ascending{start, end, b_eq_25, b_lt_25,
                                           b_gt_25};
  std::sort(with_column_b_ascending.begin(), with_column_b_ascending.end());

  EXPECT_THAT(with_column_b_ascending,
              testing::ElementsAre(start, b_lt_25, b_eq_25, b_gt_25, end));

  // Check order with descending columns.
  b_eq_25.SetColumnDescending(1, true);
  b_lt_25.SetColumnDescending(1, true);
  b_gt_25.SetColumnDescending(1, true);

  std::vector<Key> with_column_b_descending{start, end, b_eq_25, b_lt_25,
                                            b_gt_25};
  std::sort(with_column_b_descending.begin(), with_column_b_descending.end());

  EXPECT_THAT(with_column_b_descending,
              testing::ElementsAre(start, b_gt_25, b_eq_25, b_lt_25, end));
}

TEST(Key, SortsNullsCorrectly) {
  Key null({String("prefix"), Null(Int64Type())});
  Key one({String("prefix"), Int64(1)});

  EXPECT_GT(one, null);
  EXPECT_LT(null, one);

  null.SetColumnDescending(1, true);
  one.SetColumnDescending(1, true);

  EXPECT_GT(null, one);
  EXPECT_LT(one, null);
}

TEST(Key, OrdersKeysOfSameLength) {
  EXPECT_EQ(Key({Int64(1)}), Key({Int64(1)}));
  EXPECT_LT(Key({Int64(1)}), Key({Int64(2)}));
  EXPECT_GT(Key({Int64(2)}), Key({Int64(1)}));
  EXPECT_EQ(Key({String("B"), Int64(1)}), Key({String("B"), Int64(1)}));
  EXPECT_LT(Key({String("B"), Int64(1)}), Key({String("B"), Int64(2)}));
}

TEST(Key, OrdersKeysOfDifferentLength) {
  EXPECT_LT(Key({Int64(1)}), Key({Int64(1), String("One")}));
  EXPECT_GT(Key({String("B"), Int64(1)}), Key({String("A")}));
}

TEST(Key, OrdersSpecialKeys) {
  EXPECT_EQ(Key::Empty(), Key());
  EXPECT_EQ(Key::Empty(), Key::Empty());
  EXPECT_EQ(Key::Infinity(), Key::Infinity());
  EXPECT_LT(Key::Empty(), Key::Infinity());
  EXPECT_LT(Key::Empty(), Key({Int64(1)}));
  EXPECT_LT(Key({Int64(1)}), Key::Infinity());
}

TEST(Key, OrdersSingleColumnKeysWithDescendingColumns) {
  Key k1;
  k1.AddColumn(Int64(1), true);
  Key k2;
  k1.AddColumn(Int64(2), true);

  EXPECT_LT(k2, k1);
  EXPECT_GT(k1, k2);
}

TEST(Key, OrdersMultiColumnKeysWithDescendingColumns) {
  Key k1a2d;
  k1a2d.AddColumn(Int64(1), false);
  k1a2d.AddColumn(Int64(2), true);

  Key k1a1d;
  k1a1d.AddColumn(Int64(1), false);
  k1a1d.AddColumn(Int64(1), true);

  Key k2a2d;
  k2a2d.AddColumn(Int64(2), false);
  k2a2d.AddColumn(Int64(2), true);

  Key k2a1d;
  k2a1d.AddColumn(Int64(2), false);
  k2a1d.AddColumn(Int64(1), true);

  Key k2a3d3a;
  k2a3d3a.AddColumn(Int64(2), false);
  k2a3d3a.AddColumn(Int64(3), true);
  k2a3d3a.AddColumn(Int64(3), false);

  EXPECT_LT(k1a2d, k1a1d);
  EXPECT_LT(k1a2d, k2a2d);
  EXPECT_LT(k1a1d, k2a1d);
  EXPECT_LT(k2a3d3a, k2a1d);
}

TEST(Key, OrdersSingleColumnPrefixLimitKeys) {
  Key key({String("A")});

  EXPECT_LT(key, key.ToPrefixLimit());
  EXPECT_GT(key.ToPrefixLimit(), key);
  EXPECT_LT(key.ToPrefixLimit(), Key({String("B")}));
  EXPECT_LT(Key({String("A"), Int64(1)}), key.ToPrefixLimit());
}

TEST(Key, OrdersMultiColumnPrefixLimitKeys) {
  Key key({String("A"), Int64(1)});

  EXPECT_LT(key, key.ToPrefixLimit());
  EXPECT_GT(key.ToPrefixLimit(), key);
  EXPECT_GT(key.ToPrefixLimit(), Key({String("A"), Int64(1)}));
  EXPECT_LT(key.ToPrefixLimit(), Key({String("A"), Int64(2)}));
  EXPECT_LT(Key({String("A"), Int64(1), String("B")}), key.ToPrefixLimit());
}

TEST(Key, GeneratesPrefixKeys) {
  Key k1d2a;
  k1d2a.AddColumn(Int64(1), true);
  k1d2a.AddColumn(Int64(2), false);

  Key k1d;
  k1d.AddColumn(Int64(1), true);

  EXPECT_EQ(k1d2a.Prefix(1), k1d);
}

TEST(Key, ChecksPrefixKeys) {
  Key s = Key::Empty();
  Key a = Key({String("a")});
  Key a1 = Key({String("a"), Int64(1)});
  Key b = Key({String("b")});
  Key b1 = Key({String("b"), Int64(1)});
  Key e = Key::Infinity();

  // s is a prefix of all keys.
  EXPECT_TRUE(s.IsPrefixOf(s));
  EXPECT_TRUE(s.IsPrefixOf(a));
  EXPECT_TRUE(s.IsPrefixOf(a1));
  EXPECT_TRUE(s.IsPrefixOf(e));

  // a is a prefix of a, a1.
  EXPECT_TRUE(a.IsPrefixOf(a));
  EXPECT_TRUE(a.IsPrefixOf(a1));
  EXPECT_FALSE(a.IsPrefixOf(b));
  EXPECT_FALSE(a.IsPrefixOf(b1));
  EXPECT_FALSE(a.IsPrefixOf(e));

  // e is only a prefix of itself.
  EXPECT_FALSE(e.IsPrefixOf(s));
  EXPECT_FALSE(e.IsPrefixOf(a));
  EXPECT_FALSE(e.IsPrefixOf(a1));
  EXPECT_TRUE(e.IsPrefixOf(e));

  // a+ is not a prefix of a.
  EXPECT_FALSE(a.ToPrefixLimit().IsPrefixOf(a));
  EXPECT_FALSE(a.IsPrefixOf(a.ToPrefixLimit()));
}

TEST(Key, GeneratesDebugString) {
  EXPECT_EQ("{Int64(1)}", Key({Int64(1)}).DebugString());
  EXPECT_EQ("{Int64(1), String(\"A\")}",
            Key({Int64(1), String("A")}).DebugString());
  EXPECT_EQ("{Int64(1)}+", Key({Int64(1)}).ToPrefixLimit().DebugString());
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
