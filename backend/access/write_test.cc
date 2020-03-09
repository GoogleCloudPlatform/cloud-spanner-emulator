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

#include "backend/access/write.h"

#include <sstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/datamodel/key_range.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql::values::Int64;

// TODO: Add more tests after schema/tables have been fully
// defined.

TEST(Mutation, EmptyMutation) {
  Mutation list;
  ASSERT_EQ(0, list.ops().size());
}

TEST(Mutation, BasicOpsWork) {
  Mutation list;
  list.AddWriteOp(MutationOpType::kInsert, "Hello", {"One"}, {{Int64(1)}});

  ASSERT_EQ(1, list.ops().size());
  EXPECT_EQ("Hello", list.ops()[0].table);
}

TEST(Mutation, CanBeStreamedToOStream) {
  Mutation list;
  list.AddWriteOp(MutationOpType::kInsert, "Hello", {"One"}, {{Int64(1)}});

  std::stringstream sstr;
  sstr << list;

  EXPECT_EQ(
      "#0 Op:\n"
      "Type   : kInsert\n"
      "Table  : 'Hello'\n"
      "Columns: ['One']\n"
      "Rows   :\n"
      "- {Int64(1)}\n",
      sstr.str());
}

TEST(Mutation, DeleteOp) {
  Mutation list;
  KeySet key_set1(Key({Int64(1)}));
  KeySet key_set2;
  KeyRange range1 = KeyRange::OpenClosed(Key({Int64(1)}), Key({Int64(9)}));
  KeyRange range2 = KeyRange::OpenClosed(Key({Int64(14)}), Key({Int64(20)}));
  KeySet key_set3;
  key_set3.AddRange(range1);
  key_set3.AddRange(range2);
  list.AddDeleteOp("Table1", key_set1);
  list.AddDeleteOp("Table1", key_set2);
  list.AddDeleteOp("Table1", key_set3);

  ASSERT_EQ(3, list.ops().size());
  EXPECT_EQ("Table1", list.ops()[0].table);
  EXPECT_EQ("Table1", list.ops()[1].table);
  EXPECT_EQ("Table1", list.ops()[2].table);
  EXPECT_EQ(MutationOpType::kDelete, list.ops()[0].type);
  EXPECT_EQ(MutationOpType::kDelete, list.ops()[1].type);
  EXPECT_EQ(MutationOpType::kDelete, list.ops()[2].type);
  EXPECT_EQ("Key{Int64(1)}", list.ops()[0].key_set.DebugString());
  EXPECT_EQ("<none>", list.ops()[1].key_set.DebugString());
  EXPECT_EQ(
      "Range({Int64(1)} ... {Int64(9)}], Range({Int64(14)} ... {Int64(20)}]",
      list.ops()[2].key_set.DebugString());
}

TEST(Mutation, MixedOps) {
  Mutation list;
  list.AddWriteOp(MutationOpType::kUpdate, "Table1", {"col1", "col2", "col3"},
                  {{Int64(1)}, {Int64(2)}, {Int64(3)}});
  list.AddDeleteOp("Table1", KeySet(Key({Int64(1)})));
  list.AddWriteOp(MutationOpType::kInsertOrUpdate, "Table1", {"col1"},
                  {{Int64(1)}});
  list.AddWriteOp(MutationOpType::kReplace, "Table2", {"col2"}, {{Int64(42)}});
  list.AddDeleteOp("Table2", KeySet(Key({Int64(1)})));

  ASSERT_EQ(5, list.ops().size());
  EXPECT_EQ("Table1", list.ops()[0].table);
  EXPECT_EQ("Table1", list.ops()[1].table);
  EXPECT_EQ("Table1", list.ops()[2].table);
  EXPECT_EQ("Table2", list.ops()[3].table);
  EXPECT_EQ("Table2", list.ops()[4].table);
  EXPECT_EQ(MutationOpType::kUpdate, list.ops()[0].type);
  EXPECT_EQ(MutationOpType::kDelete, list.ops()[1].type);
  EXPECT_EQ(MutationOpType::kInsertOrUpdate, list.ops()[2].type);
  EXPECT_EQ(MutationOpType::kReplace, list.ops()[3].type);
  EXPECT_EQ(MutationOpType::kDelete, list.ops()[4].type);
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
