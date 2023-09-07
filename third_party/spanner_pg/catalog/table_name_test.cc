//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/table_name.h"

#include <iostream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_join.h"

namespace postgres_translator {
namespace {

TEST(TableNameTest, IsEmpty) {
  TableName table_name({""});

  EXPECT_TRUE(table_name.IsEmpty());
}

TEST(TableNameTest, ToString) {
  TableName table_name1({"database", "namespace", "table"});
  TableName table_name2({"table"});

  EXPECT_EQ(table_name1.ToString(), "database.namespace.table");
  EXPECT_EQ(table_name1.UnqualifiedName(), "table");
  EXPECT_EQ(table_name2.ToString(), "table");
  EXPECT_EQ(table_name2.ToString(), table_name2.UnqualifiedName());
}

TEST(TableNameTest, Streaming) {
  std::ostringstream os;
  TableName table_name1({"table_name"});
  TableName table_name2({"namespace", "table_name2"});

  os << table_name1 << " " << table_name2;

  EXPECT_EQ(
      os.str(),
      absl::StrJoin({table_name1.ToString(), table_name2.ToString()}, " "));
}

TEST(TableNameTest, Equality) {
  TableName table_name1({"namespace", "table_name"});
  TableName table_name2({"namespace", "table_name"});
  EXPECT_EQ(table_name1, table_name2);
}

TEST(TableNameTest, Hashing) {
  TableName table_name1({"namespace", "table_name"});
  TableName table_name2({"namespace", "table_name"});
  absl::flat_hash_set<TableName> table_set;
  table_set.insert(table_name1);

  EXPECT_TRUE(table_set.contains(table_name1));
  EXPECT_TRUE(table_set.contains(table_name2));
}

TEST(TableNameTest, Span) {
  TableName table_name1({"namespace", "table_name"});
  absl::Span<const std::string> span = table_name1.AsSpan();

  EXPECT_EQ(table_name1.ToString(), absl::StrJoin(span, "."));
}

TEST(TableNameTest, NamespaceName) {
  TableName table_name1({"database", "namespace", "table"});
  TableName table_name2({"namespace", "table"});
  TableName table_name3({"table"});

  ASSERT_NE(table_name1.NamespaceName(), nullptr);
  EXPECT_EQ(*table_name1.NamespaceName(), "namespace");

  ASSERT_NE(table_name2.NamespaceName(), nullptr);
  EXPECT_EQ(*table_name2.NamespaceName(), "namespace");

  ASSERT_EQ(table_name3.NamespaceName(), nullptr);
}

}  // namespace
}  // namespace postgres_translator
