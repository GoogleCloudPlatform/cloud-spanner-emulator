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

#include "third_party/spanner_pg/catalog/spangres_function_grouper.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/btree_map.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "google/protobuf/text_format.h"

namespace postgres_translator {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::SizeIs;

TEST(SpangresFunctionGrouperTest, GroupsFunctionsByNamePaths) {
  FunctionProto fn1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "pg" name_path: "fn" }
        signatures: {
          postgresql_name_paths: { name_path: "pg" name_path: "fn" }
          postgresql_name_paths: { name_path: "pg" name_path: "other_fn" }
          return_type: { oid: 20 }
        }
        signatures: {
          oid: 50001
          postgresql_name_paths: { name_path: "spanner" name_path: "fn" }
          return_type: { oid: 25 }
        }
      )pb",
      &fn1));

  FunctionProto fn2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "pg" name_path: "fn" }
        signatures: {
          oid: 50002
          postgresql_name_paths: { name_path: "spanner" name_path: "fn" }
          return_type: { oid: 16 }
        }
      )pb",
      &fn2));

  absl::btree_map<NamePathKey, FunctionProto> result =
      GroupFunctionsByNamePaths({fn1, fn2});
  ASSERT_THAT(result, SizeIs(3));

  {
    FunctionProto spanner_fn = result[{{"pg", "fn"}, {"spanner", "fn"}}];
    EXPECT_THAT(spanner_fn.postgresql_name_paths(), SizeIs(1));
    EXPECT_THAT(spanner_fn.postgresql_name_paths(0).name_path(),
                ElementsAre("spanner", "fn"));
    EXPECT_THAT(spanner_fn.signatures(), SizeIs(2));
    EXPECT_THAT(spanner_fn.signatures(0).oid(), Eq(50001));
    EXPECT_THAT(spanner_fn.signatures(0).postgresql_name_paths(), SizeIs(1));
    EXPECT_THAT(spanner_fn.signatures(0).postgresql_name_paths(0).name_path(),
                ElementsAre("spanner", "fn"));
    EXPECT_THAT(spanner_fn.signatures(0).return_type().oid(), Eq(25));
    EXPECT_THAT(spanner_fn.signatures(1).oid(), Eq(50002));
    EXPECT_THAT(spanner_fn.signatures(1).postgresql_name_paths(), SizeIs(1));
    EXPECT_THAT(spanner_fn.signatures(1).postgresql_name_paths(0).name_path(),
                ElementsAre("spanner", "fn"));
    EXPECT_THAT(spanner_fn.signatures(1).return_type().oid(), Eq(16));
  }

  {
    FunctionProto pg_fn = result[{{"pg", "fn"}, {"pg", "fn"}}];
    EXPECT_THAT(pg_fn.postgresql_name_paths(), SizeIs(1));
    EXPECT_THAT(pg_fn.postgresql_name_paths(0).name_path(),
                ElementsAre("pg", "fn"));
    EXPECT_THAT(pg_fn.signatures(), SizeIs(1));
    EXPECT_THAT(pg_fn.signatures()[0].postgresql_name_paths(), SizeIs(1));
    EXPECT_THAT(pg_fn.signatures()[0].postgresql_name_paths(0).name_path(),
                ElementsAre("pg", "fn"));
    EXPECT_THAT(pg_fn.signatures()[0].return_type().oid(), Eq(20));
  }

  {
    FunctionProto other_fn = result[{{"pg", "fn"}, {"pg", "other_fn"}}];
    EXPECT_THAT(other_fn.postgresql_name_paths(), SizeIs(1));
    EXPECT_THAT(other_fn.postgresql_name_paths(0).name_path(),
                ElementsAre("pg", "other_fn"));
    ASSERT_THAT(other_fn.signatures(), SizeIs(1));
    EXPECT_THAT(other_fn.signatures()[0].postgresql_name_paths(), SizeIs(1));
    EXPECT_THAT(other_fn.signatures()[0].postgresql_name_paths(0).name_path(),
                ElementsAre("pg", "other_fn"));
    EXPECT_THAT(other_fn.signatures()[0].return_type().oid(), Eq(20));
  }
}

}  // namespace
}  // namespace postgres_translator
