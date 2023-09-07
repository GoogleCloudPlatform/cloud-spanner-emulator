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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/util/unittest_utils.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator::spangres::test {
namespace {

// PostgreSQL End-to-end Tests
//
// This file exists for integration tests over the full Spangres-enabled
// PostgreSQL stack (parser + analyzer + catalog shims).

using PostgresE2ETest = ::postgres_translator::test::ValidMemoryContext;

absl::StatusOr<Query*> ParseAndAnalyzeFromPostgres(const std::string& sql) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  List* pg_parse_tree;
  ZETASQL_ASSIGN_OR_RETURN(pg_parse_tree, ParseFromPostgres(sql.c_str()));
  ZETASQL_RET_CHECK_EQ(list_length(pg_parse_tree), 1);
  return AnalyzeFromPostgresForTest(sql, pg_parse_tree,
                                    GetSpangresTestAnalyzerOptions());
}

// Simple demonstrator of a single query hint passing through the full PG stack
// and ending up in a Query Tree.
TEST_F(PostgresE2ETest, HintParsesAndAnalyzes) {
  const std::string test_string = "/*@ statement_hint = 1 */ select 1;";

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Query* query,
                       ParseAndAnalyzeFromPostgres(test_string));

  // Verify the Query Tree has a single hint node which contains the name
  // "statement_hint" and the Const (INT8) value 1.
  ASSERT_NE(query, nullptr);
  ASSERT_NE(query->statementHints, nullptr);
  EXPECT_EQ(list_length(query->statementHints), 1);
  const DefElem* hint = linitial_node(DefElem, query->statementHints);
  ASSERT_NE(hint, nullptr);
  EXPECT_STREQ(hint->defname, "statement_hint");
  Const* node = castNode(Const, hint->arg);
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->consttype, INT8OID);
  EXPECT_EQ(DatumGetInt64(node->constvalue), 1);
}

}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
