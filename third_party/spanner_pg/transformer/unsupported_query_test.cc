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

#include <memory>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/util/unittest_utils.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

// The unit tests in this file are PostgreSQL queries which are unsupported in
// Spangres and do not have a semantic equivalent in ZetaSQL. The goal of
// each unit test is to verify that Spangres returns a reasonable error message
// when a query is unsupported, instead of crashing in the analyzer/transformer
// or returning an incorrect result.
//
// Unsupported queries which have a semantic equivalent in ZetaSQL should live
// in the filed-based golden tests which run the Spangres analyzer and the
// ZetaSQL analyzer.
//
// The queries in this file cannot be run through the ZetaSQL analyzer and
// cannot live in the file-based golden tests.
namespace postgres_translator::spangres::test {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

using UnsupportedQueryTest = ::postgres_translator::test::ValidMemoryContext;

absl::Status RunQuery(const std::string& sql) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  List* pg_parse_tree;
  ZETASQL_ASSIGN_OR_RETURN(pg_parse_tree, ParseFromPostgres(sql));

  Query* pg_query;
  ZETASQL_ASSIGN_OR_RETURN(pg_query, AnalyzeFromPostgresForTest(sql, pg_parse_tree,
                                                        analyzer_options));

  auto transformer = std::make_unique<ForwardTransformer>(
      catalog_adapter_holder->ReleaseCatalogAdapter());

  std::unique_ptr<zetasql::ResolvedStatement> gsql_statement;
  ZETASQL_ASSIGN_OR_RETURN(gsql_statement,
                   transformer->BuildGsqlResolvedStatement(*pg_query));

  return absl::OkStatus();
}

void ExpectQueryError(const std::string& sql, absl::StatusCode error_code,
                      const std::string& error_substring) {
  EXPECT_THAT(RunQuery(sql), StatusIs(error_code, HasSubstr(error_substring)));
}

TEST_F(UnsupportedQueryTest, DeleteCursor) {
  ExpectQueryError("delete from keyvalue where current of cursor",
                   absl::StatusCode::kUnimplemented,
                   "CURRENT OF statements are not supported");
}

TEST_F(UnsupportedQueryTest, DeleteUsing) {
  ExpectQueryError(
      "delete from keyvalue using \"KeyValue2\" kv2 where keyvalue.key = "
      "kv2.key",
      absl::StatusCode::kUnimplemented, "USING statements are not supported");
}

TEST_F(UnsupportedQueryTest, CastToInt4) {
  ExpectQueryError("select cast(key as int4) from keyvalue",
                   absl::StatusCode::kInvalidArgument,
                   "cannot cast type bigint to integer");
}

}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
