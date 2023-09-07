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

#include "third_party/spanner_pg/util/unittest_utils.h"

#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/shims/memory_context_manager.h"
#include "third_party/spanner_pg/shims/memory_reservation_holder.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator::spangres::test {
namespace {

using ::postgres_translator::test::ValidMemoryContext;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

TEST_F(ValidMemoryContext, InvalidArgumentParseTest) {
  std::string sql = "select 1; select 2";
  EXPECT_THAT(ParseFromPostgres(sql),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Only one query is permitted")));
}

TEST(UnittestUtilsTest, InvalidSetupAnalyzerTest) {
  std::string sql = "select 1";
  // No valid MemoryReservationManager set up yet, expect parser error.
  EXPECT_THAT(
      ParseFromPostgres(sql),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("Thread-local MemoryContext required by parser")));
  List* parse_tree;

  {
    // Set up a MemoryReservationManager, expect parser to succeed.
    auto reservation_manager = std::make_unique<StubMemoryReservationManager>();
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        MemoryReservationHolder reservation_holder,
        MemoryReservationHolder::Create(reservation_manager.get()));
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context,
                         MemoryContextManager::Init("UnittestUtils test"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(parse_tree, ParseFromPostgres(sql));
    ZETASQL_ASSERT_OK(memory_context.Clear());
  }

  // Now there is no MemoryReservationManager, expect analyzer error.
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  EXPECT_THAT(AnalyzeFromPostgresForTest(sql, parse_tree, analyzer_options),
              StatusIs(absl::StatusCode::kInternal));

  // No valid CatalogAdapter set up yet, expect error.
  EXPECT_THAT(AnalyzeFromPostgresForTest(sql, parse_tree, analyzer_options),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(ValidMemoryContext, InvalidInputAnalyzerTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  std::string sql = "select 1";
  // No valid parse tree passed in, expect error.
  EXPECT_THAT(AnalyzeFromPostgresForTest(sql, nullptr, analyzer_options),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(ValidMemoryContext, ParseAndAnalyzeFromPostgresTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // A valid sql string, expect success.
  std::string sql = "select 1";
  List* parse_tree;
  ZETASQL_ASSERT_OK_AND_ASSIGN(parse_tree, ParseFromPostgres(sql));
  ASSERT_NE(parse_tree, nullptr);

  Query* query;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      query, AnalyzeFromPostgresForTest(sql, parse_tree, analyzer_options));

  EXPECT_NE(query, nullptr);
}

TEST(UnittestUtilsTest, ParseAndAnalyzeFromZetaSQLNegativeTest) {
  // No valid catalog, expect error.
  std::string sql = "select 1";
  zetasql::TypeFactory type_factory;
  std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output;

  // Invalid zetasql::TypeFactory, expect error.
  EXPECT_THAT(
      ParseAndAnalyzeFromZetaSQLForTest(sql, /*type_factory=*/nullptr,
                                          &gsql_output),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("A valid zetasql::TypeFactory object is needed")));

  // Invalid zetasql::AnalyzerOutput, expect error.
  EXPECT_THAT(
      ParseAndAnalyzeFromZetaSQLForTest(sql, &type_factory,
                                          /*gsql_output=*/nullptr),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("A valid zetasql::AnalyzerOutput object is needed")));
}

TEST(UnittestUtilsTest, ParseAndAnalyzeFromZetaSQLTest) {
  std::string sql = "select 1";
  zetasql::TypeFactory type_factory;
  std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output;

  // Valid arguments, expect success.
  ZETASQL_ASSERT_OK(
      ParseAndAnalyzeFromZetaSQLForTest(sql, &type_factory, &gsql_output));
  EXPECT_NE(gsql_output->resolved_statement(), nullptr);
}

TEST(UnittestUtilsTest, PrintPhases) {
  EXPECT_EQ(PrintPhases(kParse), "Phases: [Parse]");
  EXPECT_EQ(PrintPhases(kAnalyze), "Phases: [Analyze]");
  EXPECT_EQ(PrintPhases(kTransform), "Phases: [Transform]");
  EXPECT_EQ(PrintPhases(kReverseTransform), "Phases: [ReverseTransform]");
  EXPECT_EQ(PrintPhases(kDeparse), "Phases: [Deparse]");
  EXPECT_EQ(PrintPhases(kAllPhases), "Phases: [AllPhases]");
  EXPECT_EQ(PrintPhases(kTransformRoundTrip), "Phases: [TransformRoundTrip]");
  EXPECT_EQ(PrintPhases(kParse | kAnalyze), "Phases: [Parse, Analyze]");
  EXPECT_EQ(PrintPhases(kReverseTransform | kDeparse | kTransform),
            "Phases: [Transform, ReverseTransform, Deparse]");
}

}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
