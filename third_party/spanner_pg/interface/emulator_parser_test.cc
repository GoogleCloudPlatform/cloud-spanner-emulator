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

#include "third_party/spanner_pg/interface/emulator_parser.h"

#include <memory>

#include "zetasql/public/simple_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "third_party/spanner_pg/test_catalog/emulator_catalog.h"

namespace postgres_translator {
namespace spangres {

using ::google::spanner::emulator::backend::FunctionCatalog;

namespace {

TEST(EmulatorParserTest, ParseAndAnalyzePostgreSQL) {
  std::unique_ptr<zetasql::EnumerableCatalog> catalog =
      test::GetEmulatorCatalog();
  auto type_factory = std::make_unique<zetasql::TypeFactory>();
  zetasql::AnalyzerOptions analyzer_options =
      test::GetPGEmulatorTestAnalyzerOptions();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<interfaces::PGArena> extra_arena,
                       MemoryContextPGArena::Init(nullptr));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> output,
      ParseAndAnalyzePostgreSQL(
          "select 1234567890123", catalog.get(), analyzer_options,
          type_factory.get(),
          std::make_unique<FunctionCatalog>(type_factory.get())));
}

TEST(EmulatorParserTest, TranslateTableLevelExpression) {
  auto type_factory = std::make_unique<zetasql::TypeFactory>();

  zetasql::SimpleTable simple_table("T");
  ABSL_CHECK_OK(simple_table.AddColumn(
      new zetasql::SimpleColumn("T", "K", type_factory->get_int64()), true));

  zetasql::SimpleCatalog catalog("pg simple catalog");
  catalog.AddTable(&simple_table);
  zetasql::AnalyzerOptions analyzer_options =
      test::GetPGEmulatorTestAnalyzerOptions();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<interfaces::PGArena> extra_arena,
                       MemoryContextPGArena::Init(nullptr));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      postgres_translator::interfaces::ExpressionTranslateResult result,
      TranslateTableLevelExpression(
          "\"K\" > 0", "T", catalog, analyzer_options, type_factory.get(),
          std::make_unique<FunctionCatalog>(type_factory.get())));
}

TEST(EmulatorParserTest, TranslateQueryInView) {
  auto type_factory = std::make_unique<zetasql::TypeFactory>();

  zetasql::SimpleTable simple_table("T");
  ABSL_CHECK_OK(simple_table.AddColumn(
      new zetasql::SimpleColumn("T", "K", type_factory->get_int64()),
      /*is_owned=*/true));

  zetasql::SimpleCatalog catalog("pg simple catalog");
  catalog.AddTable(&simple_table);
  zetasql::AnalyzerOptions analyzer_options =
      test::GetPGEmulatorTestAnalyzerOptions();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<interfaces::PGArena> extra_arena,
      MemoryContextPGArena::Init(/*memory_reservation_manager=*/nullptr));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      postgres_translator::interfaces::ExpressionTranslateResult result,
      TranslateQueryInView(
          R"(SELECT "K" FROM "T")", catalog, analyzer_options,
          type_factory.get(),
          std::make_unique<FunctionCatalog>(type_factory.get())));
}

}  // namespace

}  // namespace spangres
}  // namespace postgres_translator

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
