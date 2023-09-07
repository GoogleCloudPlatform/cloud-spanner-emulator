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

#ifndef TRANSFORMER_TRANSFORMER_TEST_H_
#define TRANSFORMER_TRANSFORMER_TEST_H_

#include <memory>

#include "zetasql/public/analyzer_options.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/unittest_utils.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator {

// TransformerTest is a test fixture used by the Transformer unit tests.
// It is a friend class to ReverseTransformerVisitor so that the unit tests can
// access private methods through it, and must live in the same namespace as
// ReverseTransformerVisitor
class TransformerTest : public postgres_translator::test::ValidMemoryContext {
 public:
  void SetUp() override {
    ValidMemoryContext::SetUp();
    analyzer_options_ = spangres::test::GetSpangresTestAnalyzerOptions();
    forward_transformer_ = std::make_unique<ForwardTransformer>(
        spangres::test::GetSpangresTestCatalogAdapter(analyzer_options_));
  }

  void TearDown() override {
    forward_transformer_.reset();
    ValidMemoryContext::TearDown();
  }

 protected:

  zetasql::AnalyzerOptions analyzer_options_;
  std::unique_ptr<ForwardTransformer> forward_transformer_;
};

}  // namespace postgres_translator
#endif  // TRANSFORMER_TRANSFORMER_TEST_H_
