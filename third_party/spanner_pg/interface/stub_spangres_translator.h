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

#ifndef INTERFACE_STUB_SPANGRES_TRANSLATOR_H_
#define INTERFACE_STUB_SPANGRES_TRANSLATOR_H_

#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"

namespace postgres_translator {
namespace spangres {

// Stub version of SpangresTranslator that only returns an error.
class StubSpangresTranslator : public interfaces::SpangresTranslatorInterface {
 public:
  StubSpangresTranslator() = default;
  ~StubSpangresTranslator() override = default;

  // Not copyable or movable
  StubSpangresTranslator(const StubSpangresTranslator&) = delete;
  StubSpangresTranslator& operator=(const StubSpangresTranslator&) = delete;
  StubSpangresTranslator(StubSpangresTranslator&&) = delete;
  StubSpangresTranslator& operator=(StubSpangresTranslator&&) = delete;

  // Returns a FailedPreconditionError.
  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>> TranslateQuery(
      interfaces::TranslateQueryParams params) override {
    return absl::FailedPreconditionError("invoked stub SpangresTranslator");
  }

  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
  TranslateParsedQuery(interfaces::TranslateParsedQueryParams params) override {
    return absl::FailedPreconditionError("invoked stub SpangresTranslator");
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedTableLevelExpression(
      interfaces::TranslateParsedQueryParams params,
      absl::string_view table_name) override {
    return absl::FailedPreconditionError("invoked stub SpangresTranslator");
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult> TranslateQueryInView(
      interfaces::TranslateQueryParams params) override {
    return absl::FailedPreconditionError("invoked stub SpangresTranslator");
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedQueryInView(
      interfaces::TranslateParsedQueryParams params) override {
    return absl::FailedPreconditionError("invoked stub SpangresTranslator");
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateTableLevelExpression(interfaces::TranslateQueryParams params,
                                absl::string_view table_name) {
    return absl::FailedPreconditionError("invoked stub SpangresTranslator");
  }
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // INTERFACE_STUB_SPANGRES_TRANSLATOR_H_
