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

#ifndef TRANSFORMER_TEST_POSTGRES_TRANSFORMER_H_
#define TRANSFORMER_TEST_POSTGRES_TRANSFORMER_H_

#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

// Parse, analyze, and transform a given query using PostgreSQL. Also runs the
// serializer and deserializer on the PostgreSQL Parse and Query trees.
absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
ParseAndAnalyzeSQLString(
    const std::string& sql, zetasql::EnumerableCatalog* catalog,
    std::unique_ptr<EngineBuiltinFunctionCatalog> function_catalog,
    const zetasql::AnalyzerOptions& analyzer_options,
    std::vector<std::string>* analyze_query_trees,
    const uint64_t query_id = 0
);
}  // namespace postgres_translator

#endif  // TRANSFORMER_TEST_POSTGRES_TRANSFORMER_H_
