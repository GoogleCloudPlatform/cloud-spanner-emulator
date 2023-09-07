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

#ifndef TEST_CATALOG_TEST_CATALOG_H_
#define TEST_CATALOG_TEST_CATALOG_H_

#include <memory>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"

namespace postgres_translator::spangres::test {

// Return the Spanner Analyzer Options in cloud mode with slight modifications
// to the language options specific to Spangres.
zetasql::AnalyzerOptions GetSpangresTestAnalyzerOptions();

std::unique_ptr<EngineBuiltinFunctionCatalog>
GetSpangresTestBuiltinFunctionCatalog(
    const zetasql::LanguageOptions& language_options);

// If the test catalog singleton is not already initialized, initialize it.
// Return a raw pointer to the test catalog singleton.
// This test catalog is a wrapper over SimpleCatalog with added conversions for
// extended types that is quick to load and used in most Spangres tests that do
// not test schemas. If unsure, use this catalog.
//
// b/257978803: Delete the simple catalog.
zetasql::EnumerableCatalog* GetSpangresTestCatalog_UNUSED();

// If the EngineSystemCatalog singleton is not already initialized, initialize
// it using the SpangresBuiltinFunctionCatalog.
// Return a raw pointer to the EngineSystemCatalog singleton.
EngineSystemCatalog* GetSpangresTestSystemCatalog(
    const zetasql::LanguageOptions& language_options =
        GetSpangresTestAnalyzerOptions().language());

// Returns a CatalogAdapter with the spangres test system catalog.
// If a user catalog is provided (only for RQG tests), use that catalog as the
// engine user catalog. Otherwise, use the spangres test user catalog.
std::unique_ptr<CatalogAdapter> GetSpangresTestCatalogAdapter(
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::EnumerableCatalog* rqg_user_catalog = nullptr,
    absl::flat_hash_map<int, int> token_locations = {});

// Returns a CatalogAdapterHolder with the spangres test system catalog.
// If a user catalog is provided (only for RQG tests), use that catalog as the
// engine user catalog. Otherwise, use the spangres test user catalog.
std::unique_ptr<CatalogAdapterHolder> GetSpangresTestCatalogAdapterHolder(
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::EnumerableCatalog* rqg_user_catalog = nullptr,
    absl::flat_hash_map<int, int> token_locations = {});

}  // namespace postgres_translator::spangres::test

#endif  // TEST_CATALOG_TEST_CATALOG_H_
