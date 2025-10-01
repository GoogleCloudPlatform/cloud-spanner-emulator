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

/*
 * Captures all functions registered for Spangres (PG dialect) in production
 * with flag overrides enabled.
 */

#include <iostream>
#include <string>

#include "absl/flags/parse.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog_golden.h"
#include "third_party/spanner_pg/emulator_flags.h"

using ::postgres_translator::InitializeEngineSystemCatalog;
using ::postgres_translator::SpangresSystemCatalogGolden;
using ::postgres_translator::spangres::SpangresSystemCatalog;
using ::spanner::OverrideSpannerFlagsInUnittest;

ABSL_DECLARE_FLAG(bool, spangres_use_emulator_numeric_type);
ABSL_DECLARE_FLAG(bool, spangres_use_emulator_jsonb_type);
ABSL_DECLARE_FLAG(bool, spangres_use_emulator_oid_type);
ABSL_DECLARE_FLAG(bool, spangres_enable_fts_text_array_signatures);
ABSL_DECLARE_FLAG(bool, spangres_enable_generate_series_function);
ABSL_DECLARE_FLAG(bool, spangres_enable_ilike_operator);
ABSL_DECLARE_FLAG(bool, spangres_enable_pg_vector_search_ann_functions);
ABSL_DECLARE_FLAG(bool, spangres_enable_tokenize_number_array_signatures);

int main(int argc, char* argv[]) {
  InitGoogle(argv[0], &argc, &argv, true);
  OverrideSpannerFlagsInUnittest();
  absl::SetFlag(&FLAGS_spangres_enable_fts_text_array_signatures, false);
  absl::SetFlag(&FLAGS_spangres_enable_generate_series_function, false);
  absl::SetFlag(&FLAGS_spangres_enable_ilike_operator, false);
  absl::SetFlag(&FLAGS_spangres_enable_pg_vector_search_ann_functions, false);
  absl::SetFlag(&FLAGS_spangres_enable_tokenize_number_array_signatures, false);
  absl::SetFlag(&FLAGS_spangres_use_emulator_catalog, true);
  absl::SetFlag(&FLAGS_spangres_use_emulator_numeric_type, true);
  absl::SetFlag(&FLAGS_spangres_use_emulator_jsonb_type, true);
  absl::SetFlag(&FLAGS_spangres_use_emulator_oid_type, true);

  SpangresSystemCatalog* catalog =
      InitializeEngineSystemCatalog(/*is_emulator=*/true);
  SpangresSystemCatalogGolden golden(catalog);

  std::string output = golden.Capture();
  std::cout << output << "\n";

  return 0;
}
