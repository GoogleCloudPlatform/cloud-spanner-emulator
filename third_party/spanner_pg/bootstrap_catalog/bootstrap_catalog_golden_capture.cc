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
 * Captures all pg procs registered in production for the bootstrap catalog.
 */

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_accessor.h"
#include "google/protobuf/json/json.h"

using postgres_translator::PgBootstrapCatalog;
using postgres_translator::PgProcData;
using spanner::OverrideSpannerFlagsInUnittest;

int main(int argc, char* argv[]) {
  InitGoogle(argv[0], &argc, &argv, true);
  OverrideSpannerFlagsInUnittest();

  const PgBootstrapCatalog* catalog = PgBootstrapCatalog::Default();

  // Fetch all registered procs
  std::vector<const PgProcData*> procs;
  procs.reserve(catalog->TEST_GetProcsProto().size());
  for (auto& proc : catalog->TEST_GetProcsProto()) {
    procs.push_back(proc.get());
  }

  // Sort all procs by oid
  std::sort(procs.begin(), procs.end(),
            [](const PgProcData* proc1, const PgProcData* proc2) {
              return proc1->oid() < proc2->oid();
            });

  // Prints out all the procs
  for (auto& proc : procs) {
    std::string formatted_proc;

    ABSL_CHECK_OK(  // Crash OK
        google::protobuf::json::MessageToJsonString(
            *proc, &formatted_proc,
            google::protobuf::json::PrintOptions{.add_whitespace = true}));

    std::cout << formatted_proc << "\n";
  }

  return 0;
}
