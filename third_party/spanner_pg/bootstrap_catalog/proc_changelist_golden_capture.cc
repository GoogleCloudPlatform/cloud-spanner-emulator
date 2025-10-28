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

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/flags/parse.h"
#include "absl/container/flat_hash_map.h"
#include "third_party/spanner_pg/bootstrap_catalog/proc_changelist.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

using ::postgres_translator::TEST_GetProcAndDefaultArgsToUpdate;
using ::spanner::OverrideSpannerFlagsInUnittest;

int main(int argc, char* argv[]) {
  InitGoogle(argv[0], &argc, &argv, true);
  OverrideSpannerFlagsInUnittest();

  const absl::flat_hash_map<Oid, std::vector<std::string>>&
      procs_and_default_args = TEST_GetProcAndDefaultArgsToUpdate();

  std::vector<std::pair<Oid, std::vector<std::string>>>
      sorted_procs_and_default_args(procs_and_default_args.begin(),
                                    procs_and_default_args.end());
  std::sort(sorted_procs_and_default_args.begin(),
            sorted_procs_and_default_args.end(),
            [](const std::pair<Oid, std::vector<std::string>>& proc1,
               const std::pair<Oid, std::vector<std::string>>& proc2) {
              return proc1.first < proc2.first;
            });

  for (const auto& [proc, default_args] : sorted_procs_and_default_args) {
    std::cout << "[\n";
    std::cout << "\tproc: " << proc << "\n";
    for (const auto& default_arg : default_args) {
      std::cout << "\tdefault_arg: \"" << default_arg << "\"\n";
    }
    std::cout << "],\n";
  }
  std::cout << "\n";
}
