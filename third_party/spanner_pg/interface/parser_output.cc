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

#include "third_party/spanner_pg/interface/parser_output.h"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/interface/pg_arena.h"

namespace postgres_translator {
namespace interfaces {

ParserOutput::ParserOutput(List* parse_tree, ParserOutputMetadata metadata)
    : parse_tree_(parse_tree), metadata_(metadata) {}

ParserBatchOutput::ParserBatchOutput(std::unique_ptr<PGArena> arena,
                                     absl::Duration extra_time)
    : ParserBatchOutput(std::move(arena),
                        std::vector<absl::StatusOr<ParserOutput>>(),
                        absl::OkStatus(), extra_time) {}

ParserBatchOutput ParserBatchOutput::FromFailedGlobalStatus(
    std::size_t batch_size, absl::Status failed_global_status,
    absl::Duration extra_time) {
  if (failed_global_status.ok()) {
    failed_global_status =
        absl::InternalError("passed OK status to FromFailedGlobalStatus");
  }

  std::vector<absl::StatusOr<ParserOutput>> output;
  output.reserve(batch_size);
  for (std::size_t i = 0; i < batch_size; ++i) {
    output.emplace_back(failed_global_status);
  }
  return ParserBatchOutput(nullptr, std::move(output), failed_global_status,
                           extra_time);
}

ParserSingleOutput ParserSingleOutput::FromBatchOutput(
    ParserBatchOutput batch_output) {
  if (!batch_output.global_status().ok()) {
    return ParserSingleOutput(nullptr, batch_output.global_status(),
                              batch_output.statistics());
  }

  if (batch_output.output().size() != 1) {
    return ParserSingleOutput(
        nullptr,
        absl::InternalError(absl::StrCat(
            "attempted to convert a ParserBatchOutput with batch size of ",
            batch_output.output().size(), " to a ParserSingleOutput")),
        batch_output.statistics());
  }

  return ParserSingleOutput(std::move(batch_output.arena_),
                            std::move(batch_output.mutable_output()->at(0)),
                            batch_output.statistics());
}

}  // namespace interfaces
}  // namespace postgres_translator
