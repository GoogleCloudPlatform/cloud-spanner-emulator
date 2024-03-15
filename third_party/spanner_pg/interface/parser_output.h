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

#ifndef INTERFACE_PARSER_OUTPUT_H_
#define INTERFACE_PARSER_OUTPUT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/interface/pg_arena.h"

// Defined in PostgreSQL
extern "C" {
struct List;
struct Node;
}

namespace postgres_translator {
namespace interfaces {

// Additional data about a parsed query other than the parse tree itself.
struct ParserOutputMetadata {
  // A map with all token start locations and keys and the corresponding token
  // (exclusive) end locations as values
  absl::flat_hash_map<int, int> token_locations;

  // The size of the serialized parse tree received from the parser.
  size_t serialized_parse_tree_size = 0;
};

// The results returned by the parser for each SQL expression.
class ParserOutput {
 public:
  ParserOutput(List* parse_tree, ParserOutputMetadata metadata);

  // Not copyable
  ParserOutput(const ParserOutput&) = delete;
  ParserOutput& operator=(const ParserOutput&) = delete;

  // Movable
  ParserOutput(ParserOutput&&) noexcept = default;
  ParserOutput& operator=(ParserOutput&&) noexcept = default;

  const ParserOutputMetadata& metadata() const { return metadata_; }

  // PostgreSQL parser output
  List* parse_tree() const { return parse_tree_; }

  // A map with all token start locations as keys and the corresponding token
  // (exclusive) end locations as values, or an empty map if these locations are
  // not known.
  const absl::flat_hash_map<int, int>& token_locations() const {
    return metadata().token_locations;
  }

  size_t serialized_parse_tree_size() const {
    return metadata().serialized_parse_tree_size;
  }

 private:
  ParserOutput() = default;

  List* parse_tree_;
  ParserOutputMetadata metadata_;
};

class ParserSingleOutput;

// The overall output of the parser batch API.  This class has subtle
// dependencies on thread-local variables used inside PostgreSQL and should only
// be used on the same thread where it was created.
// TODO: Make this work nicely across threads and remove the above
// comment about thread-locals.
class ParserBatchOutput {
 public:
  struct Statistics {
    // The amount of CPU time spent outside the process by the parser, or zero
    // if no such time was spent. Deprecated and unreliable, so temporarily set
    // to wall time until removed.
    absl::Duration extra_time = absl::ZeroDuration();

    // CPU time spent parsing a statement. Zero if we can't reasonably fetch the
    // CPU time.
    absl::Duration parse_cpu_time = absl::ZeroDuration();

    // Wall time spent parsing a statement. Zero if for some reason we can't
    // reasonably fetch the wall time.
    absl::Duration parse_wall_time = absl::ZeroDuration();

    // The size in bytes of the input query to the parser.
    size_t sql_expressions_size = 0;

    // The size in bytes of the serialized parse tree to be sent as a response
    // from the parser.
    size_t serialized_parse_tree_size = 0;
  };

  // Constructs an empty object with OK global status.
  explicit ParserBatchOutput(std::unique_ptr<PGArena> arena,
                             absl::Duration extra_time = absl::ZeroDuration());

  // Not copyable
  ParserBatchOutput(const ParserBatchOutput&) = delete;
  ParserBatchOutput& operator=(const ParserBatchOutput&) = delete;

  // Movable
  ParserBatchOutput(ParserBatchOutput&&) = default;
  ParserBatchOutput& operator=(ParserBatchOutput&&) = default;

  // Constructs an object with a failed global_status(), with output() filled in
  // with the appropriate number of copies of global_status().
  static ParserBatchOutput FromFailedGlobalStatus(
      std::size_t batch_size, absl::Status failed_global_status,
      absl::Duration extra_time = absl::ZeroDuration());

  // The list of individual parser results in the same order as the batch of
  // SQL statements that was passed to the parser.  The returned vector
  // always has the same length as the input batch.  In the case where
  // global_status() returns an error, this vector will be the appropriate
  // number of copies of that failed status.
  const std::vector<absl::StatusOr<ParserOutput>>& output() const {
    return output_;
  }
  std::vector<absl::StatusOr<ParserOutput>>* mutable_output() {
    return &output_;
  }

  // A failed status indicates that the entire batch call to the parser failed.
  absl::Status global_status() const { return global_status_; }

  // The amount of CPU time spent outside the process by the parser, or zero if
  // no such time was spent.
  absl::Duration extra_time() const { return statistics().extra_time; }
  void set_extra_time(absl::Duration extra_time) {
    mutable_statistics()->extra_time = extra_time;
  }

  // The amount of CPU time spent by the parser, or zero if we can't tell.
  absl::Duration parse_cpu_time() const { return statistics().parse_cpu_time; }
  void set_parse_cpu_time(absl::Duration parse_cpu_time) {
    mutable_statistics()->parse_cpu_time = parse_cpu_time;
  }

  // The amount of wall time spent by the parser, or zero if we can't te..
  absl::Duration parse_wall_time() const {
    return statistics().parse_wall_time;
  }
  void set_parse_wall_time(absl::Duration parse_wall_time) {
    mutable_statistics()->parse_wall_time = parse_wall_time;
  }

  const Statistics& statistics() const { return statistics_; }
  Statistics* mutable_statistics() { return &statistics_; }

 private:
  ParserBatchOutput(std::unique_ptr<PGArena> arena,
                    std::vector<absl::StatusOr<ParserOutput>> output,
                    absl::Status global_status, absl::Duration extra_time)
      : arena_(std::move(arena)),
        output_(std::move(output)),
        global_status_(global_status),
        statistics_(Statistics{extra_time}) {}

  std::unique_ptr<PGArena> arena_;
  std::vector<absl::StatusOr<ParserOutput>> output_;
  absl::Status global_status_;
  Statistics statistics_;

  friend class ParserSingleOutput;
};

// A simplified version of ParserBatchOutput for when the batch only contains a
// single SQL expression.  This class has subtle dependencies on thread-local
// variables used inside PostgreSQL and should only be used on the same thread
// where it was created.
// TODO: Make this work nicely across threads and remove the above
// comment about thread-locals.
class ParserSingleOutput {
 public:
  // Convert a ParserBatchOutput with a batch size of exactly 1 to a
  // ParserSingleOutput.
  static ParserSingleOutput FromBatchOutput(ParserBatchOutput batch_output);

  const absl::StatusOr<ParserOutput>& output() const { return output_; }
  absl::StatusOr<ParserOutput>* mutable_output() { return &output_; }

  // The amount of CPU time spent outside the process by the parser, or zero if
  // no such time was spent.
  absl::Duration extra_time() const { return statistics().extra_time; }

  const ParserBatchOutput::Statistics& statistics() const {
    return statistics_;
  }
  ParserBatchOutput::Statistics* mutable_statistics() { return &statistics_; }

 private:
  ParserSingleOutput(std::unique_ptr<PGArena> arena,
                     absl::StatusOr<ParserOutput> output,
                     const ParserBatchOutput::Statistics& statistics)
      : arena_(std::move(arena)),
        output_(std::move(output)),
        statistics_(statistics) {}

  std::unique_ptr<PGArena> arena_;
  absl::StatusOr<ParserOutput> output_;
  ParserBatchOutput::Statistics statistics_;
};

}  // namespace interfaces
}  // namespace postgres_translator

#endif  // INTERFACE_PARSER_OUTPUT_H_
