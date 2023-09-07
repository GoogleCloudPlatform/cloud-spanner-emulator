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

#ifndef INTERFACE_TIMED_PARSER_H_
#define INTERFACE_TIMED_PARSER_H_

#include <string>

#include "absl/types/span.h"
#include "third_party/spanner_pg/interface/abstract_parser.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/interface/parser_output.h"

namespace postgres_translator {
namespace spangres {

// An extension of ParserInterface that handles the CPU time reporting for time
// spent outside the calling fiber.
class TimedParser : public AbstractParser {
 public:
  TimedParser() = default;
  ~TimedParser() override = default;

  // Not copyable or movable
  TimedParser(const TimedParser&) = delete;
  TimedParser& operator=(const TimedParser&) = delete;
  TimedParser(TimedParser&&) = delete;
  TimedParser& operator=(TimedParser&&) = delete;

 protected:
  absl::Status ParseIntoBatch(absl::Span<const std::string> sql_expressions,
                              interfaces::ParserBatchOutput* output) final;

  // Called before the timer is started.  If this returns a failed status, the
  // extra time callback will not be called.
  virtual absl::Status SetupParser(
      interfaces::ParserBatchOutput::Statistics* stats) {
    return absl::OkStatus();
  }

  // Do the actual parsing.  The execution of this function will be timed and
  // the value of extra_time() will be set to that duration before returning the
  // result to the caller, and therefore the return value of this function need
  // not have extra_time() set.
  virtual absl::Status ParseIntoBatchOffFiber(
      absl::Span<const std::string> sql_expressions,
      interfaces::ParserBatchOutput* output) = 0;
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // INTERFACE_TIMED_PARSER_H_
