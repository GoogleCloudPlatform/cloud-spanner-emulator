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

#ifndef INTERFACE_PARSER_WITHOUT_SERIALIZATION_H_
#define INTERFACE_PARSER_WITHOUT_SERIALIZATION_H_

#include "third_party/spanner_pg/interface/abstract_parser.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/shims/error_shim.h"

namespace postgres_translator {
namespace spangres {

// The most basic working implementation of ParserInterface for use in tests.
class ParserWithoutSerialization : public AbstractParser {
 public:
  ParserWithoutSerialization() = default;
  ~ParserWithoutSerialization() override = default;

  // Not copyable or movable
  ParserWithoutSerialization(const ParserWithoutSerialization&) = delete;
  ParserWithoutSerialization& operator=(const ParserWithoutSerialization&) =
      delete;
  ParserWithoutSerialization(ParserWithoutSerialization&&) = delete;
  ParserWithoutSerialization& operator=(ParserWithoutSerialization&&) = delete;

 protected:
  absl::Status ParseIntoBatch(absl::Span<const std::string> sql_expressions,
                              interfaces::ParserBatchOutput* output) override;

 private:
  absl::StatusOr<interfaces::ParserOutput> Parse(const std::string& sql);
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // INTERFACE_PARSER_WITHOUT_SERIALIZATION_H_
