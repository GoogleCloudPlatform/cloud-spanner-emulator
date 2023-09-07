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

#ifndef INTERFACE_PARSER_INTERFACE_H_
#define INTERFACE_PARSER_INTERFACE_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/interface/parser_output.h"

namespace postgres_translator {
namespace interfaces {

class ParserParamsBuilder;

class ParserParams {
 public:
  absl::Span<const std::string> sql_expressions() const {
    return sql_expressions_;
  }

  // Gives ownership of this object's MemoryReservationManager to the caller.
  // Optional.  If provided, this is used by Spangres to request permission from
  // the caller to allocate memory.
  std::unique_ptr<MemoryReservationManager> TransferMemoryReservationManager() {
    return std::move(memory_reservation_manager_);
  }

 private:
  absl::Span<const std::string> sql_expressions_;
  std::unique_ptr<MemoryReservationManager> memory_reservation_manager_;

  friend class ParserParamsBuilder;
};

class ParserParamsBuilder {
 public:
  explicit ParserParamsBuilder(absl::Span<const std::string> sql_expressions) {
    params_.sql_expressions_ = sql_expressions;
  }

  explicit ParserParamsBuilder(const std::string& sql_expression)
      : ParserParamsBuilder(absl::MakeConstSpan(&sql_expression, 1)) {}

  // Optional.  If provided, this is used by Spangres to request permission from
  // the caller to allocate memory.
  ParserParamsBuilder& SetMemoryReservationManager(
      std::unique_ptr<MemoryReservationManager> memory_reservation_manager) {
    params_.memory_reservation_manager_ = std::move(memory_reservation_manager);
    return *this;
  }

  ParserParams Build() { return std::move(params_); }

 private:
  ParserParams params_;
};

class ParserInterface {
 public:
  ParserInterface() = default;
  virtual ~ParserInterface() = default;

  // Not copyable or movable
  ParserInterface(const ParserInterface&) = delete;
  ParserInterface& operator=(const ParserInterface&) = delete;
  ParserInterface(ParserInterface&&) = delete;
  ParserInterface& operator=(ParserInterface&&) = delete;

  virtual ParserBatchOutput ParseBatch(ParserParams params) = 0;

  ParserSingleOutput Parse(const std::string& sql_expression,
                           std::unique_ptr<MemoryReservationManager>
                               memory_reservation_manager = nullptr) {
    return ParserSingleOutput::FromBatchOutput(ParseBatch(
        ParserParamsBuilder(sql_expression)
            .SetMemoryReservationManager(std::move(memory_reservation_manager))
            .Build()));
  }

  virtual absl::StatusOr<ParserInterface*> GetParser() { return this; }
};

}  // namespace interfaces
}  // namespace postgres_translator

#endif  // INTERFACE_PARSER_INTERFACE_H_
