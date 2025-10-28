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

#ifndef CATALOG_SPANGRES_FUNCTION_MAPPER_H_
#define CATALOG_SPANGRES_FUNCTION_MAPPER_H_

#include <cstdint>
#include <vector>

#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/codegen/postgresql_catalog.pb.h"

namespace postgres_translator {
class SpangresFunctionMapper {
 public:
  SpangresFunctionMapper(const EngineSystemCatalog* catalog)
      : catalog_(catalog) {}

  // Maps a catalog function into one or more `PostgresFunctionArguments` that
  // will be added to the Spangres System Catalog. Each signature is mapped to a
  // single `PostgresFunctionArgument`.
  // It is assumed here that the given function contains all signatures for a
  // postgresql_name_path. If the function or any of its signatures have more
  // than one postgresql_name_path an error will be returned. If the function
  // postgresql_name_path differs from any signature's postgresql_name_path an
  // error will be returned.
  absl::StatusOr<std::vector<PostgresFunctionArguments>>
  ToPostgresFunctionArguments(const FunctionProto& function) const;

 private:
  const EngineSystemCatalog* catalog_;

  const zetasql::Type* FindTypeByOid(uint32_t oid) const;

  absl::StatusOr<zetasql::FunctionArgumentType> FunctionArgumentTypeFrom(
      ArgumentTypeProto arg_type) const;

  absl::StatusOr<zetasql::FunctionArgumentType> FunctionArgumentTypeFrom(
      FunctionArgumentProto arg) const;
};
}  // namespace postgres_translator

#endif  // CATALOG_SPANGRES_FUNCTION_MAPPER_H_
