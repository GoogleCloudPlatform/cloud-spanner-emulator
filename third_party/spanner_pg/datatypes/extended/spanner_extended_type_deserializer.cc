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

#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type_deserializer.h"

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/type_deserializer.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/errors/error_catalog.h"
#include "zetasql/base/ret_check.h"

namespace postgres_translator::spangres {
namespace datatypes {

absl::StatusOr<const zetasql::ExtendedType*>
SpannerExtendedTypeDeserializer::Deserialize(
    const zetasql::TypeProto& type_proto,
    const zetasql::TypeDeserializer& type_deserializer) const {
  ZETASQL_RET_CHECK_EQ(type_proto.type_kind(), zetasql::TypeKind::TYPE_EXTENDED);
  if (type_proto.extended_type_name() ==
      GetPgJsonbType()->TypeName(zetasql::PRODUCT_EXTERNAL)) {
    return GetPgJsonbType();
  }
  if (type_proto.extended_type_name() ==
      GetPgNumericType()->TypeName(zetasql::PRODUCT_EXTERNAL)) {
    return GetPgNumericType();
  }
  return ::spangres::syntax_error_or_access_rule_violation ::
      FailedToDeserializeExtendedType(type_proto.extended_type_name());
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres
