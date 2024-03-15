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

#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/value_content.h"
#include "absl/flags/flag.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "zetasql/base/ret_check.h"

namespace postgres_translator::spangres::datatypes {

using ::zetasql::LanguageOptions;
using ::zetasql::ProductMode;
using ::zetasql::TypeParameters;
using ::zetasql::TypeParameterValue;
using ::zetasql::TypeProto;
using ::zetasql::ValueContent;
using ::zetasql::ValueProto;
using TypeAnnotationCode = ::google::spanner::v1::TypeAnnotationCode;

class PgOidType : public SpannerExtendedType {
 public:
  PgOidType() : SpannerExtendedType(TypeAnnotationCode::PG_OID) {}

  bool SupportsOrdering(
      const LanguageOptions& /*unused*/,
      std::string* /*unused_type_description*/) const override {
    return true;
  }

  bool SupportsEquality() const override { return true; }

  bool SupportsEquality(const LanguageOptions& /*unused*/) const override {
    return SupportsEquality();
  }

  std::string ShortTypeName(ProductMode /*unused*/) const override {
    return "PG.OID";
  }

  std::string TypeName(ProductMode unused) const override {
    return ShortTypeName(unused);
  }

  absl::StatusOr<TypeParameters> ValidateAndResolveTypeParameters(
      const std::vector<TypeParameterValue>& type_parameter_values,
      ProductMode mode) const override {
    return absl::InvalidArgumentError(absl::StrCat(
        ShortTypeName(/*unused=*/mode), " does not support type parameters"));
  }

  absl::Status ValidateResolvedTypeParameters(
      const TypeParameters& type_parameters, ProductMode mode) const override {
    return absl::InvalidArgumentError(absl::StrCat(
        ShortTypeName(/*unused=*/mode), " does not support type parameters"));
  }

 protected:
  absl::HashState HashTypeParameter(absl::HashState state) const override {
    return state;  // Type doesn't have parameters.
  }

  absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const override {
    type_proto->set_type_kind(zetasql::TypeKind::TYPE_EXTENDED);
    type_proto->set_extended_type_name(TypeName(zetasql::PRODUCT_EXTERNAL));
    return absl::OkStatus();
  }

  int64_t GetEstimatedOwnedMemoryBytesSize() const override {
    return sizeof(*this);
  }

 private:
  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override {
    if (no_grouping_type != nullptr) {
      *no_grouping_type = nullptr;
    }
    return true;
  }

  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override {
    if (no_partitioning_type != nullptr) {
      *no_partitioning_type = this;
    }
    return true;
  }

  bool EqualsForSameKind(const Type* that, bool equivalent) const override {
    return that == this;
  }

  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override {
    absl::StrAppend(debug_string,
                    ShortTypeName(/*unused=*/zetasql::PRODUCT_EXTERNAL));
  }

  void ClearValueContent(const ValueContent& value) const override {}

  bool ValueContentEquals(
      const ValueContent& x, const ValueContent& y,
      const zetasql::ValueEqualityCheckOptions& options) const override {
    return x.GetAs<int64_t>() == y.GetAs<int64_t>();
  }

  bool ValueContentLess(const ValueContent& x, const ValueContent& y,
                        const Type* other_type) const override {
    return x.GetAs<int64_t>() < y.GetAs<int64_t>();
  }

  uint64_t GetValueContentExternallyAllocatedByteSize(
      const ValueContent& value) const override {
    return 0;
  }

  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const override {
    return absl::HashState::combine(std::move(state), value.GetAs<int64_t>());
  }

  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override {
    auto oid_str = absl::StrCat(value.GetAs<int64_t>());
    if (options.mode == FormatValueContentOptions::Mode::kSQLLiteral ||
        options.mode == FormatValueContentOptions::Mode::kSQLExpression) {
      return absl::StrCat("CAST(",
                          zetasql::ToSingleQuotedStringLiteral(oid_str),
                          " AS PG.OID)");
    }
    return oid_str;
  }

  absl::Status SerializeValueContent(const ValueContent& value,
                                     ValueProto* value_proto) const override {
    value_proto->set_int64_value(value.GetAs<int64_t>());
    return absl::OkStatus();
  }

  absl::Status DeserializeValueContent(const ValueProto& value_proto,
                                       ValueContent* value) const override {
    if (!value_proto.has_int64_value()) {
      return TypeMismatchError(value_proto);
    }
    value->set(value_proto.int64_value());
    return absl::OkStatus();
  }
};

const SpannerExtendedType* GetPgOidType() {
  static PgOidType* s_pg_oid_type = new PgOidType();
  return s_pg_oid_type;
}

const zetasql::ArrayType* GetPgOidArrayType() {
  static const zetasql::ArrayType* s_pg_oid_arr_type = []() {
    const zetasql::ArrayType* pg_oid_array_type = nullptr;
    ABSL_CHECK_OK(
        GetTypeFactory()->MakeArrayType(GetPgOidType(), &pg_oid_array_type));
    return pg_oid_array_type;
  }();
  return s_pg_oid_arr_type;
}

absl::StatusOr<zetasql::Value> CreatePgOidValue(uint32_t oid) {
  return zetasql::Value::Extended(
      GetPgOidType(), zetasql::ValueContent::Create((int64_t)oid));
}

absl::StatusOr<int64_t> GetPgOidValue(const zetasql::Value& value) {
  ZETASQL_RET_CHECK(!value.is_null());
  ZETASQL_RET_CHECK(value.type() == GetPgOidType());
  return value.extended_value().GetAs<int64_t>();
}

}  // namespace postgres_translator::spangres::datatypes
