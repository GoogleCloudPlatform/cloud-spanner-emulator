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

#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/value_content.h"
#include "absl/flags/flag.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/common/pg_numeric_parse.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/compact_reference_counted.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::datatypes {

using LanguageOptions = ::zetasql::LanguageOptions;
using ProductMode = ::zetasql::ProductMode;
using TypeModifiers = ::zetasql::TypeModifiers;
using TypeParameters = ::zetasql::TypeParameters;
using TypeParameterValue = ::zetasql::TypeParameterValue;
using TypeProto = ::zetasql::TypeProto;
using ValueContent = ::zetasql::ValueContent;
using ValueProto = ::zetasql::ValueProto;
using TypeAnnotationCode = ::google::spanner::v1::TypeAnnotationCode;

using zetasql_base::refcount::CompactReferenceCounted;

// PgNumericRef is ref counted wrapper around an encoded pg_numeric value.
class PgNumericRef final : public CompactReferenceCounted<PgNumericRef> {
 public:
  PgNumericRef() {}
  explicit PgNumericRef(absl::Cord normalized)
      : normalized_(std::move(normalized)) {}

  PgNumericRef(const PgNumericRef&) = delete;
  PgNumericRef& operator=(const PgNumericRef&) = delete;

  const absl::Cord& value() const { return normalized_; }

  uint64_t physical_byte_size() const {
    uint64_t temp = sizeof(PgNumericRef) + normalized_.size() * sizeof(char);
    return temp;
  }

 private:
  const absl::Cord normalized_;
};

class PgNumericType : public SpannerExtendedType {
 public:
  PgNumericType() : SpannerExtendedType(TypeAnnotationCode::PG_NUMERIC) {}

  bool SupportsOrdering(const LanguageOptions& unused,
                        std::string* unused_type_description) const override {
    return true;
  }

  bool SupportsEquality() const override { return true; }

  bool SupportsEquality(const LanguageOptions& unused) const override {
    return SupportsEquality();
  }

  std::string ShortTypeName(ProductMode unused) const override {
    return "PG.NUMERIC";
  }

  std::string TypeName(ProductMode unused) const override {
    return ShortTypeName(unused);
  }

  absl::StatusOr<std::string> TypeNameWithModifiers(
      const TypeModifiers& type_modifiers, ProductMode mode) const override {
    ZETASQL_RET_CHECK(type_modifiers.collation().Empty());
    if (type_modifiers.type_parameters().IsEmpty()) {
      return ShortTypeName(/*unused=*/mode);
    }
    ZETASQL_RET_CHECK_OK(ValidateResolvedTypeParameters(
        type_modifiers.type_parameters(), /*mode=*/mode));
    return absl::StrCat(ShortTypeName(mode), "(",
                        type_modifiers.type_parameters()
                            .extended_type_parameters()
                            .parameter(0)
                            .int64_value(),
                        ",",
                        type_modifiers.type_parameters()
                            .extended_type_parameters()
                            .parameter(1)
                            .int64_value(),
                        ")");
  }

  absl::StatusOr<TypeParameters> ValidateAndResolveTypeParameters(
      const std::vector<TypeParameterValue>& type_parameter_values,
      ProductMode mode) const override {
    if (type_parameter_values.size() == 2 &&
        type_parameter_values.at(0).GetValue().has_int64_value() &&
        type_parameter_values.at(1).GetValue().has_int64_value()) {
      return zetasql::TypeParameters::MakeExtendedTypeParameters(
          zetasql::ExtendedTypeParameters(
              {type_parameter_values.at(0).GetValue(),
               type_parameter_values.at(1).GetValue()}));
    }
    return absl::InvalidArgumentError(absl::StrCat(
        ShortTypeName(/*unused=*/mode),
        " only accepts precision and scale type parameters in the form "
        "of 2 INT64 arguments."));
  }

  absl::Status ValidateResolvedTypeParameters(
      const TypeParameters& type_parameters, ProductMode mode) const override {
    ZETASQL_RET_CHECK(type_parameters.IsExtendedTypeParameters())
        << type_parameters.DebugString();
    const zetasql::ExtendedTypeParameters& params =
        type_parameters.extended_type_parameters();
    ZETASQL_RET_CHECK_EQ(params.num_parameters(), 2);
    ZETASQL_RET_CHECK(params.parameter(0).has_int64_value());
    ZETASQL_RET_CHECK(params.parameter(1).has_int64_value());
    return absl::OkStatus();
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
      *no_partitioning_type = nullptr;
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

  void CopyValueContent(const ValueContent& from,
                        ValueContent* to) const override {
    from.GetAs<CompactReferenceCounted<PgNumericRef>*>()->Ref();
    *to = from;
  }

  void ClearValueContent(const ValueContent& value) const override {
    value.GetAs<CompactReferenceCounted<PgNumericRef>*>()->Unref();
  }

  absl::StatusOr<int32_t> CollatedCompare(
      const absl::Cord& lhs_normalized,
      const absl::Cord& rhs_normalized) const {
    // This implementation calls PG `numeric_cmp` to compare the input numerics.

    // Convert absl::Cord to std::string
    std::string lhs_normalized_str;
    lhs_normalized_str.reserve(lhs_normalized.size());
    absl::CopyCordToString(lhs_normalized, &lhs_normalized_str);
    std::string rhs_normalized_str;
    rhs_normalized_str.reserve(rhs_normalized.size());
    absl::CopyCordToString(rhs_normalized, &rhs_normalized_str);

    // Create numeric datums from `lhs_normalized_str` and `rhs_normalized_str`
    // by indirectly calling PG function `numeric_in`:
    // - lhs_normalized_str/rhs_normalized_str: numeric datum input
    // - InvalidOid: unused argument
    // - Int32GetDatum(-1): typmod -1 (unlimited/unknown), i.e. numeric without
    // typmod
    ZETASQL_ASSIGN_OR_RETURN(
        Datum lhs_numeric,
        postgres_translator::CheckedOidFunctionCall3(
            F_NUMERIC_IN, CStringGetDatum(lhs_normalized_str.c_str()),
            ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
    ZETASQL_ASSIGN_OR_RETURN(
        Datum rhs_numeric,
        postgres_translator::CheckedOidFunctionCall3(
            F_NUMERIC_IN, CStringGetDatum(rhs_normalized_str.c_str()),
            ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));

    // Compare `lhs_numeric` and `rhs_numeric` datums by indirectly calling PG
    // function `numeric_cmp`
    // - lhs_numeric: lhs numeric datum
    // - rhs_numeric: rhs numeric datum
    // - numeric_cmp_result: comparison result datum
    ZETASQL_ASSIGN_OR_RETURN(Datum numeric_cmp_result,
                     postgres_translator::CheckedOidFunctionCall2(
                         F_NUMERIC_CMP, lhs_numeric, rhs_numeric));
    return DatumGetInt32(numeric_cmp_result);
  }

  bool ValueContentEquals(
      const ValueContent& x, const ValueContent& y,
      const zetasql::ValueEqualityCheckOptions& options) const override {
    absl::StatusOr<int32_t> comparison_result = CollatedCompare(
        x.GetAs<PgNumericRef*>()->value(), y.GetAs<PgNumericRef*>()->value());
    if (!comparison_result.ok()) {
      ABSL_LOG(FATAL) << "PG.NUMERIC Comparison failed. Status: "
                 << comparison_result.status();
      return false;
    }
    return comparison_result.value() == 0;
  }

  bool ValueContentLess(const ValueContent& x, const ValueContent& y,
                        const Type* other_type) const override {
    absl::StatusOr<int32_t> comparison_result = CollatedCompare(
        x.GetAs<PgNumericRef*>()->value(), y.GetAs<PgNumericRef*>()->value());
    if (!comparison_result.ok()) {
      ABSL_LOG(FATAL) << "PG.NUMERIC Comparison failed. Status: "
                 << comparison_result.status();
      return false;
    }
    return comparison_result.value() < 0;
  }

  uint64_t GetValueContentExternallyAllocatedByteSize(
      const ValueContent& value) const override {
    return value.GetAs<PgNumericRef*>()->physical_byte_size();
  }

  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const override {
    return absl::HashState::combine(std::move(state),
                                    value.GetAs<PgNumericRef*>()->value());
  }

  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override {
    std::string normalized;
    normalized.reserve(value.GetAs<PgNumericRef*>()->value().size());
    absl::CopyCordToString(value.GetAs<PgNumericRef*>()->value(), &normalized);
    if (options.mode == FormatValueContentOptions::Mode::kSQLLiteral ||
        options.mode == FormatValueContentOptions::Mode::kSQLExpression) {
      return absl::StrCat("pg.cast_to_numeric('", normalized, "')");
    }

    return normalized;
  }

  absl::Status SerializeValueContent(const ValueContent& value,
                                     ValueProto* value_proto) const override {
    return absl::InvalidArgumentError(
        absl::StrCat(ShortTypeName(/*unused=*/zetasql::PRODUCT_EXTERNAL),
                     " does not support serializing value content."));
  }

  absl::Status DeserializeValueContent(const ValueProto& value_proto,
                                       ValueContent* value) const override {
    return absl::InvalidArgumentError(
        absl::StrCat(ShortTypeName(/*unused=*/zetasql::PRODUCT_EXTERNAL),
                     " does not support deserializing value content."));
  }
};

const SpannerExtendedType* GetPgNumericType() {
  static PgNumericType* s_pg_numeric_type = new PgNumericType();
  return s_pg_numeric_type;
}

const zetasql::ArrayType* GetPgNumericArrayType() {
  static const zetasql::ArrayType* s_pg_numeric_arr_type = []() {
    const zetasql::ArrayType* pg_numeric_array_type = nullptr;
    ABSL_CHECK_OK(GetTypeFactory()->MakeArrayType(GetPgNumericType(),
                                             &pg_numeric_array_type));
    return pg_numeric_array_type;
  }();
  return s_pg_numeric_arr_type;
}

absl::StatusOr<zetasql::Value> CreatePgNumericValue(
    absl::string_view readable_numeric) {
  ZETASQL_ASSIGN_OR_RETURN(std::string normalized_numeric,
                   common::NormalizePgNumeric(readable_numeric));
  absl::Cord normalized(normalized_numeric);
  return zetasql::Value::Extended(
      GetPgNumericType(),
      zetasql::ValueContent::Create(new PgNumericRef(normalized)));
}

absl::StatusOr<zetasql::Value> CreatePgNumericValueWithMemoryContext(
    absl::string_view numeric_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return CreatePgNumericValue(numeric_string);
}

absl::StatusOr<zetasql::Value> CreatePgNumericValueWithPrecisionAndScale(
    absl::string_view readable_numeric, int64_t precision, int64_t scale) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::string normalized_numeric,
      common::NormalizePgNumeric(readable_numeric, precision, scale));
  absl::Cord normalized(normalized_numeric);
  return zetasql::Value::Extended(
      GetPgNumericType(),
      zetasql::ValueContent::Create(new PgNumericRef(normalized)));
}

absl::StatusOr<absl::Cord> GetPgNumericNormalizedValue(
    const zetasql::Value& value) {
  ZETASQL_RET_CHECK(!value.is_null());
  ZETASQL_RET_CHECK(value.type() == GetPgNumericType());
  return value.extended_value().GetAs<PgNumericRef*>()->value();
}

}  // namespace postgres_translator::spangres::datatypes
