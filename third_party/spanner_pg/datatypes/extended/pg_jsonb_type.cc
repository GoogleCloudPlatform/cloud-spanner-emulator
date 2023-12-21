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

#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value_content.h"
#include "absl/flags/flag.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_parse.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "zetasql/base/compact_reference_counted.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres {
namespace datatypes {

using ExtendedType = ::zetasql::ExtendedType;
using LanguageOptions = ::zetasql::LanguageOptions;
using ProductMode = ::zetasql::ProductMode;
using Type = ::zetasql::Type;
using TypeParameters = ::zetasql::TypeParameters;
using TypeParameterValue = ::zetasql::TypeParameterValue;
using TypeProto = ::zetasql::TypeProto;
using ValueContent = ::zetasql::ValueContent;
using ValueProto = ::zetasql::ValueProto;
using postgres_translator::spangres::datatypes::common::jsonb::ParseJson;
using TypeAnnotationCode = ::google::spanner::v1::TypeAnnotationCode;

using absl::StrAppend;
using absl::StrCat;
using zetasql_base::refcount::CompactReferenceCounted;

// PgJsonbRef is ref counted wrapper around an normalized pg_jsonb value
class PgJsonbRef final : public CompactReferenceCounted<PgJsonbRef> {
 public:
  PgJsonbRef() {}
  explicit PgJsonbRef(absl::Cord normalized)
      : normalized_(std::move(normalized)) {}

  PgJsonbRef(const PgJsonbRef&) = delete;
  PgJsonbRef& operator=(const PgJsonbRef&) = delete;

  const absl::Cord& value() const { return normalized_; }

  uint64_t physical_byte_size() const {
    return sizeof(PgJsonbRef) + normalized_.size() * sizeof(char);
  }

 private:
  const absl::Cord normalized_;
};

class PgJsonbType : public SpannerExtendedType {
 public:
  PgJsonbType() : SpannerExtendedType(TypeAnnotationCode::PG_JSONB) {}

  bool SupportsOrdering(const LanguageOptions& unused,
                        std::string* unused_type_description) const override {
    return false;
  }

  bool SupportsEquality() const override { return false; }

  bool SupportsEquality(const LanguageOptions& unused) const override {
    return SupportsEquality();
  }

  std::string ShortTypeName(ProductMode unused) const override {
    return "PG.JSONB";
  }

  std::string TypeName(ProductMode unused) const override {
    return ShortTypeName(unused);
  }

  absl::StatusOr<TypeParameters> ValidateAndResolveTypeParameters(
      const std::vector<TypeParameterValue>& type_parameter_values,
      ProductMode mode) const override {
    return absl::InvalidArgumentError(StrCat(
        ShortTypeName(/*unused=*/mode), " does not support type parameters"));
  }

  absl::Status ValidateResolvedTypeParameters(
      const TypeParameters& type_parameters, ProductMode mode) const override {
    return absl::InvalidArgumentError(StrCat(
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
  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override {
    if (no_partitioning_type != nullptr) {
      *no_partitioning_type = this;
    }
    return false;
  }

  bool EqualsForSameKind(const Type* that, bool equivalent) const override {
    return that == this;
  }

  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override {
    StrAppend(debug_string,
              ShortTypeName(/*unused=*/zetasql::PRODUCT_EXTERNAL));
  }

  void CopyValueContent(const ValueContent& from,
                        ValueContent* to) const override {
    from.GetAs<CompactReferenceCounted<PgJsonbRef>*>()->Ref();
    *to = from;
  }

  void ClearValueContent(const ValueContent& value) const override {
    value.GetAs<CompactReferenceCounted<PgJsonbRef>*>()->Unref();
  }

  bool ValueContentEquals(
      const ValueContent& x, const ValueContent& y,
      const zetasql::ValueEqualityCheckOptions& options) const override {
    return x.GetAs<PgJsonbRef*>()->value().Compare(
               y.GetAs<PgJsonbRef*>()->value()) == 0;
  }

  bool ValueContentLess(const ValueContent& x, const ValueContent& y,
                        const Type* other_type) const override {
    ABSL_LOG(FATAL) << ShortTypeName(/*unused=*/zetasql::PRODUCT_EXTERNAL)
               << " does not support comparisons.";
    return false;
  }

  uint64_t GetValueContentExternallyAllocatedByteSize(
      const ValueContent& value) const override {
    return value.GetAs<PgJsonbRef*>()->physical_byte_size();
  }

  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const override {
    return absl::HashState::combine(std::move(state),
                                    value.GetAs<PgJsonbRef*>()->value());
  }

  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override {
    const std::string normalized_jsonb =
        std::string(value.GetAs<PgJsonbRef*>()->value());
    if (options.mode == FormatValueContentOptions::Mode::kSQLLiteral ||
        options.mode == FormatValueContentOptions::Mode::kSQLExpression) {
      return StrCat("CAST(",
                    zetasql::ToSingleQuotedStringLiteral(normalized_jsonb),
                    " AS PG.JSONB)");
    }
    return normalized_jsonb;
  }

  absl::Status SerializeValueContent(const ValueContent& value,
                                     ValueProto* value_proto) const override {
    return absl::InvalidArgumentError(
        StrCat(ShortTypeName(/*unused=*/zetasql::PRODUCT_EXTERNAL),
               " does not support serializing value content."));
  }

  absl::Status DeserializeValueContent(const ValueProto& value_proto,
                                       ValueContent* value) const override {
    return absl::InvalidArgumentError(
        StrCat(ShortTypeName(/*unused=*/zetasql::PRODUCT_EXTERNAL),
               " does not support deserializing value content."));
  }
};

const SpannerExtendedType* GetPgJsonbType() {
  static PgJsonbType* s_pg_jsonb_type = new PgJsonbType();
  return s_pg_jsonb_type;
}

const zetasql::ArrayType* GetPgJsonbArrayType() {
  static const zetasql::ArrayType* s_pg_jsonb_arr_type = []() {
    const zetasql::ArrayType* pg_jsonb_array_type = nullptr;
    ABSL_CHECK_OK(GetTypeFactory()->MakeArrayType(GetPgJsonbType(),
                                             &pg_jsonb_array_type));
    return pg_jsonb_array_type;
  }();
  return s_pg_jsonb_arr_type;
}

absl::StatusOr<zetasql::Value> CreatePgJsonbValue(
    absl::string_view denormalized_jsonb) {
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, ParseJson(denormalized_jsonb));
  return CreatePgJsonbValueFromNormalized(jsonb);
}

absl::StatusOr<zetasql::Value> CreatePgJsonbValueWithMemoryContext(
    absl::string_view jsonb_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return spangres::datatypes::CreatePgJsonbValue(jsonb_string);
}

zetasql::Value CreatePgJsonbValueFromNormalized(
    const absl::Cord& normalized_jsonb) {
  return zetasql::Value::Extended(
      GetPgJsonbType(),
      zetasql::ValueContent::Create(new PgJsonbRef(normalized_jsonb)));
}

absl::StatusOr<absl::Cord> GetPgJsonbNormalizedValue(
    const zetasql::Value& value) {
  ZETASQL_RET_CHECK(!value.is_null());
  ZETASQL_RET_CHECK(value.type() == GetPgJsonbType());
  return value.extended_value().GetAs<PgJsonbRef*>()->value();
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres
