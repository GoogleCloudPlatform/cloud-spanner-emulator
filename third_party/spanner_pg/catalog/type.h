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

#ifndef CATALOG_TYPE_H_
#define CATALOG_TYPE_H_

#include <string>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

class PostgresTypeMapping : public zetasql::ExtendedType {
 public:
  explicit PostgresTypeMapping(const zetasql::TypeFactory* factory,
                               const Oid oid);
  ~PostgresTypeMapping() {}
  PostgresTypeMapping(const PostgresTypeMapping&) = delete;
  PostgresTypeMapping& operator=(const PostgresTypeMapping&) = delete;

  // Returns the corresponding mapped builtin type. Returns nullptr if
  // no corresponding mapping exists.
  virtual const zetasql::Type* mapped_type() const = 0;

  virtual const Oid PostgresTypeOid() const { return postgres_type_oid_; }

  // Returns the raw type name with the PostgreSQL prefix to easily identify
  // the type as a PostgreSQL type.
  // Does not use the product mode.
  std::string TypeName(zetasql::ProductMode mode) const override;

  absl::StatusOr<const char*> PostgresExternalTypeName() const;

  // Returns the raw type name without the PostgreSQL prefix.
  absl::string_view raw_type_name() const;

  bool IsSupportedType(
      const zetasql::LanguageOptions& language_options) const override;
  absl::HashState HashTypeParameter(absl::HashState state) const override;
  absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorSetMapOptions& options,
      zetasql::TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const override;
  int64_t GetEstimatedOwnedMemoryBytesSize() const override;

  // Constructs a zetasql::Value of this type given a valid
  // PostgreSQL const. Used when transforming PostgreSQL
  // constants to zetasql::ResolvedLiterals. The transformer
  // will look up the appropriate `PostgresTypeMapping` using
  // the EngineSystemCatalog and call this method to construct
  // the value.
  virtual absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const;

  // Constructs an appropriately-typed PostgreSQL constant given an input
  // zetasql::Value. Used when transforming zetasql::ResolvedLiterals to
  // PostgreSQL constants. The transformer will look up the appropriate
  // `PostgresTypeMapping`using the EngineSystemCatalog and call this method to
  // construct the value.
  virtual absl::StatusOr<Const*> MakePgConst(const zetasql::Value& val) const;

 private:
  // Initializes the TypeName using default postgres bootstrap catalog.
  std::string DefaultInitializeTypeName(const Oid oid) const;

  bool EqualsForSameKind(const Type* that, bool equivalent) const override;
  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override;
  bool ValueContentEquals(
      const zetasql::ValueContent& x, const zetasql::ValueContent& y,
      const zetasql::ValueEqualityCheckOptions& options) const override;
  bool ValueContentLess(const zetasql::ValueContent& x,
                        const zetasql::ValueContent& y,
                        const Type* other_type) const override;
  absl::HashState HashValueContent(const zetasql::ValueContent& value,
                                   absl::HashState state) const override;
  std::string FormatValueContent(
      const zetasql::ValueContent& value,
      const FormatValueContentOptions& options) const override;
  absl::Status SerializeValueContent(
      const zetasql::ValueContent& value,
      zetasql::ValueProto* value_proto) const override;
  absl::Status DeserializeValueContent(
      const zetasql::ValueProto& value_proto,
      zetasql::ValueContent* value) const override;

  const Oid postgres_type_oid_;
  const std::string raw_type_name_;
};

// Array helper class handles type-generic transformations.
class PostgresExtendedArrayMapping : public PostgresTypeMapping {
 public:
  PostgresExtendedArrayMapping(const zetasql::TypeFactory* type_factory,
                               Oid array_type_oid,
                               const PostgresTypeMapping* element_type,
                               const zetasql::Type* mapped_type,
                               bool requires_nan_handling)
      : PostgresTypeMapping(type_factory, array_type_oid),
        element_type_(element_type),
        mapped_type_(mapped_type),
        requires_nan_handling_(requires_nan_handling) {}

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override;

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override;

  const zetasql::Type* mapped_type() const override { return mapped_type_; }

 private:
  const PostgresTypeMapping* element_type_;
  const zetasql::Type* mapped_type_;
  const bool requires_nan_handling_;
};

// Returns a global static instance of a TypeFactory used to construct types in
// Spangres.
zetasql::TypeFactory* GetTypeFactory();

namespace types {

// Supported Scalar Types.
const PostgresTypeMapping* PgBoolMapping();
const PostgresTypeMapping* PgInt8Mapping();
const PostgresTypeMapping* PgFloat8Mapping();
const PostgresTypeMapping* PgVarcharMapping();
const PostgresTypeMapping* PgTextMapping();
const PostgresTypeMapping* PgByteaMapping();
const PostgresTypeMapping* PgTimestamptzMapping();
const PostgresTypeMapping* PgDateMapping();

// Supported Array Types.
const PostgresTypeMapping* PgBoolArrayMapping();
const PostgresTypeMapping* PgInt8ArrayMapping();
const PostgresTypeMapping* PgFloat8ArrayMapping();
const PostgresTypeMapping* PgVarcharArrayMapping();
const PostgresTypeMapping* PgTextArrayMapping();
const PostgresTypeMapping* PgByteaArrayMapping();
const PostgresTypeMapping* PgTimestamptzArrayMapping();
const PostgresTypeMapping* PgDateArrayMapping();

}  // namespace types

}  // namespace postgres_translator

#endif  // CATALOG_TYPE_H_
