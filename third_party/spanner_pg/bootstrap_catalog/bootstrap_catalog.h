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

#ifndef BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_H_
#define BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_H_

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog_info.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

// Makes the PostgreSQL Bootstrap Catalog system catalog objects (types,
// functions, etc.) available to Spanner PG, bypassing the need to startup a full
// PostgreSQL backend. PgBootstrapCatalog is entirely static and makes use of a
// global singleton to access it. Catalog data is scraped from pg_type.h,
// pg_cast.h, etc. files (similarly to standard PostgreSQL).
// Example usage:
//   Oid int4oid = PgBootstrapCatalog::Default()->GetTypeOid("int4");
class PgBootstrapCatalog {
 public:
  // Constructs a bootstrap catalog from arrays of raw catalog data
  PgBootstrapCatalog(
      absl::Span<const FormData_pg_collation> pg_collation_data,
      absl::Span<const FormData_pg_namespace> pg_namespace_data,
      absl::Span<const FormData_pg_type> pg_type_data,
      absl::Span<const FormData_pg_proc_WithArgTypes> pg_proc_data,
      absl::Span<const FormData_pg_cast> pg_cast_data,
      absl::Span<const FormData_pg_operator> pg_operator_data,
      absl::Span<const FormData_pg_aggregate> pg_aggregate_data,
      absl::Span<const FormData_pg_opclass> pg_opclass_data,
      absl::Span<const FormData_pg_am> pg_am_data,
      absl::Span<const FormData_pg_amop> pg_amop_data,
      absl::Span<const FormData_pg_amproc> pg_amproc_data);

  // The default catalog. Constructed using compiled-in data.
  static const PgBootstrapCatalog* Default();

  // Collation =================================================================
  // Given a collation name, return the corresponding pg_collation.
  absl::StatusOr<const FormData_pg_collation*> GetCollationByName(
      absl::string_view collation_name) const;
  // Given a collation name, return the corresponding oid.
  absl::StatusOr<Oid> GetCollationOid(absl::string_view collation_name) const;

  // Namespace =================================================================
  // Given an oid, returns the corresponding namespace name.
  absl::StatusOr<const char*> GetNamespaceName(Oid oid) const;

  // Given a namespace name, return the corresponding oid.
  absl::StatusOr<Oid> GetNamespaceOid(absl::string_view name) const;

  // Type ======================================================================
  // Given an oid, returns the corresponding pg_type.
  absl::StatusOr<const FormData_pg_type*> GetType(Oid oid) const;

  // Given a type oid, returns the corresponding internal catalog name.
  absl::StatusOr<const char*> GetTypeName(Oid oid) const;

  // Given a type oid, returns the formatted user-friendly type name.
  // Requires a MemoryContext in order to allocate the formatted name.
  // This version should be used for all error messages.
  absl::StatusOr<const char*> GetFormattedTypeName(Oid oid) const;

  // Given a list of type oids, returns a list of corresponding names.
  // Requires a MemoryContext in order to allocate the formatted names.
  absl::StatusOr<std::vector<const char*>> GetFormattedTypeNames(
      absl::Span<const Oid> type_oids) const;

  // Given a type name, returns the corresponding list of types.
  absl::StatusOr<absl::Span<const FormData_pg_type* const>> GetTypesByName(
      absl::string_view name) const;

  // Proc ======================================================================
  // Given an oid, returns the corresponding pg_proc.
  absl::StatusOr<const FormData_pg_proc*> GetProc(Oid oid) const;

  // Given an oid, returns the corresponding proc name.
  absl::StatusOr<const char*> GetProcName(Oid oid) const;

  absl::StatusOr<absl::Span<const FormData_pg_proc* const>> GetProcsByName(
      absl::string_view name) const;

  // Given a namespace name, proc name, and list of argument types, return the
  // corresponding Oid.
  absl::StatusOr<Oid> GetProcOid(absl::string_view namespace_name,
                                 absl::string_view proc_name,
                                 absl::Span<const Oid> argument_types) const;

  // Cast ======================================================================
  // Given a pair of oids, returns the corresponding pg_cast. There is at most
  // one cast struct for each pair of source/target types (this is a PostgreSQL
  // choice).
  absl::StatusOr<const FormData_pg_cast*> GetCast(Oid source_type_id,
                                                  Oid target_type_id) const;
  absl::StatusOr<const FormData_pg_cast*> GetCastByFunctionOid(
      Oid function_oid) const;

  // Operator ==================================================================
  // Given an oid, returns the corresponding pg_operator.
  absl::StatusOr<const FormData_pg_operator*> GetOperator(Oid oid) const;

  // Given an oprcode (a proc oid), returns a list of the corresponding operator
  // Oids. Note that this list is not promised to be sorted.
  absl::StatusOr<absl::Span<const Oid>> GetOperatorOidsByOprcode(
      Oid oprcode) const;

  // Given an operator name, returns a list of the corresponding Oids. Note that
  // this list is not promised to be sorted.
  absl::StatusOr<absl::Span<const Oid>> GetOperatorOids(
      absl::string_view name) const;

  // Given an operator name, oprleft, and oprright, function returns the
  // operator Oid that matches all three arguments.
  absl::StatusOr<Oid> GetOperatorOidByOprLeftRight(absl::string_view name,
                                                   Oid oprleft,
                                                   Oid oprright) const;

  // Aggregate =================================================================
  // Given an oid, returns the corresponding pg_aggregate.
  absl::StatusOr<const FormData_pg_aggregate*> GetAggregate(Oid oid) const;

  // OpClass ===================================================================
  // Given an oid, returns the corresponding pg_opclass.
  absl::StatusOr<const FormData_pg_opclass*> GetOpclass(Oid oid) const;

  // Given as Access Method (AM), returns a list of matching pg_opclasses.
  absl::StatusOr<absl::Span<const Oid>> GetOpclassesByAm(Oid am_id) const;

  // Access Method Operator ====================================================
  // Given an Operator Family, left and right argument types, and an Access
  // Method Strategy, returns the corresponding pg_amop.
  absl::StatusOr<const FormData_pg_amop*> GetAmopByFamily(Oid opfamily,
                                                          Oid lefttype,
                                                          Oid righttype,
                                                          int16_t strategy) const;

  // Given an Access Method Operator ID, returns a list of matching pg_amops.
  absl::StatusOr<absl::Span<const FormData_pg_amop* const>> GetAmopsByAmopOpId(
      Oid opid) const;

  // Access Method Procedure ===================================================
  // Given an Operator Family, left and right input data types, and a support
  // procedure index, returns the corresponding pg_amproc.
  absl::StatusOr<const FormData_pg_amproc*> GetAmprocByFamily(
      Oid opfamily, Oid lefttype, Oid righttype, int16_t index) const;

  // Given an Operator Family and an input data type, returns a list of matching
  // pg_amprocs.
  absl::StatusOr<absl::Span<const FormData_pg_amproc* const>>
  GetAmprocsByFamily(Oid opfamily, Oid lefttype) const;

 private:
  // CastKey is a key for looking up pg_cast structs keyed on a Source Oid and
  // Target Oid.
  struct CastKey {
    Oid source = InvalidOid;
    Oid target = InvalidOid;

    friend bool operator==(const CastKey& a, const CastKey& b) {
      return a.source == b.source && a.target == b.target;
    }

    template <typename H>
    friend H AbslHashValue(H h, const CastKey& k) {
      return H::combine(std::move(h), k.source, k.target);
    }
  };

  // AmopFamilyKey is a key for looking up pg_amop structs keyed by OpFamily,
  // left and right arguments, and strategy number.
  struct AmopFamilyKey {
    // Operator family id.
    Oid opfamily;
    // Type id of left argument.
    Oid lefttype;
    // Type id of right argument.
    Oid righttype = InvalidOid;
    // Strategy number (defined in stratnum.h).
    int16_t strategy;

    friend bool operator==(const AmopFamilyKey& a, const AmopFamilyKey& b) {
      return a.opfamily == b.opfamily && a.lefttype == b.lefttype &&
          a.righttype == b.righttype && a.strategy == b.strategy;
    }

    template <typename H>
    friend H AbslHashValue(H h, const AmopFamilyKey& k) {
      return H::combine(std::move(h), k.opfamily, k.lefttype, k.righttype,
                        k.strategy);
    }
  };

  // AmprocFamilyKey is a key for lookup up pg_amproc structs keyed by OpFamily,
  // left and right arguments, and support procedure index.
  struct AmprocFamilyKey {
    // Operator family id.
    Oid opfamily;
    // Type id of left input data.
    Oid lefttype;
    // Type id of right input data.
    Oid righttype = InvalidOid;
    // Support procedure index (See comment above BTORDER_PROC in nbtree.h).
    int16_t index;

    friend bool operator==(const AmprocFamilyKey& a, const AmprocFamilyKey& b) {
      return a.opfamily == b.opfamily && a.lefttype == b.lefttype &&
          a.righttype == b.righttype && a.index == b.index;
    }

    template <typename H>
    friend H AbslHashValue(H h, const AmprocFamilyKey& k) {
      return H::combine(std::move(h), k.opfamily, k.lefttype, k.righttype,
                        k.index);
    }
  };

  bool IsIntType(Oid type) {
    return type == INT2OID || type == INT4OID || type == INT8OID;
  }

  // Create a copy of original_proc with the updated arg types and return type.
  // Store the updated copy in updated_proc_by_oid_.
  void UpdateProc(const FormData_pg_proc_WithArgTypes& original_proc,
                  const std::vector<Oid>& updated_arg_types,
                  Oid updated_return_type);

  // Create a copy of original_operator with the updated arg types and return
  // type. Store the updated copy in updated_operator_by_oid_.
  void UpdateOperator(const FormData_pg_operator& original_operator,
                      const oidvector& updated_arg_types,
                      Oid updated_return_type);

  // Given a proc, return the final version of it. If the proc is unmodified,
  // return its raw pointer. If it's deleted, return a nullptr. If it is
  // modified, use `UpdateProc` to create a modified copy and return a raw
  // pointer to the modified copy.
  const FormData_pg_proc_WithArgTypes* GetFinalProcData(
      const FormData_pg_proc_WithArgTypes& original_proc);

  // Given an operator, return the final version of it. If the underlying proc
  // is unmodified, return the raw pointer of the original operator. If the
  // underlying proc is deleted, return a nullptr. If the underlying proc is
  // modified, use `UpdateOperator` to create a modified copy of the operator
  // and return a raw pointer to the modified copy.
  const FormData_pg_operator* GetFinalOperatorData(
      const FormData_pg_operator& original_operator);

  absl::flat_hash_map<std::string, const FormData_pg_collation*>
      collation_by_name_;
  absl::flat_hash_map<Oid, const FormData_pg_namespace*> namespace_by_oid_;
  absl::flat_hash_map<std::string, Oid> namespace_name_to_oid_;
  absl::flat_hash_map<Oid, const FormData_pg_type*> type_by_oid_;
  absl::flat_hash_map<std::string, std::vector<const FormData_pg_type*>>
      type_by_name_;
  absl::flat_hash_map<Oid, const FormData_pg_proc*> proc_by_oid_;
  absl::flat_hash_map<std::string, std::vector<const FormData_pg_proc*>>
      proc_by_name_;
  absl::flat_hash_map<Oid, std::vector<const FormData_pg_amop*>>
      amop_by_oproid_;
  absl::flat_hash_map<CastKey, const FormData_pg_cast*> cast_by_castkey_;
  absl::flat_hash_map<Oid, const FormData_pg_cast*> cast_by_castfunc_;
  // The operator maps do not include operators with two different integer type
  // inputs.
  absl::flat_hash_map<Oid, const FormData_pg_operator*> operator_by_oid_;
  absl::flat_hash_map<Oid, std::vector<Oid>> operator_oprcode_to_oids_;
  absl::flat_hash_map<std::string, std::vector<Oid>> operator_name_to_oids_;
  absl::flat_hash_map<Oid, const FormData_pg_aggregate*> aggregate_by_oid_;
  absl::flat_hash_map<Oid, const FormData_pg_opclass*> opclass_by_oid_;
  absl::flat_hash_map<Oid, std::vector<Oid>> opclass_am_to_opclasses_;
  absl::flat_hash_map<AmopFamilyKey, const FormData_pg_amop*>
      amop_by_familykey_;
  absl::flat_hash_map<AmprocFamilyKey, const FormData_pg_amproc*>
      amproc_by_familykey_;
  // TODO: Refactor code to only use `amproc_by_familykey_`.
  absl::flat_hash_map<AmprocFamilyKey, std::vector<const FormData_pg_amproc*>>
      amprocs_by_partial_familykey_;

  // The updated FormData_pg_proc_WithArgTypes and FormData_pg_operator
  // objects are owned by the bootstrap catalog.
  absl::flat_hash_map<Oid, std::unique_ptr<FormData_pg_proc_WithArgTypes>>
      updated_proc_by_oid_;
  absl::flat_hash_map<Oid, std::unique_ptr<FormData_pg_operator>>
      updated_operator_by_oid_;
};

}  // namespace postgres_translator

#endif  // BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_H_
