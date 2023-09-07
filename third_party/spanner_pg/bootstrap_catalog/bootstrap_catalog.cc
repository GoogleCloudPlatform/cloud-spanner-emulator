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

#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog_info.h"
#include "third_party/spanner_pg/bootstrap_catalog/proc_changelist.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

// Ensure that our bootstrap Oids fit within the BootstrapObjectId range.
static_assert(kHighestAssignedBootstrapOid < FirstNormalObjectId);

PgBootstrapCatalog::PgBootstrapCatalog(
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
    absl::Span<const FormData_pg_amproc> pg_amproc_data) {
  collation_name_to_oid_.reserve(pg_collation_data.size());
  for (const FormData_pg_collation& data : pg_collation_data) {
    collation_name_to_oid_[NameStr(data.collname)] = data.oid;
  }

  namespace_by_oid_.reserve(pg_namespace_data.size());
  namespace_name_to_oid_.reserve(pg_namespace_data.size());
  for (const FormData_pg_namespace& data : pg_namespace_data) {
    namespace_by_oid_[data.oid] = &data;
    namespace_name_to_oid_[NameStr(data.nspname)] = data.oid;
  }

  type_by_oid_.reserve(pg_type_data.size());
  type_by_name_.reserve(pg_type_data.size());
  for (const FormData_pg_type& data : pg_type_data) {
    type_by_oid_[data.oid] = &data;
    type_by_name_[NameStr(data.typname)].push_back(&data);
  }

  proc_by_oid_.reserve(pg_proc_data.size());
  // This proc_by_name_ resize is slightly larger than necessary since
  // there are duplicate names, but duplicate names are <10% of all entries.
  proc_by_name_.reserve(pg_proc_data.size());
  for (const FormData_pg_proc_WithArgTypes& data : pg_proc_data) {
    // Get the updated (as needed) proc data.
    const FormData_pg_proc_WithArgTypes* final_proc_data =
        GetFinalProcData(data);
    if (final_proc_data == nullptr) {
      // The proc is deleted from the BootstrapCatalog.
      continue;
    }
    proc_by_oid_[final_proc_data->data.oid] = &final_proc_data->data;
    proc_by_name_[NameStr(final_proc_data->data.proname)].push_back(
        &final_proc_data->data);
  }
  cast_by_castkey_.reserve(pg_cast_data.size());
  for (const FormData_pg_cast& data : pg_cast_data) {
    if (data.castfunc != InvalidOid &&
        proc_by_oid_.find(data.castfunc) == proc_by_oid_.end()) {
      // The underlying proc is deleted from the BootstrapCatalog. Delete the
      // cast as well.
      continue;
    }
    cast_by_castkey_[{.source = data.castsource, .target = data.casttarget}] =
        &data;
    if (data.castfunc != 0) {
      cast_by_castfunc_[data.castfunc] = &data;
    }
  }
  operator_by_oid_.reserve(pg_operator_data.size());
  // This oprcode_to_operator_oids resize is slightly larger than necessary
  // since there are some overloaded operators and some will be filtered out.
  // However, the duplicate and pruned func ids are < 10% of all entries.
  operator_oprcode_to_oids_.reserve(pg_operator_data.size());
  for (const FormData_pg_operator& data : pg_operator_data) {
    // Get the updated (as needed) operator data.
    const FormData_pg_operator* final_operator_data =
        GetFinalOperatorData(data);
    if (final_operator_data == nullptr) {
      // The underlying proc is deleted from the BootstrapCatalog. Delete the
      // operator as well.
      continue;
    }
    operator_by_oid_[final_operator_data->oid] = final_operator_data;
    operator_name_to_oids_[NameStr(final_operator_data->oprname)].push_back(
        final_operator_data->oid);
    operator_oprcode_to_oids_[final_operator_data->oprcode].push_back(
        final_operator_data->oid);
  }
  aggregate_by_oid_.reserve(pg_operator_data.size());
  for (const FormData_pg_aggregate& data : pg_aggregate_data) {
    // Aggregate rows don't have their own Oids. We'll use the pg_proc Oid as a
    // substitute. Ensure there aren't duplicates.
    if (!aggregate_by_oid_.insert({data.aggfnoid, &data}).second) {
      ABSL_LOG(ERROR) << "Found duplicate aggregate oid in map for oid "
                 << data.aggfnoid;
    }
  }
  opclass_by_oid_.reserve(pg_opclass_data.size());
  // Access Methods (the key here) come from pg_am.
  opclass_am_to_opclasses_.reserve(pg_am_data.size());
  for (const FormData_pg_opclass& data : pg_opclass_data) {
    opclass_by_oid_[data.oid] = &data;
    // Opclass rows don't have their own Oids. We're not going to index them
    // individually by Oid since at this time noone wants to consume them that
    // way anyway. Group them by their Access Methods.
    opclass_am_to_opclasses_[data.opcmethod].push_back(data.oid);
  }
  amop_by_oproid_.reserve(pg_amop_data.size());
  amop_by_familykey_.reserve(pg_amop_data.size());
  for (const FormData_pg_amop& data : pg_amop_data) {
    amop_by_familykey_[{.opfamily = data.amopfamily,
                        .lefttype = data.amoplefttype,
                        .righttype = data.amoprighttype,
                        .strategy = data.amopstrategy}] = &data;
    amop_by_oproid_[data.amopopr].push_back(&data);
  }
  amproc_by_familykey_.reserve(pg_amproc_data.size());
  amprocs_by_partial_familykey_.reserve(pg_amproc_data.size());
  for (const FormData_pg_amproc& data : pg_amproc_data) {
    amproc_by_familykey_[{.opfamily = data.amprocfamily,
                          .lefttype = data.amproclefttype,
                          .righttype = data.amprocrighttype,
                          .index = data.amprocnum}] = &data;
    amprocs_by_partial_familykey_[{.opfamily = data.amprocfamily,
                           .lefttype = data.amproclefttype}]
        .push_back(&data);
  }
}

const PgBootstrapCatalog* PgBootstrapCatalog::Default() {
  static PgBootstrapCatalog default_catalog(
      {pg_collation_data, pg_collation_data_size},
      {pg_namespace_data, pg_namespace_data_size},
      {pg_type_data, pg_type_data_size}, {pg_proc_data, pg_proc_data_size},
      {pg_cast_data, pg_cast_data_size},
      {pg_operator_data, pg_operator_data_size},
      {pg_aggregate_data, pg_aggregate_data_size},
      {pg_opclass_data, pg_opclass_data_size}, {pg_am_data, pg_am_data_size},
      {pg_amop_data, pg_amop_data_size}, {pg_amproc_data, pg_amproc_data_size});
  return &default_catalog;
}

absl::StatusOr<Oid> PgBootstrapCatalog::GetCollationOid(
    absl::string_view collation_name) const {
  auto it = collation_name_to_oid_.find(collation_name);
  if (it == collation_name_to_oid_.end()) {
    return absl::NotFoundError(absl::StrCat("Collation ", collation_name,
                                            " is not found or not supported"));
  }
  return it->second;
}

absl::StatusOr<const char*> PgBootstrapCatalog::GetNamespaceName(
    Oid oid) const {
  auto it = namespace_by_oid_.find(oid);
  if (it == namespace_by_oid_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Namespace with oid ", oid, " not found"));
  }
  return NameStr(it->second->nspname);
}

absl::StatusOr<Oid> PgBootstrapCatalog::GetNamespaceOid(
    absl::string_view name) const {
  auto it = namespace_name_to_oid_.find(name);
  if (it == namespace_name_to_oid_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Namespace with name ", name, " not found"));
  }
  return it->second;
}

absl::StatusOr<const FormData_pg_type*> PgBootstrapCatalog::GetType(
    Oid oid) const {
  auto it = type_by_oid_.find(oid);
  if (it == type_by_oid_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Type with oid ", oid, " not found"));
  }
  return it->second;
}

absl::StatusOr<const char*> PgBootstrapCatalog::GetTypeName(Oid oid) const {
  ZETASQL_ASSIGN_OR_RETURN(const FormData_pg_type* pg_type, GetType(oid));
  return NameStr(pg_type->typname);
}

absl::StatusOr<const char*> PgBootstrapCatalog::GetFormattedTypeName(
    Oid oid) const {
  return CheckedPgFormatTypeBe(oid);
}

absl::StatusOr<std::vector<const char*>>
PgBootstrapCatalog::GetFormattedTypeNames(
    absl::Span<const Oid> type_oids) const {
  std::vector<const char*> type_names;
  for (const Oid oid : type_oids) {
    ZETASQL_ASSIGN_OR_RETURN(const char* type_name, GetFormattedTypeName(oid));
    type_names.push_back(type_name);
  }
  return type_names;
}

absl::StatusOr<absl::Span<const FormData_pg_type* const>>
PgBootstrapCatalog::GetTypesByName(absl::string_view name) const {
  auto it = type_by_name_.find(name);
  if (it == type_by_name_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Type with name ", name, " not found"));
  }
  return it->second;
}

absl::StatusOr<const FormData_pg_proc*> PgBootstrapCatalog::GetProc(
    Oid oid) const {
  auto it = proc_by_oid_.find(oid);
  if (it == proc_by_oid_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Procedure with oid ", oid, " not found"));
  }
  return it->second;
}

absl::StatusOr<const char*> PgBootstrapCatalog::GetProcName(Oid oid) const {
  ZETASQL_ASSIGN_OR_RETURN(const FormData_pg_proc* pg_proc, GetProc(oid));
  return NameStr(pg_proc->proname);
}

absl::StatusOr<absl::Span<const FormData_pg_proc* const>>
PgBootstrapCatalog::GetProcsByName(absl::string_view name) const {
  auto it = proc_by_name_.find(name);
  if (it == proc_by_name_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Procedure with name ", name, " not found"));
  }
  return it->second;
}

absl::StatusOr<Oid> PgBootstrapCatalog::GetProcOid(
    absl::string_view namespace_name, absl::string_view proc_name,
    absl::Span<const Oid> argument_types) const {
  ZETASQL_ASSIGN_OR_RETURN(Oid namespace_oid, GetNamespaceOid(namespace_name));

  ZETASQL_ASSIGN_OR_RETURN(absl::Span<const FormData_pg_proc* const> proc_data,
                   GetProcsByName(proc_name));
  for (const FormData_pg_proc* proc : proc_data) {
    if (proc->pronamespace != namespace_oid) {
      continue;
    }

    if (proc->proargtypes.vl_len_ != argument_types.size()) {
      continue;
    }
    bool arguments_match = true;
    for (int i = 0; i < proc->proargtypes.vl_len_; ++i) {
      if (proc->proargtypes.values[i] != argument_types[i]) {
        arguments_match = false;
        break;
      }
    }
    if (arguments_match) {
      return proc->oid;
    }
  }

  // Return an error that the proc was not found. Convert the types from oids to
  // names for a better error message.
  absl::StatusOr<std::vector<const char*>> argument_type_names_or =
      GetFormattedTypeNames(argument_types);
  std::ostringstream logged_types;
  if (argument_type_names_or.ok()) {
    std::copy(std::begin(*argument_type_names_or),
              std::end(*argument_type_names_or),
              std::ostream_iterator<const char*>(logged_types, ", "));
  } else {
    std::copy(std::begin(argument_types),
              std::end(argument_types),
              std::ostream_iterator<const Oid>(logged_types, ", "));
  }
  return absl::NotFoundError(absl::StrCat(
      "Procedure with namespace ", namespace_name, " and name ", proc_name,
      " and argument types ", logged_types.str(), " was not found"));
}

absl::StatusOr<const FormData_pg_cast*> PgBootstrapCatalog::GetCast(
    Oid source_type_id, Oid target_type_id) const {
  auto it = cast_by_castkey_.find(
      {.source = source_type_id, .target = target_type_id});
  if (it == cast_by_castkey_.end()) {
    return zetasql_base::NotFoundErrorBuilder()
           << "Cast function from type oid " << source_type_id
           << " to type oid " << target_type_id << " not found";
  }
  return it->second;
}

absl::StatusOr<const FormData_pg_cast*>
PgBootstrapCatalog::GetCastByFunctionOid(Oid function_oid) const {
  auto it = cast_by_castfunc_.find(function_oid);
  if (it == cast_by_castfunc_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Cast function with oid ", function_oid, " not found"));
  }
  return it->second;
}

absl::StatusOr<const FormData_pg_operator*> PgBootstrapCatalog::GetOperator(
    Oid oid) const {
  auto it = operator_by_oid_.find(oid);
  if (it == operator_by_oid_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Operator with oid ", oid, " not found"));
  }
  return it->second;
}

absl::StatusOr<absl::Span<const Oid>>
PgBootstrapCatalog::GetOperatorOidsByOprcode(Oid oprcode) const {
  auto it = operator_oprcode_to_oids_.find(oprcode);
  if (it == operator_oprcode_to_oids_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Operator with oprcode ", oprcode, " not found."));
  }
  return it->second;
}

absl::StatusOr<absl::Span<const Oid>> PgBootstrapCatalog::GetOperatorOids(
    absl::string_view name) const {
  auto it = operator_name_to_oids_.find(name);
  if (it == operator_name_to_oids_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Operator with name ", name, " not found"));
  }
  return it->second;
}

absl::StatusOr<Oid> PgBootstrapCatalog::GetOperatorOidByOprLeftRight(
    absl::string_view name, Oid oprleft, Oid oprright) const {
  ZETASQL_ASSIGN_OR_RETURN(absl::Span<const Oid> oper_oid_list, GetOperatorOids(name));

  for (const Oid oper_oid : oper_oid_list) {
    ZETASQL_ASSIGN_OR_RETURN(const FormData_pg_operator* curr_oper,
                     PgBootstrapCatalog::Default()->GetOperator(oper_oid));
    // If left and right values match, then return operator oid. Else return a
    // NotFoundError.
    if (curr_oper->oprleft == oprleft && curr_oper->oprright == oprright) {
      return oper_oid;
    }
  }
  return absl::NotFoundError(
      absl::StrCat("Operator with name: ", name, ", oprleft: ", oprleft,
                   ", oprright: ", oprright, ", not found."));
}

absl::StatusOr<const FormData_pg_aggregate*> PgBootstrapCatalog::GetAggregate(
    Oid oid) const {
  auto it = aggregate_by_oid_.find(oid);
  if (it == aggregate_by_oid_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Aggregate with oid ", oid, " not found"));
  }
  return it->second;
}

absl::StatusOr<const FormData_pg_opclass*> PgBootstrapCatalog::GetOpclass(
    Oid oid) const {
  auto it = opclass_by_oid_.find(oid);
  if (it == opclass_by_oid_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Operator Class with oid ", oid, " not found"));
  }
  return it->second;
}

absl::StatusOr<absl::Span<const Oid>> PgBootstrapCatalog::GetOpclassesByAm(
    Oid am_id) const {
  auto it = opclass_am_to_opclasses_.find(am_id);
  if (it == opclass_am_to_opclasses_.end()) {
    return absl::NotFoundError(absl::StrCat(
        "Operator Class with Access Method ", am_id, " not found"));
  }
  return it->second;
}

absl::StatusOr<absl::Span<const FormData_pg_amop* const>>
PgBootstrapCatalog::GetAmopsByAmopOpId(Oid opid) const {
  auto it = amop_by_oproid_.find(opid);
  if (it == amop_by_oproid_.end()) {
    return absl::NotFoundError(absl::StrCat(
        "Access Method Operator for operator ID ", opid, " not found"));
  }
  return it->second;
}

absl::StatusOr<const FormData_pg_amop*> PgBootstrapCatalog::GetAmopByFamily(
    Oid opfamily, Oid lefttype, Oid righttype, int16_t strategy) const {
  auto it = amop_by_familykey_.find({.opfamily = opfamily,
                                     .lefttype = lefttype,
                                     .righttype = righttype,
                                     .strategy = strategy});
  if (it == amop_by_familykey_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Access Method Operator for opfamily ", opfamily,
                     ", left argument type ", lefttype, " right argument type ",
                     righttype, " and strategy ", strategy, " not found"));
  }
  return it->second;
}

absl::StatusOr<const FormData_pg_amproc*> PgBootstrapCatalog::GetAmprocByFamily(
    Oid opfamily, Oid lefttype, Oid righttype, int16_t index) const {
  auto it = amproc_by_familykey_.find({.opfamily = opfamily,
                                       .lefttype = lefttype,
                                       .righttype = righttype,
                                       .index = index});
  if (it == amproc_by_familykey_.end()) {
    return absl::NotFoundError(absl::StrCat(
        "Access Method Procedure for opfamily ", opfamily,
        ", left input data type ", lefttype, " right input data type ",
        righttype, " and support procedure index ", index, " not found"));
  }
  return it->second;
}

absl::StatusOr<absl::Span<const FormData_pg_amproc* const>>
PgBootstrapCatalog::GetAmprocsByFamily(Oid opfamily, Oid lefttype) const {
  auto it =
      amprocs_by_partial_familykey_.find({.opfamily = opfamily,
                                          .lefttype = lefttype});
  if (it == amprocs_by_partial_familykey_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Access Method Procedure for opfamily ", opfamily,
                     ", input data type ", lefttype, " not found"));
  }
  return it->second;
}

void PgBootstrapCatalog::UpdateProc(
    const FormData_pg_proc_WithArgTypes& original_proc,
    const std::vector<Oid>& updated_arg_types, Oid updated_return_type) {
  if (updated_arg_types.size() != original_proc.data.proargtypes.vl_len_) {
    ABSL_LOG(ERROR) << "Updated proc for " << NameStr(original_proc.data.proname)
               << " has a different number of input arguments than the "
                  "original proc.";
  }

  // Create a non-const copy of the proc.
  size_t data_size = sizeof(FormData_pg_proc_WithArgTypes);
  std::unique_ptr<FormData_pg_proc_WithArgTypes> new_proc =
      std::make_unique<FormData_pg_proc_WithArgTypes>();
  memcpy(new_proc.get(), &original_proc, data_size);

  // Update the arg types.
  for (int i = 0; i < new_proc->data.proargtypes.vl_len_; ++i) {
    new_proc->data.proargtypes.values[i] = updated_arg_types[i];
  }

  // Update the return type.
  new_proc->data.prorettype = updated_return_type;

  // Store the updated proc.
  updated_proc_by_oid_.insert({original_proc.data.oid, std::move(new_proc)});
}

void PgBootstrapCatalog::UpdateOperator(
    const FormData_pg_operator& original_operator,
    const oidvector& updated_arg_types, Oid updated_return_type) {
  // Confirm that the number of updated args matches the number of inputs
  // for this operator.
  int num_operator_args = 0;
  if (original_operator.oprleft != InvalidOid) {
    ++num_operator_args;
  }
  if (original_operator.oprright != InvalidOid) {
    ++num_operator_args;
  }
  if (updated_arg_types.vl_len_ != num_operator_args) {
    ABSL_LOG(ERROR) << "Updated proc for the " << NameStr(original_operator.oprname)
               << " operator has a different number of input arguments.";
  }

  // Create a non-const copy of the operator.
  size_t data_size = sizeof(FormData_pg_operator);
  std::unique_ptr<FormData_pg_operator> new_operator =
      std::make_unique<FormData_pg_operator>();
  memcpy(new_operator.get(), &original_operator, data_size);

  // Update the arg types.
  if (updated_arg_types.vl_len_ == 2) {
    // A binary operator. Update both input types.
    new_operator->oprleft = updated_arg_types.values[0];
    new_operator->oprright = updated_arg_types.values[1];
  } else {
    // A unary operator. Usually the input will be oprright but sometimes
    // PostgreSQL puts the input in oprleft. Update the single input type.
    if (new_operator->oprleft == InvalidOid) {
      new_operator->oprright = updated_arg_types.values[0];
    } else {
      new_operator->oprleft = updated_arg_types.values[0];
    }
  }

  // Update the return type.
  new_operator->oprresult = updated_return_type;

  // Store the updated operator.
  updated_operator_by_oid_.insert(
      {original_operator.oid, std::move(new_operator)});
}

// Get a raw pointer to the FormData_pg_proc_WithArgTypes to store in the
// bootstrap catalog. If the proc is removed, return a nullptr. If the proc
// is unmodified, return the original input. If the proc is modified, make a
// copy of the original input, update the necessary fields, and return the
// updated copy.
const FormData_pg_proc_WithArgTypes* PgBootstrapCatalog::GetFinalProcData(
    const FormData_pg_proc_WithArgTypes& original_proc) {
  if (ProcIsRemoved(original_proc.data)) {
    return nullptr;
  }

  if (!ProcIsModified(original_proc.data)) {
    return &original_proc;
  }

  const PgProcSignature* updated_signature =
      GetUpdatedProcSignature(original_proc.data);
  UpdateProc(original_proc, updated_signature->arg_types,
             updated_signature->return_type);
  return updated_proc_by_oid_.find(original_proc.data.oid)->second.get();
}

// Get a raw pointer to the FormData_pg_operator to store in the
// bootstrap catalog. Should only be run after all procs have been
// processed. If the underlying proc is removed, return a nullptr. If the
// underlying proc is unmodified, return the original input. If the
// underlying proc is modified, make a copy of the original input, update
// the necessary fields, and return the updated copy.
const FormData_pg_operator* PgBootstrapCatalog::GetFinalOperatorData(
    const FormData_pg_operator& original_operator) {
  Oid proc_oid = original_operator.oprcode;

  if (proc_by_oid_.find(proc_oid) == proc_by_oid_.end()) {
    // The underlying proc was removed.
    return nullptr;
  }

  // Look up the modified proc if it exists.
  auto updated_proc_it = updated_proc_by_oid_.find(proc_oid);
  if (updated_proc_it == updated_proc_by_oid_.end()) {
    // The underlying proc was unmodified.
    return &original_operator;
  }

  const FormData_pg_proc_WithArgTypes* updated_proc =
      updated_proc_it->second.get();
  UpdateOperator(original_operator, updated_proc->data.proargtypes,
                 updated_proc->data.prorettype);
  return updated_operator_by_oid_.find(original_operator.oid)->second.get();
}

}  // namespace postgres_translator
