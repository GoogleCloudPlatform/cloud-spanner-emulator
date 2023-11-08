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

#include "third_party/spanner_pg/bootstrap_catalog/proc_changelist.h"

#include <vector>

#include "zetasql/base/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

namespace postgres_translator {
namespace {

// The proc name and signature uniquely identify a proc.
// Used as a member of procs_to_remove and the key of procs_to_update.
struct PgProcId {
  std::string name;
  PgProcSignature signature;

  // Define the == operator and hashing function so that the PgProcId can be
  // used in an absl container.
  bool operator==(const PgProcId& other) const {
    return name == other.name && signature == other.signature;
  }

  template <typename H>
  friend H AbslHashValue(H state, const PgProcId& proc_id) {
    return H::combine(std::move(state), proc_id.name, proc_id.signature);
  }
};

static PgProcId MakePgProcId(const FormData_pg_proc& proc) {
  std::vector<Oid> args;
  args.reserve(proc.proargtypes.dim1);
  for (int i = 0; i < proc.proargtypes.dim1; ++i) {
    args.push_back(proc.proargtypes.values[i]);
  }
  PgProcSignature signature{.arg_types = args, .return_type = proc.prorettype};
  PgProcId id{.name = NameStr(proc.proname), .signature = signature};
  return id;
}

// Metadata to unique identify procs to remove from the bootstrap catalog.
// Initially populated with all casts to int4.
absl::flat_hash_set<PgProcId> procs_to_remove = {
    {"int4", {{BITOID}, INT4OID}},    {"int4", {{BOOLOID}, INT4OID}},
    {"int4", {{CHAROID}, INT4OID}},   {"int4", {{FLOAT4OID}, INT4OID}},
    {"int4", {{FLOAT8OID}, INT4OID}}, {"int4", {{INT2OID}, INT4OID}},
    {"int4", {{INT8OID}, INT4OID}},   {"int4", {{JSONBOID}, INT4OID}},
    {"int4", {{NUMERICOID}, INT4OID}}};

// Metadata to unique identify procs to update in the bootstrap catalog, along
// with the new signature for each proc.
absl::flat_hash_map<PgProcId, PgProcSignature> procs_to_update = {
    {{"substr", {{TEXTOID, INT4OID}, TEXTOID}}, {{TEXTOID, INT8OID}, TEXTOID}},
    {{"substr", {{TEXTOID, INT4OID, INT4OID}, TEXTOID}},
     {{TEXTOID, INT8OID, INT8OID}, TEXTOID}},
    {{"substring", {{TEXTOID, INT4OID}, TEXTOID}},
     {{TEXTOID, INT8OID}, TEXTOID}},
    {{"substring", {{TEXTOID, INT4OID, INT4OID}, TEXTOID}},
     {{TEXTOID, INT8OID, INT8OID}, TEXTOID}},
    {{"substr", {{BYTEAOID, INT4OID}, BYTEAOID}},
     {{BYTEAOID, INT8OID}, BYTEAOID}},
    {{"substr", {{BYTEAOID, INT4OID, INT4OID}, BYTEAOID}},
     {{BYTEAOID, INT8OID, INT8OID}, BYTEAOID}},
    {{"substring", {{BYTEAOID, INT4OID}, BYTEAOID}},
     {{BYTEAOID, INT8OID}, BYTEAOID}},
    {{"substring", {{BYTEAOID, INT4OID, INT4OID}, BYTEAOID}},
     {{BYTEAOID, INT8OID, INT8OID}, BYTEAOID}},
    {{"int8shl", {{INT8OID, INT4OID}, INT8OID}}, {{INT8OID, INT8OID}, INT8OID}},
    {{"int8shr", {{INT8OID, INT4OID}, INT8OID}}, {{INT8OID, INT8OID}, INT8OID}},
    {{"jsonb_array_element", {{JSONBOID, INT4OID}, JSONBOID}},
     {{JSONBOID, INT8OID}, JSONBOID}},
    {{"jsonb_array_element_text", {{JSONBOID, INT4OID}, TEXTOID}},
     {{JSONBOID, INT8OID}, TEXTOID}},
    {{"strpos", {{TEXTOID, TEXTOID}, INT4OID}}, {{TEXTOID, TEXTOID}, INT8OID}},
    {{"length", {{TEXTOID}, INT4OID}}, {{TEXTOID}, INT8OID}},
    {{"length", {{BYTEAOID}, INT4OID}}, {{BYTEAOID}, INT8OID}},
    {{"lpad", {{TEXTOID, INT4OID}, TEXTOID}}, {{TEXTOID, INT8OID}, TEXTOID}},
    {{"lpad", {{TEXTOID, INT4OID, TEXTOID}, TEXTOID}},
     {{TEXTOID, INT8OID, TEXTOID}, TEXTOID}},
    {{"rpad", {{TEXTOID, INT4OID}, TEXTOID}}, {{TEXTOID, INT8OID}, TEXTOID}},
    {{"rpad", {{TEXTOID, INT4OID, TEXTOID}, TEXTOID}},
     {{TEXTOID, INT8OID, TEXTOID}, TEXTOID}},
    {{"left", {{TEXTOID, INT4OID}, TEXTOID}}, {{TEXTOID, INT8OID}, TEXTOID}},
    {{"right", {{TEXTOID, INT4OID}, TEXTOID}}, {{TEXTOID, INT8OID}, TEXTOID}},
    {{"repeat", {{TEXTOID, INT4OID}, TEXTOID}}, {{TEXTOID, INT8OID}, TEXTOID}},
    {{"ascii", {{TEXTOID}, INT4OID}}, {{TEXTOID}, INT8OID}},
    {{"chr", {{INT4OID}, TEXTOID}}, {{INT8OID}, TEXTOID}},
    {{"to_timestamp", {{FLOAT8OID}, TIMESTAMPTZOID}},
     {{INT8OID}, TIMESTAMPTZOID}},
    {{"make_date", {{INT4OID, INT4OID, INT4OID}, DATEOID}},
     {{INT8OID, INT8OID, INT8OID}, DATEOID}},
    {{"trunc", {{NUMERICOID, INT4OID}, NUMERICOID}},
     {{NUMERICOID, INT8OID}, NUMERICOID}},
    {{"nextval", {{REGCLASSOID}, INT8OID}}, {{TEXTOID}, INT8OID}},
    {{"array_length", {{ANYARRAYOID, INT4OID}, INT4OID}},
     {{ANYARRAYOID, INT8OID}, INT8OID}},
    {{"array_upper", {{ANYARRAYOID, INT4OID}, INT4OID}},
     {{ANYARRAYOID, INT8OID}, INT8OID}},
    {{"date_mi", {{DATEOID, DATEOID}, INT4OID}}, {{DATEOID, DATEOID}, INT8OID}},
    {{"date_mii", {{DATEOID, INT4OID}, DATEOID}},
     {{DATEOID, INT8OID}, DATEOID}},
    {{"date_pli", {{DATEOID, INT4OID}, DATEOID}},
     {{DATEOID, INT8OID}, DATEOID}},
};

}  // namespace

bool ProcIsRemoved(const FormData_pg_proc& proc) {
  return procs_to_remove.contains(MakePgProcId(proc));
}

bool ProcIsModified(const FormData_pg_proc& proc) {
  return procs_to_update.find(MakePgProcId(proc)) != procs_to_update.end();
}

const PgProcSignature* GetUpdatedProcSignature(const FormData_pg_proc& proc) {
  ABSL_CHECK(ProcIsModified(proc));
  return &procs_to_update.find(MakePgProcId(proc))->second;
}

}  // namespace postgres_translator
