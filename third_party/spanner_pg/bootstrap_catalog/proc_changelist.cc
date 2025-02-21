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

#include <cstdint>
#include <vector>

#include "zetasql/base/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_data.pb.h"

namespace postgres_translator {
namespace {

#define TOKENLISTOID 50001

// Metadata to identify procs to remove from the bootstrap catalog.
// Initially populated with all casts to int4.
absl::flat_hash_set<Oid> procs_to_remove = {
    F_INT4_BIT,   F_INT4_BOOL,  F_INT4_CHAR,  F_INT4_FLOAT4,  F_INT4_FLOAT8,
    F_INT4_INT2,  F_INT4_INT8,  F_INT4_JSONB, F_INT4_NUMERIC, F_FLOAT48PL,
    F_FLOAT84PL,  F_FLOAT48MI,  F_FLOAT84MI,  F_FLOAT48DIV,   F_FLOAT84DIV,
    F_FLOAT48MUL, F_FLOAT84MUL,
};

// Metadata to identify procs in the bootstrap catalog whose signatures should
// be updated.
absl::flat_hash_map<Oid, PgProcSignature> proc_signatures_to_update = {
    {F_SUBSTR_TEXT_INT4, {{TEXTOID, INT8OID}, TEXTOID}},
    {F_SUBSTR_TEXT_INT4_INT4, {{TEXTOID, INT8OID, INT8OID}, TEXTOID}},
    {F_SUBSTRING_TEXT_INT4, {{TEXTOID, INT8OID}, TEXTOID}},
    {F_SUBSTRING_TEXT_INT4_INT4, {{TEXTOID, INT8OID, INT8OID}, TEXTOID}},
    {F_SUBSTR_BYTEA_INT4, {{BYTEAOID, INT8OID}, BYTEAOID}},
    {F_SUBSTR_BYTEA_INT4_INT4, {{BYTEAOID, INT8OID, INT8OID}, BYTEAOID}},
    {F_SUBSTRING_BYTEA_INT4, {{BYTEAOID, INT8OID}, BYTEAOID}},
    {F_SUBSTRING_BYTEA_INT4_INT4, {{BYTEAOID, INT8OID, INT8OID}, BYTEAOID}},
    {F_INT8SHL, {{INT8OID, INT8OID}, INT8OID}},
    {F_INT8SHR, {{INT8OID, INT8OID}, INT8OID}},
    {F_JSONB_ARRAY_ELEMENT, {{JSONBOID, INT8OID}, JSONBOID}},
    {F_JSONB_ARRAY_ELEMENT_TEXT, {{JSONBOID, INT8OID}, TEXTOID}},
    {F_STRPOS, {{TEXTOID, TEXTOID}, INT8OID}},
    {F_LENGTH_TEXT, {{TEXTOID}, INT8OID}},
    {F_LENGTH_BYTEA, {{BYTEAOID}, INT8OID}},
    {F_LPAD_TEXT_INT4, {{TEXTOID, INT8OID}, TEXTOID}},
    {F_LPAD_TEXT_INT4_TEXT, {{TEXTOID, INT8OID, TEXTOID}, TEXTOID}},
    {F_RPAD_TEXT_INT4, {{TEXTOID, INT8OID}, TEXTOID}},
    {F_RPAD_TEXT_INT4_TEXT, {{TEXTOID, INT8OID, TEXTOID}, TEXTOID}},
    {F_LEFT, {{TEXTOID, INT8OID}, TEXTOID}},
    {F_RIGHT, {{TEXTOID, INT8OID}, TEXTOID}},
    {F_REPEAT, {{TEXTOID, INT8OID}, TEXTOID}},
    {F_ASCII, {{TEXTOID}, INT8OID}},
    {F_CHR, {{INT8OID}, TEXTOID}},
    {F_TO_TIMESTAMP_FLOAT8, {{INT8OID}, TIMESTAMPTZOID}},
    {F_MAKE_DATE, {{INT8OID, INT8OID, INT8OID}, DATEOID}},
    {F_TRUNC_NUMERIC_INT4, {{NUMERICOID, INT8OID}, NUMERICOID}},
    {F_NEXTVAL, {{TEXTOID}, INT8OID}},
    {F_ARRAY_LENGTH, {{ANYARRAYOID, INT8OID}, INT8OID}},
    {F_ARRAY_UPPER, {{ANYARRAYOID, INT8OID}, INT8OID}},
    {F_DATE_MI, {{DATEOID, DATEOID}, INT8OID}},
    {F_DATE_MII, {{DATEOID, INT8OID}, DATEOID}},
    {F_DATE_PLI, {{DATEOID, INT8OID}, DATEOID}},
    {F_JSONB_DELETE_JSONB_INT4, {{JSONBOID, INT8OID}, JSONBOID}},
};

// Metadata to identify procs in the boostrap catalog with default arguments.
absl::flat_hash_map<Oid, std::vector<std::string>> proc_default_args_to_update =
    {
};

// Metadata to identify procs in the bootstrap catalog whose argument names
// should be updated.
absl::flat_hash_map<Oid, std::vector<std::string>> proc_arg_names_to_update = {
    {F_JSONB_SET_LAX,
     {"jsonb_in", "path", "replacement", "create_if_missing",
      "null_value_treatment"}},
    {F_JSONB_SET, {"jsonb_in", "path", "replacement", "create_if_missing"}},
    {F_JSONB_INSERT, {"jsonb_in", "path", "replacement", "insert_after"}},
};

}  // namespace

bool ProcIsRemoved(Oid proc_oid) { return procs_to_remove.contains(proc_oid); }

bool ProcIsModified(Oid proc_oid) {
  return (proc_signatures_to_update.find(proc_oid) !=
          proc_signatures_to_update.end()) ||
         (proc_default_args_to_update.find(proc_oid) !=
          proc_default_args_to_update.end()) ||
         (proc_arg_names_to_update.find(proc_oid) !=
          proc_arg_names_to_update.end());
}

const PgProcSignature* GetUpdatedProcSignature(Oid proc_oid) {
  auto it = proc_signatures_to_update.find(proc_oid);
  if (it != proc_signatures_to_update.end()) {
    return &it->second;
  }
  return nullptr;
}

uint16_t GetProcDefaultArgumentCount(Oid proc_oid) {
  auto it = proc_default_args_to_update.find(proc_oid);
  if (it != proc_default_args_to_update.end()) {
    return it->second.size();
  }
  return 0;
}

const std::vector<std::string>* GetProcDefaultArguments(Oid proc_oid) {
  auto it = proc_default_args_to_update.find(proc_oid);
  if (it != proc_default_args_to_update.end()) {
    return &it->second;
  }
  return nullptr;
}

const std::vector<std::string>* GetUpdatedProcArgNames(Oid proc_oid) {
  auto it = proc_arg_names_to_update.find(proc_oid);
  if (it != proc_arg_names_to_update.end()) {
    return &it->second;
  }
  return nullptr;
}

}  // namespace postgres_translator
