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

#include <vector>

#include "third_party/spanner_pg/interface/bootstrap_catalog_data.pb.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

// A class to represent the signature of a proc.
struct PgProcSignature {
  std::vector<Oid> arg_types;
  Oid return_type;

  // Define the == operator and hashing function so that the signature can be
  // used in an absl container.
  bool operator==(const PgProcSignature& other) const {
    return arg_types == other.arg_types && return_type == other.return_type;
  }

  template <typename H>
  friend H AbslHashValue(H state, const PgProcSignature& signature) {
    return H::combine(std::move(state), signature.arg_types,
                      signature.return_type);
  }
};

// Returns true if a proc should be excluded from the bootstrap catalog.
bool ProcIsRemoved(const FormData_pg_proc& proc);

// Returns true if a proc needs to be modified before being added to the
// bootstrap catalog. If true, should be followed by GetUpdatedProcSignature
// and GetProcDefaultArgumentCount/GetProcDefaultArguments.
bool ProcIsModified(const FormData_pg_proc& proc);

// Returns the new signature for this proc. Returns a nullptr if there is no
// updated signature for this proc.
const PgProcSignature* GetUpdatedProcSignature(const FormData_pg_proc& proc);

// Returns the new signature for this proc. Does not check if the proc should be
// modified. Only call on functions that should be modified.
const PgProcSignature* GetUpdatedProcSignature(const PgProcData& proc);

// Returns the new pronargdefaults value for this proc. Returns 0 if the proc
// should not have any default arguments.
uint16_t GetProcDefaultArgumentCount(const FormData_pg_proc& proc);

// Returns the default arguments for this proc. Returns a nullptr if the proc
// should not have any default arguments.
const std::vector<std::string>* GetProcDefaultArguments(const PgProcData& proc);

}  // namespace postgres_translator
