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

#ifndef CODEGEN_DEFAULT_VALUES_READER_H_
#define CODEGEN_DEFAULT_VALUES_READER_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"

namespace postgres_translator {

// Reads the default values for all PostgreSQL signatures that contain optional
// arguments. The output is a map of signature OIDs to their default values,
// represented as a vector of string PostgreSQL expressions (e.g.
// "null::bigint", "'test'::text").
absl::StatusOr<absl::flat_hash_map<uint32_t, std::vector<std::string>>>
GetProcsDefaultValues();

absl::StatusOr<absl::flat_hash_map<uint32_t, std::vector<std::string>>>
GetProcsDefaultValues(std::string_view json_string);

absl::flat_hash_map<uint32_t, std::vector<std::string>>
GetProcsDefaultValuesOrDie();

}  // namespace postgres_translator

#endif  // CODEGEN_DEFAULT_VALUES_READER_H_
