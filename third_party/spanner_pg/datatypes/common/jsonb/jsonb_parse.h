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

// This file declares the PG.JSONB parsing interface method.
#ifndef DATATYPES_COMMON_JSONB_JSONB_PARSE_H_
#define DATATYPES_COMMON_JSONB_JSONB_PARSE_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {

// Checks that `json` is a valid JSON document and converts it into Spanner
// normalized representation.
absl::StatusOr<absl::Cord> ParseJson(absl::string_view json);

// Normalizes a string to conform to PG.JSONB's normalization rules, for
// dealing with escaping characters and rejecting \u0000.
std::string NormalizeJsonbString(absl::string_view value);

// Checks if an input Jsonb string contains unicode 0 (\0).
bool IsValidJsonbString(absl::string_view str);

}  // namespace postgres_translator::spangres::datatypes::common::jsonb

#endif  // DATATYPES_COMMON_JSONB_JSONB_PARSE_H_
