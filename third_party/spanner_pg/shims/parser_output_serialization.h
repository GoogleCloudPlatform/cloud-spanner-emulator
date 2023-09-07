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

#ifndef SHIMS_PARSER_OUTPUT_SERIALIZATION_H_
#define SHIMS_PARSER_OUTPUT_SERIALIZATION_H_

#include "absl/status/statusor.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/shims/parser_output.pb.h"
#include "third_party/spanner_pg/shims/parser_shim.h"

namespace postgres_translator {
namespace spangres {

// Translate the token_locations field of the input proto into a map with token
// start locations as keys and token end locations as values.
absl::flat_hash_map<int, int> TokenLocationsFromProto(
    const ParserOutputProto& proto);

// Deserializes and returns the given serialized parse tree.  Assumes the caller
// has already set up the necessary memory context, for example by holding an
// instance of MemoryContextPGArena.
absl::StatusOr<List*> DeserializeParseQuery(
    const std::string& serialized_parse_tree);

// Deserializes and returns the given serialized parse tree.  Assumes the caller
// has already set up the necessary memory context, for example by holding an
// instance of MemoryContextPGArena.
absl::StatusOr<Node*> DeserializeParseExpression(
    const std::string& serialized_parse_tree);

// Parser output serialization / deserialization
absl::StatusOr<interfaces::ParserOutput> ParserOutputFromProto(
    const ParserOutputProto& proto);
absl::StatusOr<ParserOutputProto> ParserOutputToProto(
    const interfaces::ParserOutput& output);

}  // namespace spangres
}  // namespace postgres_translator

#endif  // SHIMS_PARSER_OUTPUT_SERIALIZATION_H_
