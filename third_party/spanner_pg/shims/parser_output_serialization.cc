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

#include "third_party/spanner_pg/shims/parser_output_serialization.h"

#include "absl/cleanup/cleanup.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace spangres {

absl::flat_hash_map<int, int> TokenLocationsFromProto(
    const ParserOutputProto& proto) {
  absl::flat_hash_map<int, int> token_locations;
  for (const auto& location : proto.token_locations()) {
    if (location.has_start() && location.has_end()) {
      token_locations[location.start()] = location.end();
    } else {
      ABSL_LOG(ERROR) << "unexpected token location missing "
                 << (location.has_start() ? "end" : "start");
    }
  }
  return token_locations;
}

absl::StatusOr<void*> DeserializeParseTree(
    const std::string& serialized_parse_tree) {
  // const_cast because stringToNode requires a regular char*.
  // This is safe with Google's (and most modern) std::string implementation
  // because .c_str() just calls .data() and .data() returns a regular char*.
  //
  // DON'T use std::string::data() so that, if an eager dev tries to
  // refactor this method to use an absl::string_view, the line below will
  // fail to compile.  We require null termination, which std::string::data()
  // does guarantee and absl::string_view::data() does not always guarantee.
  char* parse_tree_c_str = const_cast<char*>(serialized_parse_tree.c_str());

  // stringToNode() is an existing PostgreSQL function that returns a void*
  // that points to an instance of a struct that is effectively a subclass
  // of `Node`.  Specifically, in our use case it is always a `List` node.
  // So we take the void* and immediately perform a checked cast to List*.
  ZETASQL_ASSIGN_OR_RETURN(void* generic_node_ptr,
                   CheckedPgStringToNode(parse_tree_c_str));
  return generic_node_ptr;
}

absl::StatusOr<List*> DeserializeParseQuery(
    const std::string& serialized_parse_tree) {
  ZETASQL_ASSIGN_OR_RETURN(void* generic_node_ptr,
                   DeserializeParseTree(serialized_parse_tree));

  return generic_node_ptr == nullptr
             ? nullptr
             : internal::PostgresCastNode(List, generic_node_ptr);
}

absl::StatusOr<Node*> DeserializeParseExpression(
    const std::string& serialized_parse_tree) {
  ZETASQL_ASSIGN_OR_RETURN(void* generic_node_ptr,
                   DeserializeParseTree(serialized_parse_tree));

  return generic_node_ptr == nullptr
             ? nullptr
             : internal::PostgresCastToNode(generic_node_ptr);
}

absl::StatusOr<ParserOutputProto> ParserOutputToProto(
    const interfaces::ParserOutput& output) {
  ParserOutputProto proto;
  ZETASQL_ASSIGN_OR_RETURN(char* serialized_parse_tree,
                   CheckedPgNodeToString(output.parse_tree()));
  proto.set_serialized_parse_tree(serialized_parse_tree);
  for (const auto& start_end_pair : output.token_locations()) {
    auto* location = proto.add_token_locations();
    location->set_start(start_end_pair.first);
    location->set_end(start_end_pair.second);
  }
  return proto;
}

absl::StatusOr<interfaces::ParserOutput> ParserOutputFromProto(
    const ParserOutputProto& proto) {
  List* parse_tree;
  ZETASQL_ASSIGN_OR_RETURN(parse_tree,
                   DeserializeParseQuery(proto.serialized_parse_tree()));
  return interfaces::ParserOutput(parse_tree, TokenLocationsFromProto(proto));
}

}  // namespace spangres
}  // namespace postgres_translator
