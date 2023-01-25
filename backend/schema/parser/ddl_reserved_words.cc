//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/schema/parser/ddl_reserved_words.h"

#include <string>

#include "backend/common/case.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

namespace {

static const CaseInsensitiveStringSet* const reserved_words =
    new CaseInsensitiveStringSet {
    "ALL",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASSERT_ROWS_MODIFIED",
    "AT",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COLLATE",
    "CONTAINS",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "DEFAULT",
    "DEFINE",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "ENUM",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDE",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FOLLOWING",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HASH",
    "HAVING",
    "IF",
    "IGNORE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOOKUP",
    "MERGE",
    "NATURAL",
    "NEW",
    "NO",
    "NOT",
    "NULL",
    "NULLS",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRECEDING",
    "PROTO",
    "RANGE",
    "RECURSIVE",
    "RESPECT",
    "RIGHT",
    "ROLLUP",
    "ROWS",
    "SELECT",
    "SET",
    "SOME",
    "STRUCT",
    "TABLESAMPLE",
    "THEN",
    "TO",
    "TREAT",
    "TRUE",
    "UNBOUNDED",
    "UNION",
    "UNNEST",
    "USING",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "WITHIN",
};

static const CaseInsensitiveStringSet* const pseudo_reserved_words =
    new CaseInsensitiveStringSet {
    "ACTION",
    "ADD",
    "ALTER",
    "ANALYZE",
    "BOOL",
    "BUNDLE",
    "BYTES",
    "CASCADE",
    "CHANGE",
    "CHECK",
    "COLUMN",
    "CONSTRAINT",
    "DATABASE",
    "DATE",
    "DAY",
    "DELETE",
    "DELETION",
    "DROP",
    "EXECUTE",
    "FIRST",
    "FLOAT64",
    "FOREIGN",
    "FUNCTION",
    "GRANT",
    "INDEX",
    "INSERT",
    "INT64",
    "INTERLEAVE",
    "INPUT",
    "INVOKER",
    "JSON",
    "KEY",
    "LAST",
    "MAX",
    "MODEL",
    "NULL_FILTERED",
    "NUMERIC",
    "OPTIONS",
    "OUTPUT",
    "PARENT",
    "PG",
    "POLICY",
    "PRIMARY",
    "REMOTE",
    "REFERENCES",
    "REPLACE",
    "REVOKE",
    "ROLE",
    "ROW",
    "SCHEMA",
    "SEARCH",
    "SECURITY",
    "SEQUENCE",
    "SQL",
    "STATISTICS",
    "STORED",
    "STORING",
    "STREAM",
    "STRING",
    "TABLE",
    "TIMESTAMP",
    "TOKENLIST",
    "UNIQUE",
    "UPDATE",
    "VIEW",
};

}  // namespace

bool IsReservedWord(absl::string_view reserved_word) {
  return reserved_words->contains(std::string(reserved_word));
}

const CaseInsensitiveStringSet& GetReservedWords() { return *reserved_words; }

const CaseInsensitiveStringSet& GetPseudoReservedWords() {
  return *pseudo_reserved_words;
}


}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
