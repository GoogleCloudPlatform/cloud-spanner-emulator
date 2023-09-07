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

#ifndef CATALOG_TABLE_NAME_H_
#define CATALOG_TABLE_NAME_H_

#include <iostream>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace postgres_translator {

// TableName
// Class used internally by the postgres translator as a global identifier of a
// user's table.
//
// This is necessitated by different zetasql::Table and zetasql::Catalog
// implementations that have different global identifier semantics (for example,
// some implement table->FullName() as a global identifier, others do not).
class TableName {
 public:
  // Initializes this object with an ordered list of names from coarsest
  // qualifier (namespace, usually) to finest (table's simple name).
  explicit TableName(absl::Span<const std::string> name);

  // Gets the full name of this table, parts joined with '.' characters.
  //
  // Since we do not enforce restrictions on our content, this is not
  // necessarily a unique identifier if the underlying engine permits '.'
  // characters in its identifiers.
  std::string ToString() const;

  // Gets only the unqualified name of this table.
  // String reference instead of string_view because some users want a c_str.
  const std::string& UnqualifiedName() const;

  // Checks whether this is a single empty string.
  bool IsEmpty() const;

  // Gets an absl::Span view for zetasql::Catalog lookups.
  absl::Span<const std::string> AsSpan() const { return name_; }

  // Returns the namespace name of this name, nullptr if name is not qualified.
  const std::string* NamespaceName() const;

  // Convenience overloads for streaming, comparison, hashing etc.
  friend bool operator==(const TableName& a, const TableName& b) {
    return a.name_ == b.name_;
  }
  friend bool operator!=(const TableName& a, const TableName& b) {
    return !(a == b);
  }
  friend std::ostream& operator<<(std::ostream& os, const TableName& table) {
    return os << table.ToString();
  }
  template <typename H>
  friend H AbslHashValue(H h, const TableName& table) {
    return H::combine(std::move(h), table.name_);
  }

 private:
  // Internally, table name is represented as a vector of strings.
  const std::vector<std::string> name_;
};

}  // namespace postgres_translator

#endif  // CATALOG_TABLE_NAME_H_
