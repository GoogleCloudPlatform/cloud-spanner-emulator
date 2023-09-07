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

#include "third_party/spanner_pg/catalog/table_name.h"

#include "zetasql/base/logging.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"

namespace postgres_translator {

TableName::TableName(absl::Span<const std::string> name)
    : name_(name.begin(), name.end()) {
  // A 1-size empty string TableName is ok. A 0-size is not well-formed.
  ABSL_CHECK_GT(name_.size(), 0);
  // A list of names longer than 3 can't have come from PostgreSQL since they
  // are hard coded to at most 3. ZetaSQL should still be able to handle
  // longer, but it's pretty strange if it happens and likely points to a bug
  // somewhere.
  ABSL_CHECK_LE(name_.size(), 3);
}

std::string TableName::ToString() const { return absl::StrJoin(name_, "."); }

const std::string& TableName::UnqualifiedName() const { return name_.back(); }

const std::string* TableName::NamespaceName() const {
  if (name_.size() > 2) {
    return &name_[name_.size() - 2];
  } else if (name_.size() <= 1) {
    return nullptr;
  } else {
    return &name_[0];
  }
}

bool TableName::IsEmpty() const {
  return name_.size() == 1 && name_[0].empty();
}

}  // namespace postgres_translator
