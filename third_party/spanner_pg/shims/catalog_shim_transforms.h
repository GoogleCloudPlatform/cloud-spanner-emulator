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

#ifndef SHIMS_CATALOG_SHIM_TRANSFORMS_H_
#define SHIMS_CATALOG_SHIM_TRANSFORMS_H_

#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/table_name.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

// This file was formerly transformation funcitons common to both the PostgreSQL
// analyzer and our transformer. Most functions have been moved into the
// transformer.
// TODO: Merge this file into catalog_shim_cc_wrappers.
namespace postgres_translator {

// Given a PostgreSQL RangeVar representing a Table object, gets a TableName
// representing the qualified name per PostgreSQL namespacing rules
// (<catalogname>.<schemaname>.<relname>).
absl::StatusOr<TableName> TableNameFromRangeVar(RangeVar& relation);

}  // namespace postgres_translator

#endif  // SHIMS_CATALOG_SHIM_TRANSFORMS_H_
