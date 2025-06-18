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

#ifndef CATALOG_SPANGRES_SYSTEM_CATALOG_GOLDEN_H_
#define CATALOG_SPANGRES_SYSTEM_CATALOG_GOLDEN_H_

#include <string>
#include <vector>

#include "zetasql/public/function.pb.h"
#include "zetasql/public/options.pb.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.pb.h"

namespace postgres_translator {

spangres::SpangresSystemCatalog* InitializeEngineSystemCatalog(
    bool is_emulator);

class SpangresSystemCatalogGolden {
 public:
  SpangresSystemCatalogGolden(spangres::SpangresSystemCatalog* catalog)
      : catalog_(catalog) {}

  std::string Capture();

 private:
  spangres::SpangresSystemCatalog* catalog_;

  spangres::SpangresFunctionSignatureProto ToProto(
      const PostgresFunctionSignatureArguments& signature);

  spangres::SpangresFunctionProto ToProto(
      const PostgresFunctionArguments& function);

  spangres::SpangresSystemCatalogProto ToProto(
      std::vector<PostgresFunctionArguments>& functions);
};

}  // namespace postgres_translator

#endif  // CATALOG_SPANGRES_SYSTEM_CATALOG_GOLDEN_H_
