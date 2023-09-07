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

#ifndef BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_STRUCTS_H_
#define BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_STRUCTS_H_

#include <cstdint>
#include <string>

#include "util/gtl/extend/debug_printing.h"
#include "util/gtl/extend/equality.h"
#include "util/gtl/extend/extend.h"

namespace postgres_translator {

struct PgTypeData : gtl::Extend<PgTypeData>::With<
    gtl::EqualityExtension, gtl::DebugPrintingExtension> {
  uint32_t oid;
  std::string typname;
  int16_t typlen;
  bool typbyval;
  char typtype;
  char typcategory;
  bool typispreferred;
  bool typisdefined;
  char typdelim;
  uint32_t typelem;
  uint32_t typarray;
};

}  // namespace postgres_translator

#endif  // BOOTSTRAP_CATALOG_BOOTSTRAP_CATALOG_STRUCTS_H_
