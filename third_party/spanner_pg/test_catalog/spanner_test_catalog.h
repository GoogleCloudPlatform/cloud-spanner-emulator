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

#ifndef TEST_CATALOG_SPANNER_TEST_CATALOG_H_
#define TEST_CATALOG_SPANNER_TEST_CATALOG_H_

#include "zetasql/public/catalog.h"

namespace postgres_translator::spangres::test {

// If the Spanner EngineUserCatalog singleton is not already initialized,
// initializes it.
// Returns a raw pointer to the Spanner EngineUserCatalog singleton.
// This EngineUserCatalog is a Spanner SqlCatalog that is loaded directly from
// the SDL file and can take a while (several seconds).
// Due to the dependency chain, it cannot be used in systests.
//
// THIS IS NOT THREAD SAFE!
// If a catalog must be used by multiple threads concurrently, either
// synchronize access to it or give each thread their open copy via
// `GetSpangresTestSqlCatalog()`.
zetasql::EnumerableCatalog* GetSpangresTestSpannerUserCatalog();

}  // namespace postgres_translator::spangres::test

#endif  // TEST_CATALOG_SPANNER_TEST_CATALOG_H_
