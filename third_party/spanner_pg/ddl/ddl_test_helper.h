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

#ifndef DDL_DDL_TEST_HELPER_H_
#define DDL_DDL_TEST_HELPER_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/ddl/ddl_translator.h"
#include "third_party/spanner_pg/ddl/spangres_schema_printer.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"

namespace postgres_translator::spangres {

class DdlTestHelper {
 public:
  // Initializes all the resources. PostgreSQL parser uses memory arena that is
  // set as thread-local, so it should be called in every thread (which means
  // for file_based_test_driver calling this in the test itself).
  DdlTestHelper();

  PostgreSQLToSpannerDDLTranslator* Translator() const {
    return translator_.get();
  }
  SpangresSchemaPrinter* SchemaPrinter() const { return schema_printer_.get(); }
  interfaces::ParserInterface* Parser() const { return parser_.get(); }

 private:
  // Not copyable or movable
  DdlTestHelper(const DdlTestHelper&) = delete;
  DdlTestHelper& operator=(const DdlTestHelper&) = delete;
  DdlTestHelper(DdlTestHelper&&) = delete;
  DdlTestHelper& operator=(DdlTestHelper&&) = delete;

  std::unique_ptr<PostgreSQLToSpannerDDLTranslator> translator_;
  std::unique_ptr<SpangresSchemaPrinter> schema_printer_;

  std::unique_ptr<interfaces::SpangresTranslatorInterface> spangres_translator_;
  std::unique_ptr<interfaces::ParserInterface> parser_;
};
}  // namespace postgres_translator::spangres

#endif  // DDL_DDL_TEST_HELPER_H_
