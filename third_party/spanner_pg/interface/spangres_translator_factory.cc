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

#include "third_party/spanner_pg/interface/spangres_translator_factory.h"

#include <memory>

#include "third_party/spanner_pg/interface/stub_spangres_translator.h"

namespace postgres_translator {
namespace interfaces {

std::function<std::unique_ptr<SpangresTranslatorInterface>()>*
    SpangresTranslatorFactory::factory_ = nullptr;

void SpangresTranslatorFactory::InitStatics() {
  static auto* stub_factory =
      new std::function<std::unique_ptr<SpangresTranslatorInterface>()>([]() {
        return std::make_unique<spangres::StubSpangresTranslator>();
      });
  if (factory_ == nullptr) {
    factory_ = stub_factory;
  }
}

}  // namespace interfaces
}  // namespace postgres_translator

namespace {
static bool translator_factory_module_initialization_complete = []() {
  postgres_translator::interfaces::SpangresTranslatorFactory::InitStatics();
  return true;
} ();
}  // namespace
