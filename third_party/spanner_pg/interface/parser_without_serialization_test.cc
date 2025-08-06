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

#include "third_party/spanner_pg/interface/parser_without_serialization.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"

namespace postgres_translator {
namespace spangres {

namespace {
using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

class ParserWithoutSerializationTest : public ::testing::Test {
 protected:
  ParserWithoutSerialization parser_;
};

TEST_F(ParserWithoutSerializationTest, ExistingMemoryContext) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<interfaces::PGArena> extra_arena,
                       MemoryContextPGArena::Init(
                           std::make_unique<StubMemoryReservationManager>()));
  interfaces::ParserSingleOutput output(parser_.Parse("select 1234567890123"));
  // We should get an internal error from attempting to overwrite an active
  // MemoryContext.
  EXPECT_THAT(output.output().status(),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(ParserWithoutSerializationTest,
       ReturnsErrorOnInvalidUnicodeEscapeValue) {
  interfaces::ParserSingleOutput output(parser_.Parse("u&\'\\+8FF2C7\'"));
  EXPECT_THAT(output.output().status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid Unicode escape value")));
}

}  // namespace

}  // namespace spangres
}  // namespace postgres_translator
