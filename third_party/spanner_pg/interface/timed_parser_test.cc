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

#include "third_party/spanner_pg/interface/timed_parser.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/interface/parser_output.h"

namespace postgres_translator {
namespace spangres {

namespace {

using testing::Return;
using zetasql_base::testing::StatusIs;

class MockTimedParser : public TimedParser {
 public:
  ~MockTimedParser() override = default;

  MOCK_METHOD(absl::Status, SetupParser,
              (interfaces::ParserBatchOutput::Statistics*), (override));

  absl::Status ParseIntoBatchOffFiber(
      absl::Span<const std::string> sql_expressions,
      interfaces::ParserBatchOutput* output) override {
    for (const std::string& sql : sql_expressions) {
      output->mutable_output()->push_back(absl::FailedPreconditionError(
          absl::StrCat("requested to parse: ", sql)));
    }
    return absl::OkStatus();
  }
};

class TimedParserTest : public ::testing::Test {
 public:
  void SetUp() override { timed_parser_ = std::make_unique<MockTimedParser>(); }

 protected:
  std::unique_ptr<MockTimedParser> timed_parser_;
};

TEST_F(TimedParserTest, SetupFailure) {
  ON_CALL(*timed_parser_, SetupParser)
      .WillByDefault(Return(absl::InternalError("foo")));

  EXPECT_THAT(timed_parser_->Parse("shouldn't matter").output(),
              StatusIs(absl::StatusCode::kInternal, "foo"));
}

}  // namespace

}  // namespace spangres
}  // namespace postgres_translator
