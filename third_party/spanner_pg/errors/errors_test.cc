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

#include "third_party/spanner_pg/errors/errors.h"

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace spanner {
namespace error {

namespace {

using spangres::error::CanonicalCode;

TEST(Errors, MapPGErrorCodeToCanonicalCode) {
  EXPECT_EQ(CanonicalCode(ERRCODE_FEATURE_NOT_SUPPORTED),
            absl::StatusCode::kUnimplemented);
  EXPECT_EQ(CanonicalCode(ERRCODE_INSUFFICIENT_RESOURCES),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(CanonicalCode(ERRCODE_UNDEFINED_SCHEMA),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(CanonicalCode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            absl::StatusCode::kPermissionDenied);
  EXPECT_EQ(CanonicalCode(ERRCODE_UNDEFINED_TABLE),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(CanonicalCode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(CanonicalCode(ERRCODE_INDEX_CORRUPTED),
            absl::StatusCode::kDataLoss);
  EXPECT_EQ(CanonicalCode(ERRCODE_INTERNAL_ERROR),
            absl::StatusCode::kInternal);
  EXPECT_EQ(CanonicalCode(ERRCODE_DATA_EXCEPTION),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(CanonicalCode(ERRCODE_STATEMENT_TOO_COMPLEX),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(CanonicalCode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            absl::StatusCode::kOutOfRange);
  EXPECT_EQ(CanonicalCode(0), absl::StatusCode::kUnknown);
}

}  // namespace

}  // namespace error
}  // namespace spanner
