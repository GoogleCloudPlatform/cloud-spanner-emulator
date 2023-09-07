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

#include "third_party/spanner_pg/util/integral_helpers.h"

#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

namespace {

using testing::Eq;
using testing::StrEq;
using zetasql_base::testing::StatusIs;

constexpr int32_t kInt32Min = std::numeric_limits<int32_t>::min();
constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();

constexpr int32_t kInt32Max = std::numeric_limits<int32_t>::max();
constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();

TEST(SafeAddTest, ReturnsSumWhenThereIsNoOverflow) {
  int32_t result;

  ZETASQL_EXPECT_OK(SafeAdd(-1, 1, &result));
  EXPECT_THAT(result, Eq(0));

  ZETASQL_EXPECT_OK(SafeAdd(kInt32Min, 0, &result));
  EXPECT_THAT(result, Eq(kInt32Min));

  ZETASQL_EXPECT_OK(SafeAdd(kInt32Max, 0, &result));
  EXPECT_THAT(result, Eq(kInt32Max));

  ZETASQL_EXPECT_OK(SafeAdd(kInt32Max - 1, 1, &result));
  EXPECT_THAT(result, Eq(kInt32Max));
}

TEST(SafeAddTest, ReturnsErroWhenSumUnderflows) {
  int32_t result;

  EXPECT_THAT(SafeAdd(-1, kInt32Min, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("underflow")));
  EXPECT_THAT(SafeAdd(0, kInt64Min, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("underflow")));
  EXPECT_THAT(SafeAdd(-1, kInt64Min, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("underflow")));
}

TEST(SafeAddTest, ReturnsErroWhenSumOverflows) {
  int32_t result;

  EXPECT_THAT(SafeAdd(kInt32Max, 1, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("overflow")));
  EXPECT_THAT(SafeAdd(0, static_cast<int64_t>(kInt32Max) + 1, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("overflow")));
  EXPECT_THAT(SafeAdd(0, kInt64Max, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("overflow")));
}

TEST(SafeSubtract, ReturnsSubtractionWhenThereIsNoUnderflow) {
  int32_t result;

  ZETASQL_EXPECT_OK(SafeSubtract(1, 1, &result));
  EXPECT_THAT(result, Eq(0));

  ZETASQL_EXPECT_OK(SafeSubtract(kInt32Max, 0, &result));
  EXPECT_THAT(result, Eq(kInt32Max));

  ZETASQL_EXPECT_OK(SafeSubtract(kInt32Min, 0, &result));
  EXPECT_THAT(result, Eq(kInt32Min));

  ZETASQL_EXPECT_OK(SafeSubtract(static_cast<int64_t>(kInt32Min) + 1, 1, &result));
  EXPECT_THAT(result, Eq(kInt32Min));
}

TEST(SafeSubtract, ReturnsErroWhenSubtractionUnderflows) {
  int32_t result;

  EXPECT_THAT(SafeSubtract(kInt32Min, 1, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("underflow")));
  EXPECT_THAT(SafeSubtract(0, static_cast<int64_t>(kInt32Max) + 2, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("underflow")));
  EXPECT_THAT(SafeSubtract(0, kInt64Max, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("underflow")));
}

TEST(SafeSubtract, ReturnsErroWhenSubtractionOverflows) {
  int32_t result;

  EXPECT_THAT(SafeSubtract(kInt32Max, -1, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("overflow")));
  EXPECT_THAT(SafeSubtract(0, kInt32Min, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("overflow")));
  EXPECT_THAT(SafeSubtract(0, kInt64Min, &result),
              StatusIs(absl::StatusCode::kOutOfRange, StrEq("overflow")));
}

}  // namespace
