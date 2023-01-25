//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "frontend/converters/time.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace converters {

namespace {
using ::google::protobuf::Duration;
using ::google::protobuf::Timestamp;
using ::google::spanner::emulator::test::EqualsProto;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

TEST(TimestampToProtoConversionTest, UnixEpoch) {
  EXPECT_THAT(TimestampToProto(absl::UnixEpoch()),
              IsOkAndHolds(EqualsProto(R"pb(seconds: 0 nanos: 0)pb")));
}

TEST(TimestampToProtoConversionTest, UniversalEpoch) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_value,
                       TimestampToProto(absl::UniversalEpoch()));
  EXPECT_LT(proto_value.seconds(), 0);
  EXPECT_EQ(proto_value.nanos(), 0);
}

TEST(TimestampToProtoConversionTest, MinAllowedValue) {
  absl::Time time =
      absl::FromCivil(absl::CivilSecond(1, 1, 1, 0, 0, 0), absl::UTCTimeZone());
  ZETASQL_ASSERT_OK(TimestampToProto(time));
}

TEST(TimestampToProtoConversionTest, MaxAllowedValue) {
  absl::Time time = absl::FromCivil(absl::CivilSecond(9999, 12, 31, 23, 59, 59),
                                    absl::UTCTimeZone());
  ZETASQL_ASSERT_OK(TimestampToProto(time));
}

TEST(TimestampToProtoConversionTest, PositiveTimeInNanoSeconds) {
  absl::Time time = absl::FromUnixMicros(1200000);
  EXPECT_THAT(TimestampToProto(time),
              IsOkAndHolds(EqualsProto(R"pb(seconds: 1 nanos: 200000000)pb")));
}

TEST(TimestampToProtoConversionTest, NegativeTimeInNanoSeconds) {
  absl::Time time = absl::FromUnixMicros(-1200000);
  EXPECT_THAT(TimestampToProto(time),
              IsOkAndHolds(EqualsProto(R"pb(seconds: -2 nanos: 800000000)pb")));
}

TEST(TimestampToProtoConversionTest, LessThanMinAllowedValue) {
  absl::Time time = absl::FromCivil(absl::CivilSecond(1, 1, 1, 0, 0, 0) - 1,
                                    absl::UTCTimeZone());
  EXPECT_THAT(TimestampToProto(time),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TimestampToProtoConversionTest, GreaterThanMaxAllowedValue) {
  absl::Time time = absl::FromCivil(
      absl::CivilSecond(9999, 12, 31, 23, 59, 59) + 1, absl::UTCTimeZone());
  EXPECT_THAT(TimestampToProto(time),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TimestampToProtoConversionTest, InfinitePast) {
  absl::Time time = absl::InfinitePast();
  EXPECT_THAT(TimestampToProto(time),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TimestampToProtoConversionTest, InfiniteFuture) {
  absl::Time time = absl::InfiniteFuture();
  EXPECT_THAT(TimestampToProto(time),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TimestampFromProtoConversionTest, UnixEpoch) {
  Timestamp timestamp;
  timestamp.set_seconds(0);
  timestamp.set_nanos(0);
  EXPECT_THAT(TimestampFromProto(timestamp), IsOkAndHolds(absl::UnixEpoch()));
}

TEST(TimestampFromProtoConversionTest, MinAllowedValue) {
  int64_t seconds = -62135596800;
  Timestamp timestamp;
  timestamp.set_seconds(seconds);
  timestamp.set_nanos(0);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto time, TimestampFromProto(timestamp));
  EXPECT_EQ(time, absl::FromUnixSeconds(seconds));
}

TEST(TimestampFromProtoConversionTest, MaxAllowedValue) {
  int64_t seconds = 253402300799;
  Timestamp timestamp;
  timestamp.set_seconds(seconds);
  timestamp.set_nanos(0);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto time, TimestampFromProto(timestamp));
  EXPECT_EQ(time, absl::FromUnixSeconds(seconds));
}

TEST(TimestampFromProtoConversionTest, LessThanMinAllowedValue) {
  Timestamp timestamp;
  timestamp.set_seconds(-62135596801);
  timestamp.set_nanos(0);
  EXPECT_THAT(TimestampFromProto(timestamp),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TimestampFromProtoConversionTest, GreaterThanMaxAllowedValue) {
  Timestamp timestamp;
  timestamp.set_seconds(253402300800);
  timestamp.set_nanos(0);
  EXPECT_THAT(TimestampFromProto(timestamp),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TimestampFromProtoConversionTest, LessThanMinAllowedValueForNanoSeconds) {
  Timestamp timestamp;
  timestamp.set_seconds(1);
  timestamp.set_nanos(-1);
  EXPECT_THAT(TimestampFromProto(timestamp),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TimestampFromProtoConversionTest,
     GreaterThanMaxAllowedValueForNanoSeconds) {
  Timestamp timestamp;
  timestamp.set_seconds(1);
  timestamp.set_nanos(1000000000);
  EXPECT_THAT(TimestampFromProto(timestamp),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(DurationFromProtoConversionTest, UnixEpoch) {
  Duration duration;
  duration.set_seconds(0);
  duration.set_nanos(0);
  EXPECT_THAT(
      DurationFromProto(duration),
      IsOkAndHolds(absl::time_internal::ToUnixDuration(absl::UnixEpoch())));
}

TEST(DurationFromProtoConversionTest, MinAllowedValue) {
  Duration duration;
  duration.set_seconds(-315576000000);
  duration.set_nanos(-999999999);
  ZETASQL_ASSERT_OK(DurationFromProto(duration));
}

TEST(DurationFromProtoConversionTest, MaxAllowedValue) {
  Duration duration;
  duration.set_seconds(315576000000);
  duration.set_nanos(999999999);
  ZETASQL_ASSERT_OK(DurationFromProto(duration));
}

TEST(DurationFromProtoConversionTest, LessThanMinAllowedValue) {
  Duration duration;
  duration.set_seconds(-315576000001);
  duration.set_nanos(-999999999);
  EXPECT_THAT(DurationFromProto(duration),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(DurationFromProtoConversionTest, GreaterThanMaxAllowedValue) {
  Duration duration;
  duration.set_seconds(315576000001);
  duration.set_nanos(999999999);
  EXPECT_THAT(DurationFromProto(duration),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(DurationFromProtoConversionTest, LessThanMinAllowedValueForNanoSeconds) {
  Duration duration;
  duration.set_seconds(1);
  duration.set_nanos(-1000000000);
  EXPECT_THAT(DurationFromProto(duration),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(DurationFromProtoConversionTest,
     GreaterThanMaxAllowedValueForNanoSeconds) {
  Duration duration;
  duration.set_seconds(1);
  duration.set_nanos(1000000000);
  EXPECT_THAT(DurationFromProto(duration),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace converters
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
