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

#include "common/clock.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

TEST(Clock, ClockReturnsIncreasingValues) {
  Clock clock;
  absl::Time t1 = clock.Now();
  absl::Time t2 = clock.Now();

  EXPECT_GT(t2, t1);
}

TEST(Clock, ClockReturnsValuesAtMicrosecondGranularity) {
  Clock clock;
  absl::Time t1 = clock.Now();
  EXPECT_EQ(t1, absl::FromUnixMicros(absl::ToUnixMicros(t1)));
}

TEST(Clock, ClockReturnsToTheSystemClockAfterABurst) {
  Clock clock;
  for (int i = 0; i < 20000; ++i) {
    clock.Now();
  }
  absl::SleepFor(absl::Milliseconds(100));

  absl::Time clock_now = clock.Now();
  absl::Time sys_now = absl::Now();
  EXPECT_LE(clock_now, sys_now);
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
