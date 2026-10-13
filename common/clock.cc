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

#include <algorithm>

#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

// Returns the current time at microsecond granularity.
absl::Time NowMicros() {
  return absl::FromUnixMicros(absl::ToUnixMicros(absl::Now()));
}

}  // namespace

Clock::Clock()
    : last_system_time_(NowMicros()), last_dispensed_time_(last_system_time_) {}

absl::Time Clock::Now() {
  absl::MutexLock lock(mu_);

  // Follows the system clock, and steps one microsecond past the last value
  // only while the system clock has not moved beyond it. Adding the elapsed
  // system time to the last value instead keeps every such step for good, and
  // a strong read waits for the system clock to reach its read timestamp.
  absl::Time now = NowMicros();
  last_system_time_ = now;
  last_dispensed_time_ =
      std::max(now, last_dispensed_time_ + absl::Microseconds(1));

  return last_dispensed_time_;
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
