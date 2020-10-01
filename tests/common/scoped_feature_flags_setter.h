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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_SCOPED_FEATURE_FLAGS_SETTER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_SCOPED_FEATURE_FLAGS_SETTER_H_

#include "common/feature_flags.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

class ScopedEmulatorFeatureFlagsSetter {
 public:
  explicit ScopedEmulatorFeatureFlagsSetter(
      const EmulatorFeatureFlags::Flags& flags)
      : backup_flags_(EmulatorFeatureFlags::instance().flags()) {
    const_cast<EmulatorFeatureFlags&>(EmulatorFeatureFlags::instance())
        .set_flags(flags);
  }
  ~ScopedEmulatorFeatureFlagsSetter() {
    const_cast<EmulatorFeatureFlags&>(EmulatorFeatureFlags::instance())
        .set_flags(backup_flags_);
  }

 private:
  EmulatorFeatureFlags::Flags backup_flags_;
};

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_SCOPED_FEATURE_FLAGS_SETTER_H_
