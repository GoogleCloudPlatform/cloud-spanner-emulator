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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_FEATURE_FLAGS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_FEATURE_FLAGS_H_

#include "absl/synchronization/mutex.h"

namespace google {
namespace spanner {
namespace emulator {

// Global singleton flags that control feature availability. This may be
// used to control the availabiliy of features in development.
//
// This class is thread safe.
class EmulatorFeatureFlags {
 public:
  struct Flags {
    bool enable_stored_generated_columns = true;
    bool enable_check_constraint = true;
  };

  static const EmulatorFeatureFlags& instance() {
    static const EmulatorFeatureFlags* instance = new EmulatorFeatureFlags();
    return *instance;
  }

  const Flags& flags() const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::ReaderMutexLock l(&mu_);
    return flags_;
  }

  void set_flags(const Flags& flags) ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock l(&mu_);
    flags_ = flags;
  }

 private:
  EmulatorFeatureFlags() = default;
  Flags flags_ ABSL_GUARDED_BY(mu_);
  mutable absl::Mutex mu_;
};

}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_FEATURE_FLAGS_H_
