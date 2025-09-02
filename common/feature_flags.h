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
// used to control the availability of features in development.
//
// This class is thread safe.
class EmulatorFeatureFlags {
 public:
  struct Flags {
    bool enable_check_constraint = true;
    bool enable_column_default_values = true;
    bool enable_protos = true;
    bool enable_dml_returning = true;
    bool enable_views = true;
    bool enable_generated_pk = true;
    bool enable_postgresql_interface = true;
    bool enable_fk_delete_cascade_action = true;
    bool enable_bit_reversed_positive_sequences = true;
    bool enable_bit_reversed_positive_sequences_postgresql = true;
    bool enable_upsert_queries = true;
    bool enable_batch_query_with_no_table_scan = true;
    bool enable_upsert_queries_with_returning = true;
    bool enable_identity_columns = true;
    bool enable_serial_auto_increment = true;
    bool enable_user_defined_functions = false;
    bool enable_fk_enforcement_option = true;
    bool enable_search_index = true;
    bool enable_hidden_column = true;
    bool enable_default_time_zone = true;
    bool enable_property_graph_information_schema = true;
    bool enable_interleave_in = true;
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
