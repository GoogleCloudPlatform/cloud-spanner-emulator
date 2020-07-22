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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_OPTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_OPTIONS_H_

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Options that can be used to configure behavior of the query engine.
struct QueryEngineOptions {
  // If true, will disable partitionability check in PartitionQuery API.
  bool disable_query_partitionability_check = false;

  // If true, will disable checks to determine if a NULL_FILTERED index can be
  // used to answer a SQL query.
  bool disable_query_null_filtered_index_check = false;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_OPTIONS_H_
