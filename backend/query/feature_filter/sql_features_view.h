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


#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_SQL_FEATURES_VIEW_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_SQL_FEATURES_VIEW_H_

namespace google::spanner::emulator::backend {

class SqlFeaturesView {
 public:
  SqlFeaturesView() = default;
  SqlFeaturesView(const SqlFeaturesView&) = delete;
  SqlFeaturesView& operator=(const SqlFeaturesView&) = delete;

  bool enable_error_function() const { return true; }
  bool enable_supported_optimizer_version_function() const { return true; }
  bool enable_stddev() const { return true; }
  bool enable_variance() const { return true; }
  bool iso_date_parts() const { return true; }
};

}  // namespace google::spanner::emulator::backend

#endif
