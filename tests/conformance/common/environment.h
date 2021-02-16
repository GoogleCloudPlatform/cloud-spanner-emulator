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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_ENVIRONMENT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_ENVIRONMENT_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "google/cloud/spanner/connection_options.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// Globals shared by all conformance tests.
struct ConformanceTestGlobals {
  // Identifiers for the Cloud Spanner instance against which tests should run.
  std::string project_id;
  std::string instance_id;

  // True if the tests are running in a production environment.
  bool in_prod_env = false;

  // Client library options for connecting to the Cloud Spanner under test.
  std::unique_ptr<cloud::spanner::ConnectionOptions> connection_options;
};

// Returns the globals shared by all conformance tests.
const ConformanceTestGlobals& GetConformanceTestGlobals();

// Sets the globals shared by all conformance tests.
void SetConformanceTestGlobals(ConformanceTestGlobals* globals);

// Helper to convert the client library's Status to the emulator's Status.
inline absl::Status ToUtilStatus(const google::cloud::Status& status) {
  return absl::Status(static_cast<absl::StatusCode>(status.code()),
                      status.message());
}

// Helper to convert the client library's StatusOr to the emulator's StatusOr.
template <typename T>
zetasql_base::StatusOr<T> ToUtilStatusOr(const google::cloud::StatusOr<T>& status_or) {
  if (status_or.ok()) {
    return zetasql_base::StatusOr<T>(status_or.value());
  } else {
    return ToUtilStatus(status_or.status());
  }
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_ENVIRONMENT_H_
