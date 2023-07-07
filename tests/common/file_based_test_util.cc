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

#include "tests/common/file_based_test_util.h"

#include <string>

#include "zetasql/base/logging.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "tools/cpp/runfiles/runfiles.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

std::string GetTestFileDir(const std::string& path) {
  std::string error;
  auto runfiles = bazel::tools::cpp::runfiles::Runfiles::CreateForTest(&error);
  if (!error.empty()) {
    ZETASQL_LOG(WARNING) << "Error when fetching runfiles: " << error;
    return "";
  }
  return runfiles->Rlocation(path);
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
