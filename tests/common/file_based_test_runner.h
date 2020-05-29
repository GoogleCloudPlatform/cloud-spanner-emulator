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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_FILE_BASED_TEST_RUNNER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_FILE_BASED_TEST_RUNNER_H_

#include <functional>
#include <string>

#include "absl/status/status.h"
#include "zetasql/base/statusor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// Options for the file based test runner.
struct FileBasedTestOptions {
  // Delimiter for the entire test case.
  std::string test_case_delimiter = "==";

  // Delimiter for the input portion of the test case.
  std::string input_delimiter = "--";

  // Prefix for comments.
  std::string comment_prefix = "# ";
};

// Input for a file based test case.
struct FileBasedTestCaseInput {
  std::string text;
};

// Output for a file based test case.
struct FileBasedTestCaseOutput {
  std::string text;
};

// Callback (impemented by test suites) which runs a file based test case.
// Executors should return invalid status for invalid test case inputs. If the
// test case input is valid, any errors from running the test case should be
// formatted as text into the output.
using FileBasedTestCaseExecutor =
    std::function<zetasql_base::StatusOr<FileBasedTestCaseOutput>(
        const FileBasedTestCaseInput&)>;

// Runs all test cases in `file` via `executor`.
absl::Status RunTestCasesFromFile(const std::string& file,
                                  const FileBasedTestOptions& options,
                                  const FileBasedTestCaseExecutor& executor);

// Returns the runfiles directory for the given source-root relative directory.
std::string GetRunfilesDir(const std::string& dir);

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_FILE_BASED_TEST_RUNNER_H_
