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
#include <vector>

#include "absl/status/status.h"

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

  // Flag indicating the expected text is a regular expression.
  std::string regex_flag = "--regex";
};

// Input for a file based test case.
struct FileBasedTestCaseInput {
  FileBasedTestCaseInput(absl::string_view file_name, int line_no)
      : file_name(file_name), line_no(line_no) {}
  std::string text;
  bool regex = false;
  std::string file_name;
  int line_no = 0;
};

// Output for a file based test case.
struct FileBasedTestCaseOutput {
  std::string text;
};

// Container for input and exected result of a file-based test case.
struct FileBasedTestCase {
  FileBasedTestCase(absl::string_view file_name, int line_no)
      : input(file_name, line_no) {}
  FileBasedTestCaseInput input;
  FileBasedTestCaseOutput expected;
};

// Reads and returns all testcases in `file`.
std::vector<FileBasedTestCase> ReadTestCasesFromFile(
    const std::string& file, const FileBasedTestOptions& options);

// Returns the runfiles directory for the given source-root relative directory.
std::string GetRunfilesDir(const std::string& dir);

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_FILE_BASED_TEST_RUNNER_H_
