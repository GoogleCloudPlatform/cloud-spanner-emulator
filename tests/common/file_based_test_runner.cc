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

#include "tests/common/file_based_test_runner.h"

#include <fstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/match.h"
#include "tools/cpp/runfiles/runfiles.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

// States that the test case file parser can be in.
enum class ParserState {
  kReadingInput,
  kReadingOutput,
};

// Container for input and exected result of a file based test case.
struct FileBasedTestCase {
  FileBasedTestCaseInput input;
  FileBasedTestCaseOutput expected;
};

}  // namespace

absl::Status RunTestCasesFromFile(const std::string& file,
                                  const FileBasedTestOptions& options,
                                  const FileBasedTestCaseExecutor& executor) {
  FileBasedTestCase test_case;
  std::string line;
  ParserState state = ParserState::kReadingInput;

  // Process the input file a line at a time.
  std::ifstream fin(file);
  while (std::getline(fin, line)) {
    switch (state) {
      case ParserState::kReadingInput:
        if (line == options.input_delimiter) {
          // We have seen the end of the input, move on to reading output.
          state = ParserState::kReadingOutput;
        } else {
          // Add this line to the test case (unless it's a comment).
          if (!absl::StartsWith(line, options.comment_prefix)) {
            test_case.input.text += line + "\n";
          }
        }
        break;

      case ParserState::kReadingOutput:
        if (line == options.test_case_delimiter) {
          // We have seen the end of the test case, execute it.
          zetasql_base::StatusOr<FileBasedTestCaseOutput> actual =
              executor(test_case.input);

          // Assert expectations.
          ZETASQL_EXPECT_OK(actual.status());
          if (actual.status().ok()) {
            EXPECT_EQ(test_case.expected.text, actual.ValueOrDie().text)
                << "for input:\n"
                << test_case.input.text;
          }

          // Reset state for the next test case.
          state = ParserState::kReadingInput;
          test_case = FileBasedTestCase();
        } else {
          // We are still consuming the test case output.
          test_case.expected.text += line + "\n";
        }
        break;
    }
  }

  // Execute the final test case.
  zetasql_base::StatusOr<FileBasedTestCaseOutput> actual = executor(test_case.input);
  ZETASQL_EXPECT_OK(actual.status());
  if (actual.status().ok()) {
    EXPECT_EQ(test_case.expected.text, actual.ValueOrDie().text);
  }

  return absl::OkStatus();
}

std::string GetRunfilesDir(const std::string& dir) {
  std::string error;
  auto runfiles = bazel::tools::cpp::runfiles::Runfiles::CreateForTest(&error);
  if (!error.empty()) {
    LOG(WARNING) << "Error when fetching runfiles: " << error;
    return "";
  }
  return runfiles->Rlocation(
      absl::StrCat("com_google_cloud_spanner_emulator", "/", dir));
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
