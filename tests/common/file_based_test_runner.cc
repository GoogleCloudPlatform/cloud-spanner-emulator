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
#include <string>
#include <utility>
#include <vector>

#include "google/rpc/code.pb.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/strip.h"
#include "tests/common/file_based_test_util.h"
#include "re2/re2.h"

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

}  // namespace

std::vector<FileBasedTestCase> ReadTestCasesFromFile(
    const std::string& file,
    const FileBasedTestOptions& options
) {
  std::vector<FileBasedTestCase> test_cases;
  FileBasedTestCase test_case(file,
                              1
  );
  std::string line;
  int line_no = 0;
  ParserState state = ParserState::kReadingInput;

  auto add_test_case = [&]() {
    test_case.expected.expect_error =
        absl::StartsWith(test_case.expected.text, "ERROR:");
    if (test_case.expected.expect_error) {
      // If the expected string also contains the status code then extract that
      // separately.
      absl::string_view expected_textv = test_case.expected.text;
      ASSERT_TRUE(absl::ConsumePrefix(&expected_textv, "ERROR:"));
      expected_textv = absl::StripLeadingAsciiWhitespace(expected_textv);
      std::string status_message = std::string(expected_textv);

      std::string status_code_str;
      google::rpc::Code status_code;
      if (RE2::Consume(&expected_textv, "([A-Z_]+):", &status_code_str) &&
          google::rpc::Code_Parse(status_code_str, &status_code)) {
        test_case.expected.status_code =
            static_cast<absl::StatusCode>(status_code);
        expected_textv = absl::StripLeadingAsciiWhitespace(expected_textv);
        status_message = std::string(expected_textv);
      }
      // Re-attach the message prefix.
      test_case.expected.text = status_message;
    }
    test_cases.emplace_back(std::move(test_case));
    state = ParserState::kReadingInput;
    test_case = FileBasedTestCase(file,
                                  line_no + 1
    );
  };

  // Process the input file a line at a time.
  std::ifstream fin(file);
  while (std::getline(fin, line)) {
    ++line_no;
    switch (state) {
      case ParserState::kReadingInput:
        if (line == options.test_case_delimiter) {
          // The output text is expected to equal the input text.
          test_case.expected.text = test_case.input.text;
          add_test_case();
        } else if (line == options.input_delimiter) {
          // We have seen the end of the input, move on to reading output.
          state = ParserState::kReadingOutput;
        } else if (absl::StartsWith(line, options.comment_prefix)) {
          // Skip lines with comments and evaluate flags.
          if (absl::StrContains(line, options.regex_flag)) {
            test_case.input.regex = true;
          }
        } else {
          // Add this line to the test case.
          test_case.input.text += line + "\n";
        }
        break;

      case ParserState::kReadingOutput:
        if (line == options.test_case_delimiter) {
          add_test_case();
        } else {
          // We are still consuming the test case output.
          test_case.expected.text += line + "\n";
        }
        break;
    }
  }

  // Add the final test case if any.
  if (state == ParserState::kReadingOutput || !test_case.input.text.empty()) {
    add_test_case();
  }
  return test_cases;
}

std::string GetRunfilesDir(const std::string& dir) {
  return GetTestFileDir(
      absl::StrCat("com_google_cloud_spanner_emulator", "/", dir));
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
