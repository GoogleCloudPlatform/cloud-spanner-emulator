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

#include "backend/query/analyzer_options.h"

#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/options.pb.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

TEST(AnalyzerOptionsTest, BaselineLanguageFeaturesEnabled) {
  googlesql::LanguageOptions options = MakeGoogleSqlLanguageOptions();
  EXPECT_TRUE(options.LanguageFeatureEnabled(googlesql::FEATURE_SEQUENCE_ARG));
  EXPECT_TRUE(
      options.LanguageFeatureEnabled(googlesql::FEATURE_NAMED_ARGUMENTS));
  EXPECT_TRUE(options.LanguageFeatureEnabled(
      googlesql::FEATURE_TABLE_VALUED_FUNCTIONS));

  googlesql::AnalyzerOptions analyzer_options = MakeGoogleSqlAnalyzerOptions();
  EXPECT_TRUE(analyzer_options.language().LanguageFeatureEnabled(
      googlesql::FEATURE_SEQUENCE_ARG));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
