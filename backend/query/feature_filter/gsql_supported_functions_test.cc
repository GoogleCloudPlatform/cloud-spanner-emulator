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

#include "backend/query/feature_filter/gsql_supported_functions.h"

#include <string>

#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/match.h"
#include "backend/query/analyzer_options.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

class GsqlSupportedFunctionsTest : public testing::Test {
 public:
  GsqlSupportedFunctionsTest() = default;

 protected:
  zetasql::TypeFactory type_factory_;
};

TEST_F(GsqlSupportedFunctionsTest, SupportedFunctionsAreOnlyGsqlBuiltins) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_json_type = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  std::map<std::string, std::unique_ptr<zetasql::Function>>
      gsql_builtin_functions;
  zetasql::GetZetaSQLFunctions(
      &type_factory_, MakeGoogleSqlLanguageOptions(), &gsql_builtin_functions);

  // Collect the function names and their aliases.
  absl::flat_hash_set<absl::string_view> gsql_functions;
  for (const auto& [name, fn] : gsql_builtin_functions) {
    gsql_functions.insert(name);
    if (!fn->alias_name().empty()) {
      gsql_functions.insert(fn->alias_name());
    }
  }

  // Check that the SupportedZetaSQLFunctions set contains only
  // ZetaSQL built-in functions and no Cloud Spanner specific ones.
  for (const auto& function_name : *SupportedZetaSQLFunctions()) {
    SCOPED_TRACE(absl::StrCat("Function: ", function_name));
    EXPECT_NE(gsql_functions.find(function_name), gsql_functions.end());
  }
  // Do the same check for JSON functions.
  for (const auto& function_name : *SupportedJsonFunctions()) {
    SCOPED_TRACE(absl::StrCat("Function: ", function_name));
    EXPECT_NE(gsql_functions.find(function_name), gsql_functions.end());
  }
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
