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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_GSQL_SUPPORTED_FUNCTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_GSQL_SUPPORTED_FUNCTIONS_H_

#include "zetasql/public/function.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"

namespace google::spanner::emulator::backend {

// Returns the list of ZetaSQL built-in functions that are supported in Cloud
// Spanner.
const absl::flat_hash_set<absl::string_view>* SupportedZetaSQLFunctions();
// Returns the list of JSON-related ZetaSQL built-in functions that are
// supported in Cloud Spanner.
const absl::flat_hash_set<absl::string_view>* SupportedJsonFunctions();

// Returns true if the function is a supported ZetaSQL builtin function.
bool IsSupportedZetaSQLFunction(const zetasql::Function& function);

// Returns true if the function is a supported ZetaSQL JSON builtin function.
// This will be merged with IsSupportedZetaSQLFunction above after JSON is
// fully enabled.
// TODO: Merge back into IsSupportedZetaSQLFunction.
bool IsSupportedJsonFunction(const zetasql::Function& function);

}  // namespace google::spanner::emulator::backend

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_GSQL_SUPPORTED_FUNCTIONS_H_
