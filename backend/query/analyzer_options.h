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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANALYZER_OPTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANALYZER_OPTIONS_H_

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/language_options.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using admin::database::v1::DatabaseDialect;

zetasql::AnalyzerOptions MakeGoogleSqlAnalyzerOptions();

zetasql::LanguageOptions MakeGoogleSqlLanguageOptions();

zetasql::LanguageOptions MakeGoogleSqlLanguageOptionsForCompliance();

zetasql::AnalyzerOptions MakeGoogleSqlAnalyzerOptionsForViews(
    DatabaseDialect dialect = DatabaseDialect::GOOGLE_STANDARD_SQL);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANALYZER_OPTIONS_H_
