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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_URIS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_URIS_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {

// Parses a project URI into its components.
absl::Status ParseProjectUri(absl::string_view resource_uri,
                             absl::string_view* project_id);

// Parses an instance config URI into its components.
absl::Status ParseInstanceConfigUri(absl::string_view resource_uri,
                                    absl::string_view* project_id,
                                    absl::string_view* instance_config_id);

// Parses an instance URI into its components.
absl::Status ParseInstanceUri(absl::string_view resource_uri,
                              absl::string_view* project_id,
                              absl::string_view* instance_id);

// Validates an instance name.
absl::Status ValidateInstanceId(absl::string_view instance_id);

// Parses a database URI into its components.
absl::Status ParseDatabaseUri(absl::string_view resource_uri,
                              absl::string_view* project_id,
                              absl::string_view* instance_id,
                              absl::string_view* database_id);

// Validates a database name.
absl::Status ValidateDatabaseId(absl::string_view database_id);

// Parses a session URI into its components.
absl::Status ParseSessionUri(absl::string_view resource_uri,
                             absl::string_view* project_id,
                             absl::string_view* instance_id,
                             absl::string_view* database_id,
                             absl::string_view* session_id);

// Parses an operation URI into its components.
absl::Status ParseOperationUri(absl::string_view operation_uri,
                               absl::string_view* resource_uri,
                               absl::string_view* operation_id);

// Constructs a project URI from its components.
std::string MakeProjectUri(absl::string_view project_id);

// Constructs an instance config URI from its components.
std::string MakeInstanceConfigUri(absl::string_view project_id,
                                  absl::string_view instance_config_id);

// Constructs an instance URI from its components.
std::string MakeInstanceUri(absl::string_view project_id,
                            absl::string_view instance_id);

// Constructs a database URI from its components.
std::string MakeDatabaseUri(absl::string_view instance_uri,
                            absl::string_view database_id);

// Constructs a session URI from its components.
std::string MakeSessionUri(absl::string_view database_uri,
                           absl::string_view session_id);

// Constructs an operation URI from its components.
std::string MakeOperationUri(absl::string_view resource_uri,
                             absl::string_view operation_id);

// Returns true if `operation_id` is a valid schema change/database creation
// operation ID.
bool IsValidOperationId(absl::string_view operation_id);

}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_URIS_H_
