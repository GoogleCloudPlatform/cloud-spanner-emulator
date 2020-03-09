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

#include "frontend/common/uris.h"

#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace {

bool ConsumeResource(const absl::string_view expected_prefix,
                     absl::string_view* resource_uri,
                     absl::string_view* resource_id) {
  if (!absl::ConsumePrefix(resource_uri, expected_prefix)) {
    return false;
  }
  auto pos = resource_uri->find('/');
  *resource_id = resource_uri->substr(0, pos);
  absl::ConsumePrefix(resource_uri, absl::StrCat(*resource_id, "/"));
  return true;
}

bool ConsumeProject(absl::string_view* resource_uri,
                    absl::string_view* project_id) {
  return ConsumeResource("projects/", resource_uri, project_id);
}

bool ConsumeInstanceConfig(absl::string_view* resource_uri,
                           absl::string_view* instance_config_id) {
  return ConsumeResource("instanceConfigs/", resource_uri, instance_config_id);
}

bool ConsumeInstance(absl::string_view* resource_uri,
                     absl::string_view* instance_id) {
  return ConsumeResource("instances/", resource_uri, instance_id);
}

bool ConsumeDatabase(absl::string_view* resource_uri,
                     absl::string_view* database_id) {
  return ConsumeResource("databases/", resource_uri, database_id);
}

bool ConsumeSession(absl::string_view* resource_uri,
                    absl::string_view* session_id) {
  return ConsumeResource("sessions/", resource_uri, session_id);
}

bool ConsumeOperation(absl::string_view* resource_uri,
                      absl::string_view* operation_id) {
  return ConsumeResource("operations/", resource_uri, operation_id);
}

}  // namespace

zetasql_base::Status ParseProjectUri(absl::string_view resource_uri,
                             absl::string_view* project_id) {
  if (!ConsumeProject(&resource_uri, project_id)) {
    return error::InvalidProjectURI(resource_uri);
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ParseInstanceConfigUri(absl::string_view resource_uri,
                                    absl::string_view* project_id,
                                    absl::string_view* instance_config_id) {
  if (!ConsumeProject(&resource_uri, project_id)) {
    return error::InvalidProjectURI(resource_uri);
  }
  if (!ConsumeInstanceConfig(&resource_uri, instance_config_id)) {
    return error::InvalidInstanceConfigURI(resource_uri);
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ParseInstanceUri(absl::string_view resource_uri,
                              absl::string_view* project_id,
                              absl::string_view* instance_id) {
  if (!ConsumeProject(&resource_uri, project_id)) {
    return error::InvalidProjectURI(resource_uri);
  }
  if (!ConsumeInstance(&resource_uri, instance_id)) {
    return error::InvalidInstanceURI(resource_uri);
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ParseDatabaseUri(absl::string_view resource_uri,
                              absl::string_view* project_id,
                              absl::string_view* instance_id,
                              absl::string_view* database_id) {
  if (!ConsumeProject(&resource_uri, project_id)) {
    return error::InvalidProjectURI(resource_uri);
  }
  if (!ConsumeInstance(&resource_uri, instance_id)) {
    return error::InvalidInstanceURI(resource_uri);
  }
  if (!ConsumeDatabase(&resource_uri, database_id)) {
    return error::InvalidDatabaseURI(resource_uri);
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ParseSessionUri(absl::string_view resource_uri,
                             absl::string_view* project_id,
                             absl::string_view* instance_id,
                             absl::string_view* database_id,
                             absl::string_view* session_id) {
  if (!ConsumeProject(&resource_uri, project_id)) {
    return error::InvalidProjectURI(resource_uri);
  }
  if (!ConsumeInstance(&resource_uri, instance_id)) {
    return error::InvalidInstanceURI(resource_uri);
  }
  if (!ConsumeDatabase(&resource_uri, database_id)) {
    return error::InvalidDatabaseURI(resource_uri);
  }
  if (!ConsumeSession(&resource_uri, session_id)) {
    return error::InvalidSessionURI(resource_uri);
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ParseOperationUri(absl::string_view operation_uri,
                               absl::string_view* resource_uri,
                               absl::string_view* operation_id) {
  absl::string_view project_id, instance_id, database_id;
  if (!ConsumeProject(&operation_uri, &project_id)) {
    return error::InvalidProjectURI(operation_uri);
  }
  if (!ConsumeInstance(&operation_uri, &instance_id)) {
    return error::InvalidInstanceURI(operation_uri);
  }
  // Operations may be performed on an instance, or a database. Call
  // ConsumeDatabase to remove "databases/<database_id>" if exists. Proceed
  // regardless of the returned value.
  if (ConsumeDatabase(&operation_uri, &database_id)) {
    *resource_uri = absl::string_view(
        MakeDatabaseUri(MakeInstanceUri(project_id, instance_id), database_id));
  } else {
    *resource_uri = absl::string_view(MakeInstanceUri(project_id, instance_id));
  }

  if (!ConsumeOperation(&operation_uri, operation_id)) {
    return error::InvalidOperationURI(operation_uri);
  }
  return zetasql_base::OkStatus();
}

std::string MakeProjectUri(absl::string_view project_id) {
  return absl::StrCat("projects/", project_id);
}

std::string MakeInstanceConfigUri(absl::string_view project_id,
                                  absl::string_view instance_config_id) {
  return absl::StrCat("projects/", project_id, "/instanceConfigs/",
                      instance_config_id);
}

std::string MakeInstanceUri(absl::string_view project_id,
                            absl::string_view instance_id) {
  return absl::StrCat("projects/", project_id, "/instances/", instance_id);
}

std::string MakeDatabaseUri(absl::string_view instance_uri,
                            absl::string_view database_id) {
  return absl::StrCat(instance_uri, "/databases/", database_id);
}

std::string MakeSessionUri(absl::string_view database_uri,
                           absl::string_view session_id) {
  return absl::StrCat(database_uri, "/sessions/", session_id);
}

std::string MakeOperationUri(absl::string_view resource_uri,
                             absl::string_view operation_id) {
  return absl::StrCat(resource_uri, "/operations/", operation_id);
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
