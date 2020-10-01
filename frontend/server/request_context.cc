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

#include "frontend/server/request_context.h"

#include "zetasql/base/statusor.h"
#include "frontend/common/uris.h"
#include "frontend/entities/instance.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

zetasql_base::StatusOr<std::shared_ptr<Instance>> GetInstance(
    RequestContext* ctx, const std::string& instance_uri) {
  absl::string_view project_id, instance_id;
  ZETASQL_RETURN_IF_ERROR(ParseInstanceUri(instance_uri, &project_id, &instance_id));
  return ctx->env()->instance_manager()->GetInstance(instance_uri);
}

zetasql_base::StatusOr<std::shared_ptr<Database>> GetDatabase(
    RequestContext* ctx, const std::string& database_uri) {
  absl::string_view project_id, instance_id, database_id;
  ZETASQL_RETURN_IF_ERROR(
      ParseDatabaseUri(database_uri, &project_id, &instance_id, &database_id));
  // Note that databases aren't tied to instances as per the current
  // implementation in emulator. The check below only verifies that the
  // instances do exist for client unit tests to produce correct output.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Instance> instance,
                   GetInstance(ctx, MakeInstanceUri(project_id, instance_id)));
  return ctx->env()->database_manager()->GetDatabase(database_uri);
}

zetasql_base::StatusOr<std::shared_ptr<Session>> GetSession(
    RequestContext* ctx, const std::string& session_uri) {
  // The ParseSessionUri and GetDatabase calls are needed for verification that
  // the session URI and the database for this session is valid, even though
  // they are not used after that.
  absl::string_view project_id, instance_id, database_id, session_id;
  ZETASQL_RETURN_IF_ERROR(ParseSessionUri(session_uri, &project_id, &instance_id,
                                  &database_id, &session_id));
  ZETASQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Database> database,
      GetDatabase(ctx, MakeDatabaseUri(MakeInstanceUri(project_id, instance_id),
                                       database_id)));
  return ctx->env()->session_manager()->GetSession(session_uri);
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
