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

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "common/errors.h"
#include "common/limits.h"
#include "frontend/collections/database_manager.h"
#include "frontend/common/labels.h"
#include "frontend/common/uris.h"
#include "frontend/entities/database.h"
#include "frontend/entities/session.h"
#include "frontend/server/environment.h"
#include "frontend/server/handler.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace spanner_api = ::google::spanner::v1;
namespace protobuf_api = ::google::protobuf;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Creates a new session.
absl::Status CreateSession(RequestContext* ctx,
                           const spanner_api::CreateSessionRequest* request,
                           spanner_api::Session* response) {
  // Validate the request.
  absl::string_view project_id, instance_id, database_id;
  ZETASQL_RETURN_IF_ERROR(ParseDatabaseUri(request->database(), &project_id,
                                   &instance_id, &database_id));
  ZETASQL_RETURN_IF_ERROR(ValidateLabels(request->session().labels()));

  // Check that the instance is valid.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Instance> instance,
                   GetInstance(ctx, MakeInstanceUri(project_id, instance_id)));

  // Fetch the database.
  ZETASQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Database> database,
      ctx->env()->database_manager()->GetDatabase(request->database()));

  // Create a session.
  Labels labels(request->session().labels().begin(),
                request->session().labels().end());
  ZETASQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Session> session,
      ctx->env()->session_manager()->CreateSession(labels, database));

  // Return details about the newly created session.
  return session->ToProto(response, /*include_labels=*/true);
}
REGISTER_GRPC_HANDLER(Spanner, CreateSession);

// Creates a batch of new sessions.
absl::Status BatchCreateSessions(
    RequestContext* ctx, const spanner_api::BatchCreateSessionsRequest* request,
    spanner_api::BatchCreateSessionsResponse* response) {
  // Validate the request.
  absl::string_view project_id, instance_id, database_id;
  ZETASQL_RETURN_IF_ERROR(ParseDatabaseUri(request->database(), &project_id,
                                   &instance_id, &database_id));
  ZETASQL_RETURN_IF_ERROR(ValidateLabels(request->session_template().labels()));
  if (request->session_count() < 0) {
    return error::TooFewSessions(request->session_count());
  }

  // Check that the instance is valid.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Instance> instance,
                   GetInstance(ctx, MakeInstanceUri(project_id, instance_id)));

  // Fetch the database to ensure that it exists.
  ZETASQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Database> database,
      ctx->env()->database_manager()->GetDatabase(request->database()));

  // Silently truncate requested session count to max allowed session count.
  const int32_t actual_session_count =
      std::min(limits::kMaxBatchCreateSessionsCount, request->session_count());

  // Create the requested sessions.
  std::vector<std::shared_ptr<Session>> sessions(actual_session_count);
  Labels labels(request->session_template().labels().begin(),
                request->session_template().labels().end());
  for (int i = 0; i < sessions.size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(sessions[i], ctx->env()->session_manager()->CreateSession(
                                      labels, database));
  }

  // Return details about the newly created session.
  for (const auto& session : sessions) {
    ZETASQL_RETURN_IF_ERROR(session->ToProto(response->add_session()));
  }
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, BatchCreateSessions);

// Gets information about a particular session.
absl::Status GetSession(RequestContext* ctx,
                        const spanner_api::GetSessionRequest* request,
                        spanner_api::Session* response) {
  absl::string_view project_id, instance_id, database_id, session_id;
  ZETASQL_RETURN_IF_ERROR(ParseSessionUri(request->name(), &project_id, &instance_id,
                                  &database_id, &session_id));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   ctx->env()->session_manager()->GetSession(request->name()));
  return session->ToProto(response, /*include_labels=*/false);
}
REGISTER_GRPC_HANDLER(Spanner, GetSession);

// Lists all sessions in a given database that match the specified filter.
absl::Status ListSessions(RequestContext* ctx,
                          const spanner_api::ListSessionsRequest* request,
                          spanner_api::ListSessionsResponse* response) {
  // Validate the request.
  absl::string_view project_id, instance_id, database_id;
  ZETASQL_RETURN_IF_ERROR(ParseDatabaseUri(request->database(), &project_id,
                                   &instance_id, &database_id));

  // Check that the instance is valid.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Instance> instance,
                   GetInstance(ctx, MakeInstanceUri(project_id, instance_id)));

  // Fetch the database to ensure that it exists.
  ZETASQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Database> database,
      ctx->env()->database_manager()->GetDatabase(request->database()));

  // List all sessions for the given database.
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::shared_ptr<Session>> sessions,
      ctx->env()->session_manager()->ListSessions(database->database_uri()));

  int32_t page_size = request->page_size();
  static const int32_t kMaxPageSize = 1000;
  if (page_size <= 0 || page_size > kMaxPageSize) {
    page_size = kMaxPageSize;
  }

  // Sessions returned from session manager are sorted by session_uri and
  // thus we use first session uri after requested page size as next_page_token.
  for (const auto& session : sessions) {
    if (response->sessions_size() >= page_size) {
      response->set_next_page_token(session->session_uri());
      break;
    }
    if (session->session_uri() >= request->page_token()) {
      ZETASQL_RETURN_IF_ERROR(session->ToProto(response->add_sessions(),
                                       /*include_labels=*/true));
    }
  }
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, ListSessions);

// Ends a session, releasing server resources associated with it.
absl::Status DeleteSession(RequestContext* ctx,
                           const spanner_api::DeleteSessionRequest* request,
                           protobuf_api::Empty* response) {
  absl::string_view project_id, instance_id, database_id, session_id;
  ZETASQL_RETURN_IF_ERROR(ParseSessionUri(request->name(), &project_id, &instance_id,
                                  &database_id, &session_id));
  return ctx->env()->session_manager()->DeleteSession(request->name());
}
REGISTER_GRPC_HANDLER(Spanner, DeleteSession);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
