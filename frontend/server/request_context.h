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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_REQUEST_CONTEXT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_REQUEST_CONTEXT_H_

#include "zetasql/base/statusor.h"
#include "frontend/server/environment.h"
#include "grpcpp/server_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// RequestContext encapsulates the state passed to a gRPC method handler.
class RequestContext {
 public:
  RequestContext(ServerEnv* env, grpc::ServerContext* grpc)
      : env_(env), grpc_(grpc) {}

  // Accessors.
  ServerEnv* env() { return env_; }
  grpc::ServerContext* grpc() { return grpc_; }

 private:
  // Server environment shared by all requests.
  ServerEnv* env_;

  // gRPC context specific to a single request.
  grpc::ServerContext* grpc_;
};

// Checks if an instance exists. Returns the Instance entity or an error:
//   InstanceNotFound if the instance is not found.
zetasql_base::StatusOr<std::shared_ptr<Instance>> GetInstance(
    RequestContext* ctx, const std::string& instance_uri);

// Checks if a database exists. Returns the Database entity or an error:
//   InstanceNotFound if the instance is not found.
//   DatabaseNotFound if the database is not found.
zetasql_base::StatusOr<std::shared_ptr<Database>> GetDatabase(
    RequestContext* ctx, const std::string& database_uri);

// Checks if a session exists. Returns the Session entity or an error:
//   InstanceNotFound if the instance is not found.
//   DatabaseNotFound if the database is not found.
//   SessionNotFound if the session is not found.
zetasql_base::StatusOr<std::shared_ptr<Session>> GetSession(
    RequestContext* ctx, const std::string& session_uri);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_REQUEST_CONTEXT_H_
