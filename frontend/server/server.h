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

#ifndef STORAGE_SPANNER_CLOUD_EMULATOR_FRONTEND_SERVER_H_
#define STORAGE_SPANNER_CLOUD_EMULATOR_FRONTEND_SERVER_H_

#include <memory>
#include <string>

#include "frontend/server/environment.h"
#include "grpcpp/impl/codegen/service_type.h"
#include "grpcpp/server.h"
#include "grpcpp/support/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Server encapsulates the emulator gRPC server.
//
// Server implements the following services exposed by Cloud Spanner:
// - InstanceAdmin
// - DatabaseAdmin
// - Operations
// - Spanner
//
// For more details on these services, see
//   https://cloud.google.com/spanner/docs/reference/rpc
//
// To manage the complexity of the large number of method handlers, the handlers
// are not implemented as class methods but rather as free-standing functions in
// frontend/handlers, and are registered with the server via a registration
// mechanism (the REGISTER_GRPC_HANDLER macro).
//
// For each incoming request, the server performs some basic processing, bundles
// all state needed by a handler into a RequestContext and dispatches the
// request to its associated free-standing handler function.
//
class Server {
 public:
  struct Options {
    std::string server_address;
  };

  // Returns an initialized Server, or nullptr if the initialization failed.
  static std::unique_ptr<Server> Create(const Options& options);

  std::string host() const { return host_; }
  int port() const { return port_; }

  // Blocks until the server is shut down.
  void WaitForShutdown();

  // Shuts down the grpc server.
  void Shutdown();

  // Accessor to the ServerEnv of the server.
  ServerEnv* env() { return env_.get(); }

 private:
  // Constructor is only used by the factory function
  explicit Server(std::unique_ptr<ServerEnv> env);

  // Address of the gRPC server.
  std::string host_;
  int port_ = -1;

  // Environment shared by all handlers.
  std::unique_ptr<ServerEnv> env_;

  // Services implemented by this gRPC server.
  std::unique_ptr<grpc::Service> database_admin_service_;
  std::unique_ptr<grpc::Service> instance_admin_service_;
  std::unique_ptr<grpc::Service> operations_service_;
  std::unique_ptr<grpc::Service> spanner_service_;

  // Underlying gRPC server.
  std::unique_ptr<grpc::Server> grpc_server_;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // STORAGE_SPANNER_CLOUD_EMULATOR_FRONTEND_SERVER_H_
