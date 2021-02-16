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

#include <algorithm>
#include <memory>

#include "absl/flags/parse.h"
#include "zetasql/base/logging.h"
#include "absl/strings/str_cat.h"
#include "common/config.h"
#include "frontend/server/server.h"

using Server = ::google::spanner::emulator::frontend::Server;

int main(int argc, char** argv) {
  // Start the emulator gRPC server.
  absl::ParseCommandLine(argc, argv);
  Server::Options options;
  options.server_address = google::spanner::emulator::config::grpc_host_port();
  std::unique_ptr<Server> server = Server::Create(options);
  if (!server) {
    ZETASQL_LOG(ERROR) << "Failed to start gRPC server.";
    return EXIT_FAILURE;
  }

  ZETASQL_LOG(INFO) << "Cloud Spanner Emulator running.";
  ZETASQL_LOG(INFO) << "Server address: "
            << absl::StrCat(server->host(), ":", server->port());

  // Block forever until the server is terminated.
  server->WaitForShutdown();

  return EXIT_SUCCESS;
}
