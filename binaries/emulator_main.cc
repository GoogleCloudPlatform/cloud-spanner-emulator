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
#include <csignal>
#include <memory>

#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "googlesql/base/logging.h"
#include "absl/strings/str_cat.h"
#include "common/config.h"
#include "frontend/server/server.h"

using Server = ::google::spanner::emulator::frontend::Server;

namespace {
Server* g_server = nullptr;

void SignalHandler(int signal) {
  ABSL_LOG(INFO) << "Received signal " << signal << ", shutting down.";
  if (g_server) {
    g_server->Shutdown();
  }
}
}  // namespace

int main(int argc, char** argv) {
  absl::SetProgramUsageMessage(
      "Cloud Spanner Emulator\n"
      "\n"
      "A local emulator for Cloud Spanner that runs entirely on your machine.\n"
      "\n"
      "Usage:\n"
      "  emulator_main [flags]\n"
      "\n"
      "Common flags:\n"
      "  --host_port=HOST:PORT\n"
      "      Address to serve gRPC requests on (default: localhost:10007).\n"
      "\n"
      "  --data_dir=PATH\n"
      "      Directory for persisting emulator state across restarts.\n"
      "      When empty (default), the emulator runs in pure in-memory\n"
      "      mode and all data is lost on shutdown.\n"
      "\n"
      "  --snapshot_interval_secs=SECONDS\n"
      "      Interval between periodic snapshots (default: 3600 = 1 hour).\n"
      "      Set to 0 to disable periodic snapshots.\n"
      "\n"
      "  --enable_fault_injection\n"
      "      Enable fault injection for testing application error handling.\n"
      "\n"
      "  --log_requests\n"
      "      Stream gRPC request/response messages to the INFO log.\n");

  // Start the emulator gRPC server.
  absl::ParseCommandLine(argc, argv);
  Server::Options options;
  options.server_address = google::spanner::emulator::config::grpc_host_port();
  options.data_dir = google::spanner::emulator::config::data_dir();
  options.snapshot_interval_secs =
      google::spanner::emulator::config::snapshot_interval_secs();
  std::unique_ptr<Server> server = Server::Create(options);
  if (!server) {
    ABSL_LOG(ERROR) << "Failed to start gRPC server.";
    return EXIT_FAILURE;
  }

  // Install signal handlers for graceful shutdown (saves persistent state).
  g_server = server.get();
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  ABSL_LOG(INFO) << "Cloud Spanner Emulator running.";
  ABSL_LOG(INFO) << "Server address: "
            << absl::StrCat(server->host(), ":", server->port());

  // Block forever until the server is terminated.
  server->WaitForShutdown();

  return EXIT_SUCCESS;
}
