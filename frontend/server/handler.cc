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

#include "frontend/server/handler.h"

#include <map>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

// Singleton registry of all handlers in the system.
//
// The registry is initialized at static initialization time and never modified
// after that (i.e. handlers should not be added dynamically).
class HandlerRegistry {
 public:
  // Adds a handler to the registry.
  void AddHandler(std::unique_ptr<GRPCHandlerBase> handler) {
    absl::MutexLock lock(&mu_);
    handler_map_[handler->service_name() + "." + handler->method_name()] =
        std::move(handler);
  }

  // Retrieves a handler from the registry.
  GRPCHandlerBase* GetHandler(const std::string& service_name,
                              const std::string& method_name) {
    absl::MutexLock lock(&mu_);
    auto itr = handler_map_.find(service_name + "." + method_name);
    if (itr == handler_map_.end()) {
      return nullptr;
    }
    return itr->second.get();
  }

 private:
  // Mutex to guard state below.
  absl::Mutex mu_;

  // Map of all handlers registered by the registration framework.
  absl::flat_hash_map<std::string, std::unique_ptr<GRPCHandlerBase>>
      handler_map_ ABSL_GUARDED_BY(mu_);
};

// Returns a singleton instance of the handler registry.
HandlerRegistry* GetHandlerRegistry() {
  static HandlerRegistry* registry = new HandlerRegistry();
  return registry;
}

}  // namespace

HandlerRegisterer::HandlerRegisterer(std::unique_ptr<GRPCHandlerBase> handler) {
  GetHandlerRegistry()->AddHandler(std::move(handler));
}

GRPCHandlerBase* GetHandler(const std::string& service_name,
                            const std::string& method_name) {
  return GetHandlerRegistry()->GetHandler(service_name, method_name);
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
