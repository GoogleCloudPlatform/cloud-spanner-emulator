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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_DATABASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_DATABASE_H_

#include <string>

#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "backend/database/database.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Database represents a database resource within the frontend.
//
// This class provides frontend-level functionality (e.g. URI, proto conversion)
// and wraps the 'backend::Database' class which actually implements the core
// database functionality. We do this to keep the backend database a completely
// separate module from the frontend and isolate it from gRPC API details.
class Database {
 public:
  Database(const std::string& database_uri,
           std::unique_ptr<backend::Database> backend, absl::Time create_time)
      : database_uri_(database_uri),
        backend_(std::move(backend)),
        create_time_(create_time) {}

  // Returns the URI for this database.
  const std::string& database_uri() const { return database_uri_; }

  // Returns the handle to the backend database.
  backend::Database* backend() const { return backend_.get(); }

  // Converts this database object to its proto representation.
  absl::Status ToProto(admin::database::v1::Database* database);

 private:
  // The URI for this database.
  const std::string database_uri_;

  // The backend object which implements core database functionality.
  std::unique_ptr<backend::Database> backend_;

  // The time at which this database was created.
  const absl::Time create_time_;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_DATABASE_H_
