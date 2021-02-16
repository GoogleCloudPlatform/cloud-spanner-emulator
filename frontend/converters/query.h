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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_QUERY_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_QUERY_H_

#include <string>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "zetasql/base/statusor.h"
#include "backend/query/query_engine.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Converts the sql, params into backend query using given type factory.
zetasql_base::StatusOr<backend::Query> QueryFromProto(
    std::string sql, const google::protobuf::Struct& params,
    google::protobuf::Map<std::string, google::spanner::v1::Type> param_types,
    zetasql::TypeFactory* type_factory);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_QUERY_H_
