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

#include "frontend/converters/query.h"

#include <map>

#include "zetasql/public/type.h"
#include "zetasql/base/statusor.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

zetasql_base::StatusOr<backend::Query> QueryFromProto(
    std::string sql, const google::protobuf::Struct& params,
    google::protobuf::Map<std::string, google::spanner::v1::Type> param_types,
    zetasql::TypeFactory* type_factory) {
  std::map<std::string, zetasql::Value> declared;
  std::map<std::string, google::protobuf::Value> undeclared;
  for (const auto& [name, proto_value] : params.fields()) {
    auto param_type_iter = param_types.find(name);
    if (param_type_iter == param_types.end()) {
      // Defer parsing this value until the backend has performed type analysis.
      undeclared[name] = proto_value;
    } else {
      const spanner::v1::Type& proto_type = param_type_iter->second;
      const zetasql::Type* type;
      ZETASQL_RETURN_IF_ERROR(TypeFromProto(proto_type, type_factory, &type));
      ZETASQL_ASSIGN_OR_RETURN(declared[name], ValueFromProto(proto_value, type));
    }
  }
  return backend::Query{sql, std::move(declared), std::move(undeclared)};
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
