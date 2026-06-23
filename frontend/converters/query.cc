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
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/type.pb.h"
#include "googlesql/public/type.h"
#include "googlesql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

absl::StatusOr<backend::Query> QueryFromProto(
    std::string sql, const google::protobuf::Struct& params,
    google::protobuf::Map<std::string, google::spanner::v1::Type> param_types,
    googlesql::TypeFactory* type_factory,
    std::shared_ptr<const backend::ProtoBundle> proto_bundle,
    const google::protobuf::Map<std::string, google::protobuf::Value>& secure_context) {
  std::map<std::string, googlesql::Value> declared;
  std::map<std::string, google::protobuf::Value> undeclared;
  for (const auto& [name, proto_value] : params.fields()) {
    auto param_type_iter = param_types.find(name);
    if (param_type_iter == param_types.end()) {
      // Defer parsing this value until the backend has performed type analysis.
      undeclared[name] = proto_value;
    } else {
      const spanner::v1::Type& proto_type = param_type_iter->second;
      const googlesql::Type* type;
      GOOGLESQL_RETURN_IF_ERROR(TypeFromProto(proto_type, type_factory,
                                    &type
                                    ,
                                    proto_bundle
                                    ));
      GOOGLESQL_ASSIGN_OR_RETURN(declared[name], ValueFromProto(proto_value, type));
    }
  }
  absl::flat_hash_map<std::string, google::protobuf::Value> secure_context_map;
  for (const auto& [name, value] : secure_context) {
    if (!value.has_string_value() && !value.has_null_value()) {
      return absl::InvalidArgumentError(
          "Secure parameters must be string or null values.");
    }
    secure_context_map.insert({name, value});
  }
  return backend::Query{sql, std::move(declared), std::move(undeclared),
                        std::move(secure_context_map),
                        /*change_stream_internal_lookup=*/std::nullopt};
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
