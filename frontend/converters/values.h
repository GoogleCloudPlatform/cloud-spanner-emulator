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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_VALUES_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_VALUES_H_

#include "google/protobuf/struct.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/base/statusor.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Parses a ZetaSQL value from a Cloud Spanner value proto.
//
// Only handles the value types supported by Cloud Spanner. The proto encoding
// does not fully specify types (e.g. int64s are encoded as strings, and null
// value encodings do not indicate their type). Therefore, to parse correctly,
// we need additional context and pass in the expected type of the value.
//
// Unexpected types, and mismatches between expected type and proto type will
// return errors.
zetasql_base::StatusOr<zetasql::Value> ValueFromProto(
    const google::protobuf::Value& value_pb, const zetasql::Type* type);

// Converts a ZetaSQL value to a Cloud Spanner value proto.
//
// Only handles the value types supported by Cloud Spanner. Invalid values, and
// value types not supported by Cloud Spanner will return errors.
zetasql_base::StatusOr<google::protobuf::Value> ValueToProto(
    const zetasql::Value& value);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_VALUES_H_
