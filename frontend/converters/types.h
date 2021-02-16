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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_TYPES_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_TYPES_H_

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/type.h"
#include "absl/status/status.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Parses a ZetaSQL type from a Cloud Spanner type proto.
//
// Only handles the types supported by Cloud Spanner. All types created by this
// function will be owned by the supplied type factory. An unspecified type
// or incorrect type specification will return an appropriate error and leave
// type unchanged.
absl::Status TypeFromProto(const google::spanner::v1::Type& type_pb,
                           zetasql::TypeFactory* factory,
                           const zetasql::Type** type);

// Converts a ZetaSQL type to a Cloud Spanner type proto.
//
// Only handles the types supported by Cloud Spanner. The invalid type, and
// types not supported by Cloud Spanner, will return errors and leave the
// proto unchanged.
absl::Status TypeToProto(const zetasql::Type* type,
                         google::spanner::v1::Type* type_pb);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_TYPES_H_
