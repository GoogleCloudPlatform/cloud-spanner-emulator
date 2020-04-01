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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_TYPES_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_TYPES_H_

#include "zetasql/public/type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Returns true if 'type' is a supported column data type.
bool IsSupportedColumnType(const zetasql::Type* type);

// Returns true if 'type' is a supported key column data type.
bool IsSupportedKeyColumnType(const zetasql::Type* type);

// Returns the string representation of the type.
std::string ToString(const zetasql::Type* type);

// Returns 'type' if type is a supported scalar type or 'type's
// element type if type is an array type. Returns nullptr otherwise.
const zetasql::Type* BaseType(const zetasql::Type* type);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_TYPES_H_
