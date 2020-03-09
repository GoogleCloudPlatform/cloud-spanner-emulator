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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_VALUE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_VALUE_H_

#include "zetasql/public/value.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// We reuse the zetasql::Value class from ZetaSQL, but it does not provide
// operator overloads as part of the library so we define them in our namespace
// for convenience.
inline bool operator==(const zetasql::Value& v1, const zetasql::Value& v2) {
  return v1.Equals(v2);
}
inline bool operator<(const zetasql::Value& v1, const zetasql::Value& v2) {
  return v1.LessThan(v2);
}

// ValueList is a simple array of Values.
typedef std::vector<zetasql::Value> ValueList;

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_VALUE_H_
