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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_CASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_CASE_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// The following function objects can be used to create case-insensitive hash
// containers. A typical use case is the hash container for tables / columns
// with the table / column name as the key.
//
// Usage:
//     absl::flat_hash_set<std::string, CaseInsensitiveHash,
//                         CaseInsensitiveEqual> table_names;
//     table_names.insert("my-table");  // Will succeed.
//     table_names.insert("My-TaBlE");  // Will fail - entry already exist.
//
// Note that for hash containers created like this, only the member functions of
// the hash container (e.g. find, count, insert, etc) uses the case-insensitive
// function objects. The keys in the container is case-sensitive strings. If the
// key string is directly compared with another string, the comparison is still
// case sensitive. For example:
//
//     absl::flat_hash_set<std::string, CaseInsensitiveHash,
//                         CaseInsensitiveEqual> table_names;
//     table_names.insert("my-table");
//     for (auto& table : table_names) {
//       if (table.first == "My-TaBlE) {  // this condition is never true.
//         ...
//       }
//     }
//
// A hash function object for performing case-insensitive hash on strings.
struct CaseInsensitiveHash {
  size_t operator()(const std::string& keyval) const {
    std::string s = keyval;
    absl::AsciiStrToLower(&s);
    return std::hash<std::string>()(s);
  }
};

// A comparator function object for performing case-insensitive equal
// comparison on strings.
struct CaseInsensitiveEqual {
  bool operator()(const std::string& left, const std::string& right) const {
    return absl::EqualsIgnoreCase(left, right);
  }
};

// A convenient alias for flat_hash_map with case-insensitive strings as the
// keys.
template <typename ValueType>
using CaseInsensitiveStringMap =
    absl::flat_hash_map<std::string, ValueType, CaseInsensitiveHash,
                        CaseInsensitiveEqual>;

using CaseInsensitiveStringSet =
    absl::flat_hash_set<std::string, CaseInsensitiveHash, CaseInsensitiveEqual>;

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_CASE_H_
