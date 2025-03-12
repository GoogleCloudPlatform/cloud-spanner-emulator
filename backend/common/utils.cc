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

#include "backend/common/utils.h"

#include <cstdint>
#include <limits>

#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "re2/re2.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

int64_t ParseSchemaTimeSpec(absl::string_view spec) {
  const int64_t invalid_parse_result = -1;
  int64_t num = invalid_parse_result;
  char modifier = '\0';
  static LazyRE2 time_spec_re = {"(\\d+)([smhd])"};
  constexpr int64_t kint64max = std::numeric_limits<int64_t>::max();
  if (!RE2::FullMatch(spec, *time_spec_re, &num, &modifier)) {
    return -1;
  }

  if (modifier == 's') {
    // RE2 already did overflow checking for us.  NOTE: We explicitly
    // require the 's' modifier so that it will be straightforward to
    // extend the language to support "ms" and "us".
  } else if (modifier == 'm') {
    if (num > kint64max / 60) return -1;
    num *= 60;
  } else if (modifier == 'h') {
    if (num > kint64max / (60 * 60)) return -1;
    num *= (60 * 60);
  } else if (modifier == 'd') {
    if (num > kint64max / (24 * 60 * 60)) return -1;
    num *= (24 * 60 * 60);
  }

  return num;
}

bool IsSystemLocalityGroup(absl::string_view name) {
  return absl::StartsWith(name, "_");
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
