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

#include "backend/access/read.h"

#include <sstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

TEST(ReadArg, CanBeStreamedToOStream) {
  ReadArg arg{
      .table = "Hello",
      .index = "World",
      .key_set = KeySet::All(),
      .columns = {"One", "Two"},
  };
  std::stringstream sstr;
  sstr << arg;

  EXPECT_EQ(
      "Table  : 'Hello'\n"
      "Index  : 'World'\n"
      "KeySet : Range[{} ... {∞})\n"
      "Columns: ['One' ,'Two']\n",
      sstr.str());
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
