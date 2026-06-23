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
#include <string>
#include <utility>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "googlesql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/query/query_engine.h"
#include "backend/schema/catalog/proto_bundle.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

TEST(QueryConverterTest, QueryFromProtoCompiles) {
  googlesql::TypeFactory type_factory;
  auto proto_bundle = backend::ProtoBundle::CreateEmpty();
  auto shared_proto_bundle =
      std::shared_ptr<const backend::ProtoBundle>(std::move(proto_bundle));
  auto result = QueryFromProto("SELECT 1", {}, {}, &type_factory,
                               shared_proto_bundle, {});
  GOOGLESQL_ASSERT_OK(result.status());

  const auto& query = result.value();
  EXPECT_EQ(query.sql, "SELECT 1");
  EXPECT_TRUE(query.secure_context.empty());
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
