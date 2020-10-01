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

#include "backend/schema/updater/schema_updater_tests/base.h"

#include "zetasql/base/statusor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

zetasql_base::StatusOr<std::unique_ptr<const Schema>> SchemaUpdaterTest::CreateSchema(
    absl::Span<const std::string> statements) {
  return UpdateSchema(/*base_schema=*/nullptr, statements);
}

zetasql_base::StatusOr<std::unique_ptr<const Schema>> SchemaUpdaterTest::UpdateSchema(
    const Schema* base_schema, absl::Span<const std::string> statements) {
  SchemaUpdater updater;
  SchemaChangeContext context{.type_factory = &type_factory_,
                              .table_id_generator = &table_id_generator_,
                              .column_id_generator = &column_id_generator_};
  return updater.ValidateSchemaFromDDL(statements, context, base_schema);
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
