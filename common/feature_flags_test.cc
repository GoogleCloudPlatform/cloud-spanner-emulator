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

#include "common/feature_flags.h"

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

TEST(EmulatorFeatureFlags, Basic) {
  const EmulatorFeatureFlags& features = EmulatorFeatureFlags::instance();

  EXPECT_TRUE(features.flags().enable_check_constraint);
  EXPECT_TRUE(features.flags().enable_column_default_values);
  EXPECT_TRUE(features.flags().enable_generated_pk);
  EXPECT_TRUE(features.flags().enable_fk_delete_cascade_action);
  EXPECT_TRUE(features.flags().enable_bit_reversed_positive_sequences);
  EXPECT_TRUE(
      features.flags().enable_bit_reversed_positive_sequences_postgresql);
  EXPECT_TRUE(features.flags().enable_upsert_queries);
  EXPECT_TRUE(features.flags().enable_upsert_queries_with_returning);
  EXPECT_FALSE(features.flags().enable_user_defined_functions);
  EXPECT_TRUE(features.flags().enable_fk_enforcement_option);
  EXPECT_TRUE(features.flags().enable_interleave_in);
  EXPECT_TRUE(features.flags().enable_insert_on_conflict_dml);

  {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_check_constraint = false;
    flags.enable_column_default_values = false;
    flags.enable_generated_pk = false;
    flags.enable_fk_delete_cascade_action = false;
    flags.enable_bit_reversed_positive_sequences = false;
    flags.enable_bit_reversed_positive_sequences_postgresql = false;
    flags.enable_upsert_queries = false;
    flags.enable_upsert_queries_with_returning = false;
    flags.enable_fk_enforcement_option = false;
    flags.enable_insert_on_conflict_dml = false;

    test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    EXPECT_FALSE(features.flags().enable_check_constraint);
    EXPECT_FALSE(features.flags().enable_column_default_values);
    EXPECT_FALSE(features.flags().enable_generated_pk);
    EXPECT_FALSE(features.flags().enable_fk_delete_cascade_action);
    EXPECT_FALSE(features.flags().enable_bit_reversed_positive_sequences);
    EXPECT_FALSE(
        features.flags().enable_bit_reversed_positive_sequences_postgresql);
    EXPECT_FALSE(features.flags().enable_upsert_queries);
    EXPECT_FALSE(features.flags().enable_upsert_queries_with_returning);
    EXPECT_FALSE(features.flags().enable_fk_enforcement_option);
    EXPECT_FALSE(features.flags().enable_insert_on_conflict_dml);
  }
  EXPECT_TRUE(features.flags().enable_check_constraint);
  EXPECT_TRUE(features.flags().enable_column_default_values);
  EXPECT_TRUE(features.flags().enable_generated_pk);
  EXPECT_TRUE(features.flags().enable_fk_delete_cascade_action);
  EXPECT_TRUE(features.flags().enable_bit_reversed_positive_sequences);
  EXPECT_TRUE(
      features.flags().enable_bit_reversed_positive_sequences_postgresql);
  EXPECT_TRUE(features.flags().enable_upsert_queries);
  EXPECT_TRUE(features.flags().enable_upsert_queries_with_returning);
  EXPECT_TRUE(features.flags().enable_fk_enforcement_option);
  EXPECT_TRUE(features.flags().enable_insert_on_conflict_dml);
}

TEST(EmulatorFeatureFlags, ProtosFlag) {
  const EmulatorFeatureFlags& features = EmulatorFeatureFlags::instance();

  EXPECT_TRUE(features.flags().enable_protos);

  {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_protos = false;
    test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    EXPECT_FALSE(features.flags().enable_protos);
  }

  EXPECT_TRUE(features.flags().enable_protos);
}

TEST(EmulatorFeatureFlags, DmlReturningFlag) {
  const EmulatorFeatureFlags& features = EmulatorFeatureFlags::instance();

  EXPECT_TRUE(features.flags().enable_dml_returning);

  {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_dml_returning = false;
    test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    EXPECT_FALSE(features.flags().enable_dml_returning);
  }

  EXPECT_TRUE(features.flags().enable_dml_returning);
}

TEST(EmulatorFeatureFlags, PostgresqlInterfaceFlag) {
  const EmulatorFeatureFlags& features = EmulatorFeatureFlags::instance();

  EXPECT_TRUE(features.flags().enable_postgresql_interface);

  {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_postgresql_interface = false;
    test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    EXPECT_FALSE(features.flags().enable_postgresql_interface);
  }

  EXPECT_TRUE(features.flags().enable_postgresql_interface);
}

TEST(EmulatorFeatureFlags, SearchIndexFlag) {
  const EmulatorFeatureFlags& features = EmulatorFeatureFlags::instance();

  EXPECT_TRUE(features.flags().enable_search_index);

  {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_search_index = false;
    test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    EXPECT_FALSE(features.flags().enable_search_index);
  }

  EXPECT_TRUE(features.flags().enable_search_index);
}

TEST(EmulatorFeatureFlags, HiddenColumnFlag) {
  const EmulatorFeatureFlags& features = EmulatorFeatureFlags::instance();

  EXPECT_TRUE(features.flags().enable_hidden_column);

  {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_hidden_column = false;
    test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    EXPECT_FALSE(features.flags().enable_hidden_column);
  }

  EXPECT_TRUE(features.flags().enable_hidden_column);
}

}  // namespace
}  // namespace emulator
}  // namespace spanner
}  // namespace google
