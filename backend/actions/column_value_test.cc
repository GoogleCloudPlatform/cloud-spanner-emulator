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

#include "backend/actions/column_value.h"

#include <memory>
#include <string>

#include "zetasql/public/functions/string.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/types/variant.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "common/limits.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using emulator::test::ScopedEmulatorFeatureFlagsSetter;
using zetasql::values::Bool;
using zetasql::values::Bytes;
using zetasql::values::BytesArray;
using zetasql::values::Date;
using zetasql::values::Double;
using zetasql::values::Int64;
using zetasql::values::NullBool;
using zetasql::values::NullBytes;
using zetasql::values::NullDate;
using zetasql::values::NullDouble;
using zetasql::values::NullInt64;
using zetasql::values::NullNumeric;
using zetasql::values::NullString;
using zetasql::values::NullTimestamp;
using zetasql::values::Numeric;
using zetasql::values::String;
using zetasql::values::StringArray;
using zetasql::values::Timestamp;
using zetasql_base::testing::StatusIs;

struct Values {
  zetasql::Value int64_col = Int64(1);
  zetasql::Value bool_col = Bool(true);
  zetasql::Value date_col = Date(2);
  zetasql::Value float_col = Double(2.0);
  zetasql::Value string_col = String("test");
  zetasql::Value bytes_col = Bytes("01234");
  zetasql::Value timestamp_col = Timestamp(absl::Now());
  zetasql::Value array_string_col = StringArray({"test"});
  zetasql::Value array_bytes_col = BytesArray({"01234"});
  zetasql::Value numeric_col =
      Numeric(zetasql::NumericValue::FromStringStrict("1234567890.123456789")
                  .value());
};

class ColumnValueTest : public test::ActionsTest {
 public:
  ColumnValueTest()
      : scoped_flags_setter_({.enable_stored_generated_columns = false}),
        schema_(emulator::test::CreateSchemaFromDDL(
                    {
                        R"(
                            CREATE TABLE TestTable (
                              int64_col INT64 NOT NULL,
                              bool_col BOOL NOT NULL,
                              date_col DATE NOT NULL,
                              float_col FLOAT64 NOT NULL,
                              string_col STRING(MAX) NOT NULL,
                              bytes_col BYTES(MAX) NOT NULL,
                              timestamp_col TIMESTAMP NOT NULL,
                              array_string_col ARRAY<STRING(MAX)>,
                              array_bytes_col ARRAY<BYTES(MAX)>,
                              numeric_col NUMERIC NOT NULL,
                            ) PRIMARY KEY (int64_col)
                          )",
                    },
                    &type_factory_)
                    .value()),
        table_(schema_->FindTable("TestTable")),
        base_columns_(table_->columns()),
        validator_(absl::make_unique<ColumnValueValidator>()) {}

  absl::Status ValidateInsert(const Values& values) {
    return validator_->Validate(
        ctx(), Insert(table_, Key({Int64(1)}), base_columns_,
                      {values.int64_col, values.bool_col, values.date_col,
                       values.float_col, values.string_col, values.bytes_col,
                       values.timestamp_col, values.array_string_col,
                       values.array_bytes_col, values.numeric_col}));
  }

  absl::Status ValidateUpdate(const Values& values) {
    return validator_->Validate(
        ctx(), Update(table_, Key({Int64(1)}), base_columns_,
                      {values.int64_col, values.bool_col, values.date_col,
                       values.float_col, values.string_col, values.bytes_col,
                       values.timestamp_col, values.array_string_col,
                       values.array_bytes_col, values.numeric_col}));
  }

 protected:
  // Test components.
  ScopedEmulatorFeatureFlagsSetter scoped_flags_setter_;
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Table* table_;
  absl::Span<const Column* const> base_columns_;
  std::unique_ptr<Validator> validator_;
};

TEST_F(ColumnValueTest, ValidateNotNullColumns) {
  Values values;
  ZETASQL_EXPECT_OK(ValidateInsert(values));
  {
    Values values;
    values.int64_col = NullInt64();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.bool_col = NullBool();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.date_col = NullDate();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.float_col = NullDouble();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.string_col = NullString();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.bytes_col = NullBytes();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.timestamp_col = NullTimestamp();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.numeric_col = NullNumeric();
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST_F(ColumnValueTest, ValidateColumnsAreCorrectTypes) {
  Values values;
  ZETASQL_EXPECT_OK(ValidateInsert(values));
  {
    Values values;
    values.int64_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.bool_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.date_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.float_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.string_col = Int64(3);
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.bytes_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.timestamp_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.numeric_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.array_string_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    Values values;
    values.array_bytes_col = String("");
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST_F(ColumnValueTest, ValidateKeyDoesNotContainNullValuesForDelete) {
  ZETASQL_EXPECT_OK(validator_->Validate(ctx(), Delete(table_, Key({Int64(1)}))));

  EXPECT_THAT(validator_->Validate(ctx(), Delete(table_, Key({NullInt64()}))),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ColumnValueTest, ValidateStringLength) {
  Values values;
  std::string max(limits::kMaxStringColumnLength, '0');
  std::string exceed_max(limits::kMaxStringColumnLength + 1, '0');

  {
    values.string_col = String(max);
    ZETASQL_EXPECT_OK(ValidateInsert(values));
    ZETASQL_EXPECT_OK(ValidateUpdate(values));
  }
  {
    values.string_col = String(exceed_max);
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST_F(ColumnValueTest, ValidateBytesLength) {
  Values values;
  std::string max(limits::kMaxBytesColumnLength, '0');
  std::string exceed_max(limits::kMaxBytesColumnLength + 1, '0');

  {
    values.bytes_col = Bytes(max);
    ZETASQL_EXPECT_OK(ValidateInsert(values));
    ZETASQL_EXPECT_OK(ValidateUpdate(values));
  }
  {
    values.bytes_col = Bytes(exceed_max);
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST_F(ColumnValueTest, ValidateArrayStringLength) {
  Values values;
  std::string max(limits::kMaxStringColumnLength, '0');
  std::string exceed_max(limits::kMaxStringColumnLength + 1, '0');

  {
    values.array_string_col = StringArray({max, max, max});
    ZETASQL_EXPECT_OK(ValidateInsert(values));
    ZETASQL_EXPECT_OK(ValidateUpdate(values));
  }
  {
    values.array_string_col = StringArray({exceed_max, exceed_max, exceed_max});
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST_F(ColumnValueTest, ValidateArrayBytesLength) {
  Values values;
  std::string max(limits::kMaxBytesColumnLength, '0');
  std::string exceed_max(limits::kMaxBytesColumnLength + 1, '0');

  {
    values.array_bytes_col = BytesArray({max, max, max});
    ZETASQL_EXPECT_OK(ValidateInsert(values));
    ZETASQL_EXPECT_OK(ValidateUpdate(values));
  }
  {
    values.array_bytes_col = BytesArray({exceed_max, exceed_max, exceed_max});
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST_F(ColumnValueTest, ValidateUTF8StringEncoding) {
  auto buf = absl::make_unique<char[]>(limits::kMaxStringColumnLength * 4);
  std::vector<int64_t> code_points(limits::kMaxStringColumnLength);
  absl::BitGen gen;
  for (int i = 0; i < limits::kMaxStringColumnLength; ++i) {
    char32_t character = absl::Uniform<char32_t>(gen, 0x0, 0x10FFFF);
    // Ignore subset of characters that are invalid for UTF8 encoding.
    while (character > 0xD7FF && character < 0xE000) {
      character = absl::Uniform<char32_t>(gen, 0x0, 0x10FFFF);
    }
    code_points[i] = character;
  }

  // Random valid max length encoding.
  {
    std::string encoded_str;
    absl::Status error;
    zetasql::functions::CodePointsToString(code_points, &encoded_str, &error);
    EXPECT_EQ(error, absl::OkStatus());
    Values values;
    values.string_col = String(encoded_str);
    ZETASQL_EXPECT_OK(ValidateInsert(values));
    ZETASQL_EXPECT_OK(ValidateUpdate(values));
  }

  // Check all 0's
  std::fill(code_points.begin(), code_points.end(), 0);
  {
    std::string encoded_str;
    absl::Status error;
    zetasql::functions::CodePointsToString(code_points, &encoded_str, &error);
    EXPECT_EQ(error, absl::OkStatus());
    Values values;
    values.string_col = String(encoded_str);
    ZETASQL_EXPECT_OK(ValidateInsert(values));
    ZETASQL_EXPECT_OK(ValidateUpdate(values));
  }

  // Check some invalid encodings
  memset(buf.get(), 0x80, limits::kMaxStringColumnLength * 4);
  {
    Values values;
    std::string str(buf.get(), limits::kMaxStringColumnLength * 4);
    values.string_col = String(str);
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  memset(buf.get(), 0xF0, limits::kMaxStringColumnLength * 4);
  {
    Values values;
    std::string str(buf.get(), limits::kMaxStringColumnLength * 4);
    values.string_col = String(str);
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  memset(buf.get(), 0xFF, limits::kMaxStringColumnLength * 4);
  {
    Values values;
    std::string str(buf.get(), limits::kMaxStringColumnLength * 4);
    values.string_col = String(str);
    EXPECT_THAT(ValidateInsert(values),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(ValidateUpdate(values),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
