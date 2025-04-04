//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/jsonb_array_elements_table_valued_function.h"

#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_value.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "third_party/spanner_pg/shims/error_shim.h"

namespace postgres_translator {

namespace {

using ::postgres_translator::interfaces::CreatePGArena;
using ::postgres_translator::interfaces::PGArena;
using ::postgres_translator::spangres::datatypes::
    CreatePgJsonbValueFromNormalized;
using ::postgres_translator::spangres::datatypes::GetPgJsonbNormalizedValue;
using ::postgres_translator::spangres::datatypes::common::jsonb::PgJsonbValue;
using ::postgres_translator::spangres::datatypes::common::jsonb::TreeNode;

// The emulator implementation of jsonb_array_elements.
class JsonbArrayElementsEvaluator : public zetasql::EvaluatorTableIterator {
 public:
  JsonbArrayElementsEvaluator(zetasql::Value input,
                              const zetasql::TVFSchemaColumn& output_column)
      : input_(std::move(input)), output_column_(output_column) {}

  int NumColumns() const override { return 1; }

  std::string GetColumnName(int i) const override {
    ABSL_DCHECK_EQ(i, 0);
    return output_column_.name;
  }

  const zetasql::Type* GetColumnType(int i) const override {
    ABSL_DCHECK_EQ(i, 0);
    return output_column_.type;
  }

  const zetasql::Value& GetValue(int i) const override {
    ABSL_DCHECK_EQ(i, 0);
    return output_;
  }

  absl::Status Status() const override { return absl::OkStatus(); }

  absl::Status Cancel() override { ZETASQL_RET_CHECK_FAIL() << "Unimplemented"; }

  absl::Status Init() {
    output_index_ = 0;
    if (input_.is_null()) {
      return absl::OkStatus();
    }
    // Set up the PG memory context arena in case we call into native PG code.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
        CreatePGArena(nullptr));

    // Parse the input.
    ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(input_));
    std::vector<std::unique_ptr<TreeNode>> tree_nodes;
    ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                     PgJsonbValue::Parse(std::string(jsonb), &tree_nodes));
    ZETASQL_ASSIGN_OR_RETURN(jsonb_array_, jsonb_value.GetSerializedArrayElements());

    return absl::OkStatus();
  }

  bool NextRow() override {
    if (output_index_ >= jsonb_array_.size()) {
      return false;
    }
    output_ = CreatePgJsonbValueFromNormalized(jsonb_array_[output_index_]);
    output_index_++;
    return true;
  }

 private:
  const zetasql::Value input_;
  const zetasql::TVFSchemaColumn& output_column_;
  std::vector<absl::Cord> jsonb_array_;
  int output_index_;
  zetasql::Value output_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
JsonbArrayElementsTableValuedFunction::CreateEvaluator(
    std::vector<TvfEvaluatorArg> input_arguments,
    const std::vector<zetasql::TVFSchemaColumn>& output_columns,
    const zetasql::FunctionSignature* function_call_signature) const {
  ZETASQL_RET_CHECK_EQ(input_arguments.size(), 1);
  ZETASQL_RET_CHECK_EQ(output_columns.size(), 1);

  ZETASQL_RET_CHECK(input_arguments[0].value);
  zetasql::Value input = *input_arguments[0].value;

  auto evaluator = std::make_unique<JsonbArrayElementsEvaluator>(
      std::move(input), std::move(output_columns[0]));
  ZETASQL_RETURN_IF_ERROR(evaluator->Init());
  return std::move(evaluator);
}

}  // namespace postgres_translator
