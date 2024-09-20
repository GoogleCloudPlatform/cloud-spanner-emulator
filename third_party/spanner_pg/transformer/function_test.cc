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

#include <memory>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/transformer/expr_transformer_helper.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer_test.h"
#include "third_party/spanner_pg/util/postgres.h"

namespace postgres_translator::spangres::test {
namespace {

using ::zetasql::types::DoubleType;
using ::zetasql::types::Int64Type;
using ::zetasql::types::StringType;
using ::postgres_translator::internal::PostgresCastNodeTemplate;
using ::postgres_translator::internal::PostgresCastToExpr;
using ::zetasql_base::testing::StatusIs;

using FunctionPair =
    std::pair<FuncExpr*, std::unique_ptr<zetasql::ResolvedFunctionCall>>;
using FunctionPairVector = std::vector<FunctionPair>;
using OperatorPair =
    std::pair<OpExpr*, std::unique_ptr<zetasql::ResolvedFunctionCall>>;
using OperatorPairVector = std::vector<OperatorPair>;

Expr* MakeIntConst(Oid int_type) {
  auto const_status = internal::makeScalarConst(int_type, Int16GetDatum(1),
                                                /*constisnull=*/false);
  ABSL_CHECK_OK(const_status);
  return PostgresCastToExpr(*const_status);
}

std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>
MakeInt64LiteralArgument() {
  std::unique_ptr<const zetasql::ResolvedExpr> literal =
      zetasql::MakeResolvedLiteral(
          zetasql::types::Int64Type(),
          zetasql::Value::Int64(17'000'000'000'000));
  std::vector<std::unique_ptr<const zetasql::ResolvedExpr>> arguments;
  arguments.emplace_back(std::move(literal));
  return arguments;
}

std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>
MakeDoubleLiteralArgument() {
  std::unique_ptr<const zetasql::ResolvedExpr> literal =
      zetasql::MakeResolvedLiteral(
          zetasql::types::DoubleType(),
          zetasql::Value::Double(3.141592653589793));
  std::vector<std::unique_ptr<const zetasql::ResolvedExpr>> arguments;
  arguments.emplace_back(std::move(literal));
  return arguments;
}

std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>
MakeStringLiteralList(std::vector<absl::string_view> arguments) {
  std::vector<std::unique_ptr<const zetasql::ResolvedExpr>> literal_list;
  for (absl::string_view arg : arguments) {
    literal_list.emplace_back(zetasql::MakeResolvedLiteral(
        zetasql::types::StringType(), zetasql::Value::String(arg)));
  }
  return literal_list;
}

std::unique_ptr<zetasql::ResolvedFunctionCall> MakeResolvedFunctionCall(
    const zetasql::Function* function,
    const zetasql::FunctionSignature* signature,
    std::vector<std::unique_ptr<const zetasql::ResolvedExpr>> arguments) {
  return zetasql::MakeResolvedFunctionCall(
      signature->result_type().type(), function, *signature,
      std::move(arguments),
      zetasql::ResolvedFunctionCallBase::DEFAULT_ERROR_MODE);
}

class FunctionTransformerTest : public TransformerTest {
 public:
  // The function pairs use the thread-local Postgres Memory Context to generate
  // Postgres objects, so they need to be re-populated for each test
  void SetUp() override {
    TransformerTest::SetUp();

    // The function pairs do not take ownership of the zetasql::Function*
    // so they must be initialized separately and live until the test ends.
    std::vector<zetasql::FunctionSignature> sign_signature = {
        {DoubleType(), {DoubleType()}, /*context_ptr=*/nullptr}};
    sign_function_ = std::make_unique<zetasql::Function>(
        "sign", "ZetaSQL", zetasql::Function::SCALAR, sign_signature);
    std::vector<zetasql::FunctionSignature> floor_signature = {
        {DoubleType(), {DoubleType()}, /*context_ptr=*/nullptr}};
    floor_function_ = std::make_unique<zetasql::Function>(
        "floor", "ZetaSQL", zetasql::Function::SCALAR, floor_signature);
    PopulateFunctionPairs();

    std::vector<zetasql::FunctionSignature> concat_signatures = {
        {StringType(),    // Return type.
         {StringType(),   // Input type #1. Required.
          {StringType(),  // Input type #2. Repeated with one occurence
           zetasql::FunctionArgumentType::REPEATED, 1}},
         /*context_ptr=*/nullptr}};
    concat_function_ = std::make_unique<zetasql::Function>(
        "concat", "ZetaSQL", zetasql::Function::SCALAR, concat_signatures);
    ABSL_CHECK_OK(PopulateOperatorPairs());
  }

  void TearDown() override {
    sign_function_.reset();
    floor_function_.reset();
    concat_function_.reset();
    TransformerTest::TearDown();
  }

 protected:
  std::unique_ptr<zetasql::Function> sign_function_ = nullptr;
  std::unique_ptr<zetasql::Function> floor_function_ = nullptr;
  std::unique_ptr<zetasql::Function> concat_function_ = nullptr;
  FunctionPairVector function_pairs_;
  OperatorPairVector operator_pairs_;

 private:
  void PopulateFunctionPairs() {
    // sign(double) <-> sign(double)
    Const* pi_const;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        pi_const,
        internal::makeScalarConst(FLOAT8OID, Float8GetDatum(3.141592653589793),
                                  /*constisnull=*/false));
    FuncExpr* sign_func;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        sign_func,
        internal::makeFuncExpr(2310, FLOAT8OID,
                               list_make1(PostgresCastToExpr(pi_const)),
                               COERCE_EXPLICIT_CALL));
    function_pairs_.emplace_back(
        sign_func, MakeResolvedFunctionCall(sign_function_.get(),
                                            sign_function_->GetSignature(0),
                                            MakeDoubleLiteralArgument()));
    // floor(double) <-> floor(double)
    Const* pi_const2;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        pi_const2,
        internal::makeScalarConst(FLOAT8OID, Float8GetDatum(3.141592653589793),
                                  /*constisnull=*/false));
    FuncExpr* floor_func;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        floor_func,
        internal::makeFuncExpr(2309, FLOAT8OID,
                               list_make1(PostgresCastToExpr(pi_const2)),
                               COERCE_EXPLICIT_CALL));
    function_pairs_.emplace_back(
        floor_func, MakeResolvedFunctionCall(floor_function_.get(),
                                             floor_function_->GetSignature(0),
                                             MakeDoubleLiteralArgument()));
  }

  absl::Status PopulateOperatorPairs() {
    // textcat(text) <-> concat(string)
    ZETASQL_ASSIGN_OR_RETURN(
        auto hello_const,
        internal::makeStringConst(TEXTOID, "hello ", /*constisnull=*/false));
    ZETASQL_ASSIGN_OR_RETURN(auto world_const,
                     internal::makeStringConst(TEXTOID, "world",
                                               /*constisnull=*/false));
    ZETASQL_ASSIGN_OR_RETURN(
        auto expr,
        internal::makeOpExpr(654, 1258, TEXTOID,
                             list_make2(PostgresCastToExpr(hello_const),
                                        PostgresCastToExpr(world_const))));
    operator_pairs_.emplace_back(
        expr, MakeResolvedFunctionCall(
                  concat_function_.get(), concat_function_->GetSignature(0),
                  MakeStringLiteralList({"hello ", "world"})));
    return absl::OkStatus();
  }
};

TEST_F(FunctionTransformerTest, PgToGsqlFunctions) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  for (const auto& [pg_func, gsql_func] : function_pairs_) {
    ASSERT_NE(gsql_func, nullptr);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<zetasql::ResolvedFunctionCall> result,
        forward_transformer_->BuildGsqlResolvedFunctionCall(
            *pg_func, &expr_transformer_info));
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->DebugString(), gsql_func->DebugString());
  }
}

TEST_F(FunctionTransformerTest, PgToGsqlOperators) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  for (const auto& [pg_op, gsql_func] : operator_pairs_) {
    ASSERT_NE(gsql_func, nullptr);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<zetasql::ResolvedFunctionCall> result,
        forward_transformer_->BuildGsqlResolvedFunctionCall(
            *pg_op, &expr_transformer_info));
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->DebugString(), gsql_func->DebugString());
  }
}

TEST_F(FunctionTransformerTest, PgToGsqlFunctionNotFound) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  // pg_stat_get_bgwriter_timed_checkpoints is not a function Spangres will
  // ever support because the PostgreSQL backend is not run.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FuncExpr* pg_stat_get_bgwriter_timed_checkpoints,
      internal::makeFuncExpr(2769, INT8OID,
                             /*args=*/nullptr, COERCE_EXPLICIT_CALL));
  EXPECT_THAT(
      forward_transformer_->BuildGsqlResolvedFunctionCall(
          *pg_stat_get_bgwriter_timed_checkpoints, &expr_transformer_info),
      StatusIs(absl::StatusCode::kUnimplemented,
               testing::HasSubstr("checkpoints() is not supported")));
}

TEST_F(FunctionTransformerTest, PgToGsqlFunctionSignatureNotFound) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  // Abs is a supported function, but the signature with int2 types will not be
  // supported in Spangres.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FuncExpr* abs_int2,
      internal::makeFuncExpr(1398, INT2OID, list_make1(MakeIntConst(INT2OID)),
                             COERCE_EXPLICIT_CALL));
  EXPECT_THAT(forward_transformer_->BuildGsqlResolvedFunctionCall(
                  *abs_int2, &expr_transformer_info),
              StatusIs(absl::StatusCode::kUnimplemented,
                       testing::HasSubstr("Type is not supported")));
}
}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
