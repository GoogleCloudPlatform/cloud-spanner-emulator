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

#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/parse_location.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer_test.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::test {
namespace {

using ::postgres_translator::internal::PostgresCastNodeTemplate;
using ::postgres_translator::internal::PostgresCastToExpr;
using ::postgres_translator::internal::PostgresCastToNode;
using ::postgres_translator::spangres::types::PgNumericMapping;
using ::postgres_translator::test::ValidMemoryContext;
using ::zetasql_base::testing::StatusIs;

using ValuePair = std::pair<Expr*, std::unique_ptr<zetasql::ResolvedExpr>>;
using ValuePairVector = std::vector<ValuePair>;

using ExpressionTransformerTest = ::postgres_translator::TransformerTest;

// Returns a numeric `Const` from the given string of numeric value
// (e.g. "3.14").
absl::StatusOr<Const*> MakeNumericConst(absl::string_view value,
                                        bool is_null = false) {
  Datum const_value =
      DirectFunctionCall3(numeric_in, CStringGetDatum(value.data()),
                          ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
  ZETASQL_ASSIGN_OR_RETURN(auto numeric_const,
                   internal::makeScalarConst(NUMERICOID, const_value, is_null));
  return numeric_const;
}

absl::StatusOr<ValuePairVector> ConstValuePairs() {
  ValuePairVector value_pairs;
  // Can't use initializer lists with objects, like unique_ptr,
  // that can't be copy-constructed
  // bool <-> bool
  value_pairs.emplace_back(
      PostgresCastToExpr(makeBoolConst(/*value=*/true, /*isnull=*/false)),
      zetasql::MakeResolvedLiteral(zetasql::types::BoolType(),
                                     zetasql::Value::Bool(true)));
  value_pairs.emplace_back(
      PostgresCastToExpr(makeBoolConst(/*value=*/false, /*isnull=*/true)),
      zetasql::MakeResolvedLiteral(zetasql::types::BoolType(),
                                     zetasql::Value::NullBool()));

  // int8_t <-> int64_t
  ZETASQL_ASSIGN_OR_RETURN(
      auto int8_const,
      internal::makeScalarConst(INT8OID, Int64GetDatum(17'000'000'000'000),
                                /*constisnull=*/false));
  value_pairs.emplace_back(PostgresCastToExpr(int8_const),
                           zetasql::MakeResolvedLiteral(
                               zetasql::types::Int64Type(),
                               zetasql::Value::Int64(17'000'000'000'000)));
  ZETASQL_ASSIGN_OR_RETURN(int8_const,
                   internal::makeScalarConst(INT8OID, Int64GetDatum(0),
                                             /*constisnull=*/true));
  value_pairs.emplace_back(
      PostgresCastToExpr(int8_const),
      zetasql::MakeResolvedLiteral(zetasql::types::Int64Type(),
                                     zetasql::Value::NullInt64()));
  // float8 <-> double
  ZETASQL_ASSIGN_OR_RETURN(
      auto float8_const,
      internal::makeScalarConst(FLOAT8OID, Float8GetDatum(3.141592653589793),
                                /*constisnull=*/false));
  value_pairs.emplace_back(PostgresCastToExpr(float8_const),
                           zetasql::MakeResolvedLiteral(
                               zetasql::types::DoubleType(),
                               zetasql::Value::Double(3.141592653589793)));
  ZETASQL_ASSIGN_OR_RETURN(float8_const,
                   internal::makeScalarConst(FLOAT8OID, Float8GetDatum(0),
                                             /*constisnull=*/true));
  value_pairs.emplace_back(
      PostgresCastToExpr(float8_const),
      zetasql::MakeResolvedLiteral(zetasql::types::DoubleType(),
                                     zetasql::Value::NullDouble()));
  // text <-> string
  ZETASQL_ASSIGN_OR_RETURN(auto string_const,
                   internal::makeStringConst(TEXTOID, "hello world",
                                             /*constisnull=*/false));
  value_pairs.emplace_back(
      PostgresCastToExpr(string_const),
      zetasql::MakeResolvedLiteral(zetasql::types::StringType(),
                                     zetasql::Value::String("hello world")));
  ZETASQL_ASSIGN_OR_RETURN(string_const,
                   internal::makeStringConst(TEXTOID, nullptr,
                                             /*constisnull=*/true));
  value_pairs.emplace_back(
      PostgresCastToExpr(string_const),
      zetasql::MakeResolvedLiteral(zetasql::types::StringType(),
                                     zetasql::Value::NullString()));
  // bytea <-> bytes
  ZETASQL_ASSIGN_OR_RETURN(string_const,
                   internal::makeStringConst(BYTEAOID, "\x01 bytes",
                                             /*constisnull=*/false));
  value_pairs.emplace_back(
      PostgresCastToExpr(string_const),
      zetasql::MakeResolvedLiteral(zetasql::types::BytesType(),
                                     zetasql::Value::Bytes("\x01 bytes")));
  ZETASQL_ASSIGN_OR_RETURN(string_const,
                   internal::makeStringConst(BYTEAOID, nullptr,
                                             /*constisnull=*/true));
  value_pairs.emplace_back(
      PostgresCastToExpr(string_const),
      zetasql::MakeResolvedLiteral(zetasql::types::BytesType(),
                                     zetasql::Value::NullBytes()));

  // numeric <-> PG.NUMERIC
  ZETASQL_ASSIGN_OR_RETURN(auto numeric_const,
                   MakeNumericConst("-13.1357315957913513502000"));

  value_pairs.emplace_back(
      PostgresCastToExpr(numeric_const),
      zetasql::MakeResolvedLiteral(
          PgNumericMapping()->mapped_type(),
          *PgNumericMapping()->MakeGsqlValue(numeric_const)));

  ZETASQL_ASSIGN_OR_RETURN(numeric_const, MakeNumericConst("0", /*is_null=*/true));

  value_pairs.emplace_back(
      PostgresCastToExpr(numeric_const),
      zetasql::MakeResolvedLiteral(
          PgNumericMapping()->mapped_type(),
          *PgNumericMapping()->MakeGsqlValue(numeric_const)));

  return value_pairs;
}

absl::StatusOr<ValuePairVector> ParamValuePairs() {
  ValuePairVector value_pairs;
  ZETASQL_ASSIGN_OR_RETURN(auto bool_param,
                   internal::makeParam(PARAM_EXTERN, /*paramid=*/0, BOOLOID));
  value_pairs.emplace_back(PostgresCastToExpr(bool_param),
                           zetasql::MakeResolvedParameter(
                               zetasql::types::BoolType(),
                               /*name=*/absl::StrCat(kParameterPrefix, 0)));

  ZETASQL_ASSIGN_OR_RETURN(auto int_param,
                   internal::makeParam(PARAM_EXTERN, /*paramid=*/1, INT8OID));
  value_pairs.emplace_back(PostgresCastToExpr(int_param),
                           zetasql::MakeResolvedParameter(
                               zetasql::types::Int64Type(),
                               /*name=*/absl::StrCat(kParameterPrefix, 1)));
  return value_pairs;
}

absl::StatusOr<ValuePairVector> SubLinkValuePairs() {
  ValuePairVector value_pairs;
  auto pg_select1_int64 = []() -> absl::StatusOr<Query*> {
    Query* query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->jointree = makeNode(FromExpr);
    ZETASQL_ASSIGN_OR_RETURN(auto int8_const,
                     internal::makeScalarConst(INT8OID, /*constvalue=*/1,
                                               /*constisnull=*/false));
    TargetEntry* target_entry = makeTargetEntry(PostgresCastToExpr(int8_const),
                                                /*resno=*/1,
                                                /*resname=*/pstrdup("?column?"),
                                                /*resjunk=*/false);
    query->targetList = list_make1(target_entry);
    query->canSetTag = true;
    query->stmt_location = -1;
    return query;
  };
  auto gsql_subquery_select1 =
      []() -> std::unique_ptr<zetasql::ResolvedProjectScan> {
    zetasql::ResolvedColumn column(
        /*column_id=*/1,
        /*table_name=*/zetasql::IdString::MakeGlobal("$expr_subquery"),
        /*name=*/zetasql::IdString::MakeGlobal("?column?"),
        zetasql::types::Int64Type());
    std::vector<zetasql::ResolvedColumn> column_list{column};
    std::vector<std::unique_ptr<zetasql::ResolvedComputedColumn>> expr_list;
    expr_list.push_back(zetasql::MakeResolvedComputedColumn(
        column, zetasql::MakeResolvedLiteral(zetasql::types::Int64Type(),
                                               zetasql::Value::Int64(1))));
    return zetasql::MakeResolvedProjectScan(
        column_list, std::move(expr_list),
        zetasql::MakeResolvedSingleRowScan());
  };

  // EXISTS_SUBLINK: EXISTS(SELECT 1::int8)
  ZETASQL_ASSIGN_OR_RETURN(auto sublink,
                   internal::makeSubLink(
                       EXISTS_SUBLINK, /*subLinkId=*/0,
                       /*testexpr=*/nullptr, /*operName=*/nullptr,
                       /*subselect=*/PostgresCastToNode(*pg_select1_int64())));
  value_pairs.emplace_back(
      PostgresCastToExpr(sublink),
      zetasql::MakeResolvedSubqueryExpr(
          zetasql::types::BoolType(), zetasql::ResolvedSubqueryExpr::EXISTS,
          /*parameter_list=*/{}, /*in_expr=*/nullptr,
          /*subquery=*/gsql_subquery_select1()));

  // EXPR_SUBLINK: (lefthand) op (SELECT 1::int8)
  ZETASQL_ASSIGN_OR_RETURN(sublink,
                   internal::makeSubLink(
                       EXPR_SUBLINK, /*subLinkId=*/0,
                       /*testexpr=*/nullptr, /*operName=*/nullptr,
                       /*subselect=*/PostgresCastToNode(*pg_select1_int64())));
  value_pairs.emplace_back(PostgresCastToExpr(sublink),
                           zetasql::MakeResolvedSubqueryExpr(
                               zetasql::types::Int64Type(),
                               zetasql::ResolvedSubqueryExpr::SCALAR,
                               /*parameter_list=*/{}, /*in_expr=*/nullptr,
                               /*subquery=*/gsql_subquery_select1()));

  return value_pairs;
}

absl::StatusOr<ValuePairVector> ExplicitCastingCoerceViaIOValuePairs() {
  ValuePairVector value_pairs;

  // pg: cast(1 as text) -> gsql: "cast(1 as string)"
  ZETASQL_ASSIGN_OR_RETURN(auto int8_const,
                   internal::makeScalarConst(INT8OID, Int64GetDatum(1),
                                             /*constisnull=*/false));
  ZETASQL_ASSIGN_OR_RETURN(auto coerce, internal::makeCoerceViaIO(
                                    /*arg=*/PostgresCastToExpr(int8_const),
                                    /*resulttype=*/TEXTOID,
                                    /*resultcollid=*/DEFAULT_COLLATION_OID,
                                    /*coerceformat=*/COERCE_EXPLICIT_CAST));
  value_pairs.emplace_back(
      PostgresCastToExpr(coerce),
      zetasql::MakeResolvedCast(
          zetasql::types::StringType(),
          zetasql::MakeResolvedLiteral(zetasql::types::Int64Type(),
                                         zetasql::Value::Int64(1)),
          /*return_null_on_error=*/false));

  return value_pairs;
}

absl::StatusOr<ValuePairVector> CastingValuePairs() {
  ValuePairVector value_pairs;

  ZETASQL_ASSIGN_OR_RETURN(auto new_values, ExplicitCastingCoerceViaIOValuePairs());
  std::move(std::begin(new_values), std::end(new_values),
            std::back_inserter(value_pairs));

  return value_pairs;
}

absl::StatusOr<std::vector<std::pair<Expr*, absl::string_view>>>
NumericCastingValuePairs() {
  std::vector<std::pair<Expr*, absl::string_view>>
      pg_expr_to_gsql_expr_debug_string;
  // numeric -> float8
  // pg: cast(1.1 as float8) <-> gsql: pg.cast_to_double(1.1)
  ZETASQL_ASSIGN_OR_RETURN(auto numeric_const, MakeNumericConst("1.1"));
  ZETASQL_ASSIGN_OR_RETURN(
      auto float_cast,
      internal::makeFuncExpr(1746, FLOAT8OID,
                             list_make1(PostgresCastToExpr(numeric_const)),
                             COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(float_cast),
      "FunctionCall(spanner:pg.cast_to_double(PG.NUMERIC) -> "
      "DOUBLE)\n+-Literal(type=PG.NUMERIC, value=1.1)\n");

  // numeric -> int8_t
  // pg: cast(2.2 as int8_t) <-> gsql: pg.cast_to_int64(2.2)
  ZETASQL_ASSIGN_OR_RETURN(numeric_const, MakeNumericConst("2.2"));
  ZETASQL_ASSIGN_OR_RETURN(
      auto int_cast,
      internal::makeFuncExpr(1779, INT8OID,
                             list_make1(PostgresCastToExpr(numeric_const)),
                             COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(int_cast),
      "FunctionCall(spanner:pg.cast_to_int64(PG.NUMERIC) -> "
      "INT64)\n+-Literal(type=PG.NUMERIC, value=2.2)\n");

  // numeric -> text
  // pg: cast(3.3 as text) -> gsql: pg.cast_to_string(3.3)
  ZETASQL_ASSIGN_OR_RETURN(numeric_const, MakeNumericConst("3.3"));
  ZETASQL_ASSIGN_OR_RETURN(auto coerce, internal::makeCoerceViaIO(
                                    /*arg=*/PostgresCastToExpr(numeric_const),
                                    /*resulttype=*/TEXTOID,
                                    /*resultcollid=*/DEFAULT_COLLATION_OID,
                                    /*coerceformat=*/COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(coerce),
      "FunctionCall(spanner:pg.cast_to_string(PG.NUMERIC) -> "
      "STRING)\n+-Literal(type=PG.NUMERIC, value=3.3)\n");

  // numeric -> text
  // pg: cast(1000000000000.0000000000001 as text) -> gsql:
  // pg.cast_to_string(1000000000000.0000000000001)
  ZETASQL_ASSIGN_OR_RETURN(numeric_const,
                   MakeNumericConst("1000000000000.0000000000001"));
  ZETASQL_ASSIGN_OR_RETURN(coerce, internal::makeCoerceViaIO(
                               /*arg=*/PostgresCastToExpr(numeric_const),
                               /*resulttype=*/TEXTOID,
                               /*resultcollid=*/DEFAULT_COLLATION_OID,
                               /*coerceformat=*/COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(coerce),
      "FunctionCall(spanner:pg.cast_to_string(PG.NUMERIC) -> "
      "STRING)\n+-Literal(type=PG.NUMERIC, "
      "value=1000000000000.0000000000001)\n");

  // float8 -> numeric
  ZETASQL_ASSIGN_OR_RETURN(
      auto float8_const,
      internal::makeScalarConst(FLOAT8OID, Float8GetDatum(3.141592653589793),
                                /*constisnull=*/false));
  ZETASQL_ASSIGN_OR_RETURN(
      auto float_to_numeric_cast,
      internal::makeFuncExpr(1743, NUMERICOID,
                             list_make1(PostgresCastToExpr(float8_const)),
                             COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(float_to_numeric_cast),
      "FunctionCall(spanner:pg.cast_to_numeric(DOUBLE) -> "
      "PG.NUMERIC)\n+-Literal(type=DOUBLE, value=3.1415926535897931)\n");

  // int8_t -> numeric
  ZETASQL_ASSIGN_OR_RETURN(auto int8_const,
                   internal::makeScalarConst(INT8OID, Int8GetDatum(123456),
                                             /*constisnull=*/false));
  ZETASQL_ASSIGN_OR_RETURN(
      auto int_to_numeric_cast,
      internal::makeFuncExpr(1781, NUMERICOID,
                             list_make1(PostgresCastToExpr(int8_const)),
                             COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(int_to_numeric_cast),
      "FunctionCall(spanner:pg.cast_to_numeric(INT64) -> "
      "PG.NUMERIC)\n+-Literal(type=INT64, value=123456)\n");

  // text -> numeric
  ZETASQL_ASSIGN_OR_RETURN(
      auto string_const,
      internal::makeStringConst(TEXTOID, "123.456789", /*constisnull=*/false));
  ZETASQL_ASSIGN_OR_RETURN(coerce, internal::makeCoerceViaIO(
                               /*arg=*/PostgresCastToExpr(string_const),
                               /*resulttype=*/NUMERICOID,
                               /*resultcollid=*/DEFAULT_COLLATION_OID,
                               /*coerceformat=*/COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(coerce),
      "FunctionCall(spanner:pg.cast_to_numeric(STRING) -> "
      "PG.NUMERIC)\n+-Literal(type=STRING, value=\"123.456789\")\n");

  // numeric -> fixed precision numeric
  // pg: cast(3.3 as numeric(4,2)) -> gsql: pg.cast_to_numeric(3.3, 4, 5)
  ZETASQL_ASSIGN_OR_RETURN(auto int4_const,
                   // 262150 represents precision=4, scale=2
                   internal::makeScalarConst(INT4OID, Int32GetDatum(262150),
                                             /*constisnull=*/false));
  ZETASQL_ASSIGN_OR_RETURN(numeric_const, MakeNumericConst("3.3"));
  ZETASQL_ASSIGN_OR_RETURN(
      auto fixed_numeric_cast,
      internal::makeFuncExpr(
          1703, NUMERICOID,
          list_make2(PostgresCastToExpr(numeric_const), int4_const),
          COERCE_EXPLICIT_CAST));
  pg_expr_to_gsql_expr_debug_string.emplace_back(
      PostgresCastToExpr(fixed_numeric_cast),
      "FunctionCall(spanner:pg.cast_to_numeric(PG.NUMERIC, INT64, optional(1) INT64) -> "
      "PG.NUMERIC)\n+-Literal(type=PG.NUMERIC, "
      "value=3.3)\n+-Literal(type=INT64, value=4)\n+-Literal(type=INT64, "
      "value=2)\n");

  return pg_expr_to_gsql_expr_debug_string;
}

// Accumulates all value pairs for the general
// Expr <-> zetasql::ResolvedExpr tests.
std::vector<absl::StatusOr<ValuePairVector>> ValuePairVectors() {
  std::vector<absl::StatusOr<ValuePairVector>> pair_lists;
  pair_lists.push_back(ConstValuePairs());
  pair_lists.push_back(ParamValuePairs());
  return pair_lists;
}

TEST_F(ExpressionTransformerTest, PgConstToGsqlLiteralNotFoundTypeInt16) {
  Const* const_value;
  // This type isn't even recognized; should generate a different error
  ZETASQL_ASSERT_OK_AND_ASSIGN(const_value,
                       internal::makeScalarConst(INT2OID, Int16GetDatum(17),
                                                 /*constisnull=*/false));
  EXPECT_THAT(forward_transformer_->BuildGsqlResolvedLiteral(*const_value),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST_F(ExpressionTransformerTest, PgConstToGsqlLiteralWithLocation) {
  Const* pg_const;
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const,
                       internal::makeScalarConst(INT8OID, Int64GetDatum(13),
                                                 /*constisnull=*/false));
  pg_const->location = 42;

  // A new adapter and transformer are needed here as we are testing a non-null
  // parse location. The default transformer created in
  // ExpressionTransformerTest contains an adapter with an empty parse location
  // list.
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> adapter = GetSpangresTestCatalogAdapter(
      analyzer_options, /*rqg_user_catalog=*/nullptr, {{42, 44}});

  auto transformer = std::make_unique<ForwardTransformer>(std::move(adapter));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<zetasql::ResolvedLiteral> gsql_literal,
                       transformer->BuildGsqlResolvedLiteral(*pg_const));

  const auto* parse_location_range =
      gsql_literal->GetParseLocationRangeOrNULL();
  ASSERT_NE(parse_location_range, nullptr);
  EXPECT_EQ(42, parse_location_range->start().GetByteOffset());
}

TEST_F(ExpressionTransformerTest, PgConstToGsqlLiteralWithoutLocation) {
  Const* pg_const;
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const,
                       internal::makeScalarConst(INT8OID, Int64GetDatum(13),
                                                 /*constisnull=*/false));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::ResolvedLiteral> gsql_literal,
      forward_transformer_->BuildGsqlResolvedLiteral(*pg_const));

  EXPECT_EQ(gsql_literal->GetParseLocationRangeOrNULL(), nullptr);
}

TEST_F(ExpressionTransformerTest, PgParamToGsqlParameter) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto param_pairs, ParamValuePairs());
  for (const auto& [pg_expr, gsql_expr] : param_pairs) {
    Param* pg_param = PostgresCastNode(Param, pg_expr);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<const zetasql::ResolvedParameter> gsql_parameter,
        forward_transformer_->BuildGsqlResolvedParameter(*pg_param));
    ASSERT_NE(gsql_parameter, nullptr);
    EXPECT_EQ(gsql_parameter->DebugString(), gsql_expr->DebugString());
  }
}

TEST_F(ExpressionTransformerTest, PgParamToGsqlParameterInvalidArgument) {
  for (const auto param_type : {PARAM_EXEC, PARAM_SUBLINK, PARAM_MULTIEXPR}) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Param* pg_param,
        internal::makeParam(param_type, /*paramid=*/0, INT8OID));
    EXPECT_THAT(forward_transformer_->BuildGsqlResolvedParameter(*pg_param),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST_F(ValidMemoryContext, PgSubLinkToGsqlSubquery) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto sublink_pairs, SubLinkValuePairs());
  for (const auto& [pg_expr, gsql_expr] : sublink_pairs) {
    ForwardTransformer transformer(
        GetSpangresTestCatalogAdapter(analyzer_options));
    SubLink* pg_sublink = PostgresCastNode(SubLink, pg_expr);
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<zetasql::ResolvedExpr> gsql_subquery,
                         transformer.BuildGsqlResolvedSubqueryExpr(
                             *pg_sublink, &expr_transformer_info));
    ASSERT_NE(gsql_subquery, nullptr);
    EXPECT_EQ(gsql_subquery->DebugString(), gsql_expr->DebugString());
  }
}

TEST_F(ExpressionTransformerTest,
       PgExplicitCastingCoerceViaIOToGsqlResolvedExpr) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto explicit_coerce_pairs,
                       ExplicitCastingCoerceViaIOValuePairs());
  for (const auto& [pg_expr, gsql_expr] : explicit_coerce_pairs) {
    CoerceViaIO* pg_coerce_via_io = PostgresCastNode(CoerceViaIO, pg_expr);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<const zetasql::ResolvedExpr> new_gsql_expr,
        forward_transformer_->BuildGsqlResolvedExpr(*pg_coerce_via_io,
                                                    &expr_transformer_info));
    ASSERT_NE(new_gsql_expr, nullptr);
    EXPECT_EQ(new_gsql_expr->DebugString(), gsql_expr->DebugString());
  }
}

TEST_F(ExpressionTransformerTest, PgCastingToGsqlResolvedExpr) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto cast_pairs, CastingValuePairs());
  for (const auto& [pg_expr, gsql_expr] : cast_pairs) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<const zetasql::ResolvedExpr> new_gsql_expr,
        forward_transformer_->BuildGsqlResolvedExpr(*pg_expr,
                                                    &expr_transformer_info));
    ASSERT_NE(new_gsql_expr, nullptr);
    EXPECT_EQ(new_gsql_expr->DebugString(), gsql_expr->DebugString());
  }
}

TEST_F(ExpressionTransformerTest, PgCastingNumericToGsqlResolvedExpr) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto numeric_cast_pairs, NumericCastingValuePairs());
  for (const auto& [pg_expr, expected_debug_str] : numeric_cast_pairs) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<const zetasql::ResolvedExpr> new_gsql_expr,
        forward_transformer_->BuildGsqlResolvedExpr(*pg_expr,
                                                    &expr_transformer_info));
    ASSERT_NE(new_gsql_expr, nullptr);
    EXPECT_EQ(new_gsql_expr->DebugString(), expected_debug_str);
  }
}

TEST_F(ExpressionTransformerTest, PgExprToGsqlExpr) {
  VarIndexScope var_index_scope;
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&var_index_scope,
                                              /*clause_name=*/"");

  for (const auto& value_pair_list : ValuePairVectors()) {
    ZETASQL_ASSERT_OK(value_pair_list);
    for (const auto& [pg_expr, gsql_expr] : *value_pair_list) {
      ZETASQL_ASSERT_OK_AND_ASSIGN(
          std::unique_ptr<const zetasql::ResolvedExpr> result,
          forward_transformer_->BuildGsqlResolvedExpr(*pg_expr,
                                                      &expr_transformer_info));
      ASSERT_NE(result, nullptr);
      EXPECT_EQ(result->DebugString(), gsql_expr->DebugString());
    }
  }
}

using ::postgres_translator::internal::makeScalarConst;
using ::postgres_translator::internal::makeStringConst;
using ::postgres_translator::internal::PostgresCastToNode;
// Helper function creates a simple list of PG hints.
// REQUIRES: Valid MemoryContext to allocate PostgreSQL Nodes.
void GetHintList(List** list) {
  DefElem* hint1 = makeDefElem(pstrdup("hint1_name"),
                               PostgresCastToNode(makeBoolConst(
                                   /*value=*/true, /*isnull=*/false)),
                               /*location=*/-1);
  hint1->defnamespace = pstrdup("hint1_namespace");
  Const* string_const;
  ZETASQL_ASSERT_OK_AND_ASSIGN(string_const, makeStringConst(TEXTOID, "hint2_value",
                                                     /*constisnull=*/false));
  DefElem* hint2 =
      makeDefElem(pstrdup("hint2_name"), PostgresCastToNode(string_const),
                  /*location=*/-1);
  Const* int8_const;
  ZETASQL_ASSERT_OK_AND_ASSIGN(int8_const, makeScalarConst(INT8OID, Int8GetDatum(42),
                                                   /*constisnull=*/false));
  DefElem* hint3 =
      makeDefElem(pstrdup("hint3_name"), PostgresCastToNode(int8_const),
                  /*location=*/-1);
  *list = list_make3(hint1, hint2, hint3);
}

TEST_F(ExpressionTransformerTest, PgHintToGsqlHint) {
  List* hint_list;
  GetHintList(&hint_list);

  VarIndexScope var_index_scope;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::vector<std::unique_ptr<const zetasql::ResolvedOption>> option_list,
      forward_transformer_->BuildGsqlResolvedOptionList(*hint_list,
                                                        &var_index_scope));

  ASSERT_EQ(option_list.size(), 3);
  EXPECT_EQ(option_list[0]->name(), "hint1_name");
  EXPECT_EQ(option_list[0]->qualifier(), "hint1_namespace");
  EXPECT_TRUE(option_list[0]->value()->Is<zetasql::ResolvedLiteral>());
  EXPECT_TRUE(option_list[0]
                  ->value()
                  ->GetAs<zetasql::ResolvedLiteral>()
                  ->type()
                  ->IsBool());
  EXPECT_TRUE(option_list[0]
                  ->value()
                  ->GetAs<zetasql::ResolvedLiteral>()
                  ->value()
                  .bool_value());
  EXPECT_EQ(option_list[1]->name(), "hint2_name");
  EXPECT_TRUE(option_list[1]->value()->Is<zetasql::ResolvedLiteral>());
  EXPECT_TRUE(option_list[1]
                  ->value()
                  ->GetAs<zetasql::ResolvedLiteral>()
                  ->type()
                  ->IsString());
  EXPECT_EQ(option_list[1]
                ->value()
                ->GetAs<zetasql::ResolvedLiteral>()
                ->value()
                .string_value(),
            "hint2_value");
  EXPECT_EQ(option_list[2]->name(), "hint3_name");
  EXPECT_TRUE(option_list[2]->value()->Is<zetasql::ResolvedLiteral>());
  EXPECT_TRUE(option_list[2]
                  ->value()
                  ->GetAs<zetasql::ResolvedLiteral>()
                  ->type()
                  ->IsInteger());
  EXPECT_EQ(option_list[2]
                ->value()
                ->GetAs<zetasql::ResolvedLiteral>()
                ->value()
                .int64_value(),
            42);
}

TEST_F(ExpressionTransformerTest, BuildGsqlResolvedLiteralPreservesNullBytes) {
  // Create an array of bytes with the zero byte in it somewhere in the middle.
  std::vector<char> byte_vector{'a', 'b', '\0', 'c'};
  // Note: must use a conversion routine that doesn't assume null-termination.
  Datum pg_datum = PointerGetDatum(
      cstring_to_text_with_len(byte_vector.data(), byte_vector.size()));
  Const* pg_const =
      makeConst(BYTEAOID, /*consttypmod=*/-1, /*constcollid=*/InvalidOid,
                /*constlen=*/-1, pg_datum,
                /*constisnull=*/false, /*constbyval=*/false);

  std::unique_ptr<zetasql::ResolvedLiteral> literal;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      literal, forward_transformer_->BuildGsqlResolvedLiteral(*pg_const));

  ASSERT_EQ(literal->type(), zetasql::types::BytesType());
  const std::string& value = literal->value().bytes_value();
  ASSERT_EQ(value.size(), byte_vector.size()) << literal->DebugString();
  for (int i = 0; i < byte_vector.size(); ++i) {
    EXPECT_EQ(value[i], byte_vector[i]);
  }
}
}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
