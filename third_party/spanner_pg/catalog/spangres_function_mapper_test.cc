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

#include "third_party/spanner_pg/catalog/spangres_function_mapper.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_type_d.h"

namespace postgres_translator {

namespace {

using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

const zetasql::Type* gsql_bool = zetasql::TypeFactory().get_bool();
const zetasql::Type* gsql_double = zetasql::TypeFactory().get_double();
const zetasql::Type* gsql_int64 = zetasql::TypeFactory().get_int64();
const zetasql::Type* gsql_string = zetasql::TypeFactory().get_string();

MATCHER_P(PostgresFunctionArgumentsEq, other, "") {
  if (arg.postgres_function_name() != other.postgres_function_name()) {
    *result_listener << "where the expected postgres_function_name "
                     << other.postgres_function_name()
                     << " != " << arg.postgres_function_name();
    return false;
  }
  if (arg.mapped_function_name() != other.mapped_function_name()) {
    *result_listener << "where the expected mapped_function_name "
                     << other.mapped_function_name()
                     << " != " << arg.mapped_function_name();
    return false;
  }
  if (arg.mode() != other.mode()) {
    *result_listener << "where the expected mode " << other.mode()
                     << " != " << arg.mode();
    return false;
  }
  if (arg.signature_arguments().size() != other.signature_arguments().size()) {
    *result_listener << "where the expected signature_arguments size "
                     << other.signature_arguments().size()
                     << " != " << arg.signature_arguments().size();
    return false;
  }
  for (int i = 0; i < arg.signature_arguments().size(); ++i) {
    const PostgresFunctionSignatureArguments& arg_sig_args =
        arg.signature_arguments()[i];
    const PostgresFunctionSignatureArguments& other_sig_args =
        other.signature_arguments()[i];

    if (arg_sig_args.has_mapped_function() !=
        other_sig_args.has_mapped_function()) {
      *result_listener << "where the expected signature_arguments[" << i
                       << "].has_mapped_function "
                       << other_sig_args.has_mapped_function()
                       << " != " << arg_sig_args.has_mapped_function();
      return false;
    }
    if (arg_sig_args.explicit_mapped_function_name() !=
        other_sig_args.explicit_mapped_function_name()) {
      *result_listener << "where the expected signature_arguments[" << i
                       << "].explicit_mapped_function_name "
                       << other_sig_args.explicit_mapped_function_name()
                       << " != "
                       << arg_sig_args.explicit_mapped_function_name();
      return false;
    }
    if (arg_sig_args.postgres_proc_oid() !=
        other_sig_args.postgres_proc_oid()) {
      *result_listener << "where the expected signature_arguments[" << i
                       << "].postgres_proc_oid "
                       << other_sig_args.postgres_proc_oid()
                       << " != " << arg_sig_args.postgres_proc_oid();
      return false;
    }

    const zetasql::FunctionSignature& arg_sig = arg_sig_args.signature();
    const zetasql::FunctionSignature& other_sig = other_sig_args.signature();
    if (arg_sig.options().is_deprecated() !=
        other_sig.options().is_deprecated()) {
      *result_listener << "where the expected signature_arguments[" << i
                       << "].is_deprecated "
                       << other_sig.options().is_deprecated()
                       << " != " << arg_sig.options().is_deprecated();
      return false;
    }

    if (arg_sig.result_type().type() != other_sig.result_type().type()) {
      *result_listener << "where the expected signature_arguments[" << i
                       << "].signature.return_type "
                       << other_sig.result_type().type()->TypeName(
                              zetasql::PRODUCT_EXTERNAL)
                       << " != "
                       << arg_sig.result_type().type()->TypeName(
                              zetasql::PRODUCT_EXTERNAL);
      return false;
    }

    const zetasql::FunctionArgumentTypeList& arg_args = arg_sig.arguments();
    const zetasql::FunctionArgumentTypeList& other_args =
        other_sig.arguments();
    if (arg_args.size() != other_args.size()) {
      *result_listener << "where the expected signature_arguments[" << i
                       << "].signature.arguments.size " << other_args.size()
                       << " != " << arg_args.size();
      return false;
    }
    for (int j = 0; j < arg_args.size(); ++j) {
      if (arg_args[j].type() != other_args[j].type()) {
        *result_listener
            << "where the expected signature_arguments[" << i
            << "].signature.arguments[" << j << "].type "
            << other_args[j].type()->TypeName(zetasql::PRODUCT_EXTERNAL)
            << " != "
            << arg_args[j].type()->TypeName(zetasql::PRODUCT_EXTERNAL);
        return false;
      }
      if (arg_args[j].cardinality() != other_args[j].cardinality()) {
        *result_listener << "where the expected signature_argument[" << i
                         << "].signature.arguments[" << j << "].cardinality "
                         << zetasql::FunctionEnums::ArgumentCardinality_Name(
                                other_args[j].cardinality())
                         << " != "
                         << zetasql::FunctionEnums::ArgumentCardinality_Name(
                                arg_args[j].cardinality());
        return false;
      }
      std::string arg_arg_name =
          arg_args[j].has_argument_name() ? arg_args[j].argument_name() : "";
      std::string other_arg_name = other_args[j].has_argument_name()
                                       ? other_args[j].argument_name()
                                       : "";
      if (arg_arg_name != other_arg_name) {
        *result_listener << "where the expected signature_argument[" << i
                         << "].signature.arguments[" << j << "].argument_name '"
                         << other_arg_name << "' != '" << arg_arg_name << "'";
        return false;
      }
    }
  }
  return true;
}

class TestSpangresFunctionMapper : public testing::Test {
 public:
  EngineSystemCatalog* catalog_;
  std::unique_ptr<SpangresFunctionMapper> mapper_;

  void SetUp() override {
    catalog_ = spangres::test::GetSpangresTestSystemCatalog();
    mapper_ = std::make_unique<SpangresFunctionMapper>(catalog_);
  }
};

TEST_F(TestSpangresFunctionMapper, MapsMetaTypes) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "test";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig1 = function.add_signatures();
  FunctionNamePathProto sig1_pg_name_path;
  (*sig1_pg_name_path.add_name_path()) = "pg";
  (*sig1_pg_name_path.add_name_path()) = "test";
  (*sig1->add_postgresql_name_paths()) = sig1_pg_name_path;
  sig1->mutable_return_type()->set_oid(BOOLOID);
  sig1->set_enable_in_emulator(true);
  FunctionArgumentProto* sig1_arg1 = sig1->add_arguments();
  sig1_arg1->mutable_type()->set_oid(ANYOID);
  sig1_arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  FunctionArgumentProto* sig1_arg2 = sig1->add_arguments();
  sig1_arg2->mutable_type()->set_oid(ANYARRAYOID);
  sig1_arg2->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg2->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  FunctionArgumentProto* sig1_arg3 = sig1->add_arguments();
  sig1_arg3->mutable_type()->set_oid(ANYELEMENTOID);
  sig1_arg3->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg3->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);

  ZETASQL_ASSERT_OK(mapper_->ToPostgresFunctionArguments(function));
}

TEST_F(TestSpangresFunctionMapper, MapsSamePgNamePathToSingleFunction) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "regexp_contains";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig1 = function.add_signatures();
  FunctionNamePathProto sig1_pg_name_path;
  (*sig1_pg_name_path.add_name_path()) = "pg";
  (*sig1_pg_name_path.add_name_path()) = "textregexeq";
  (*sig1->add_postgresql_name_paths()) = sig1_pg_name_path;
  sig1->mutable_return_type()->set_oid(BOOLOID);
  sig1->set_enable_in_emulator(true);
  FunctionArgumentProto* sig1_arg1 = sig1->add_arguments();
  sig1_arg1->mutable_type()->set_oid(TEXTOID);
  sig1_arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  sig1_arg1->set_name("required_arg1");
  FunctionArgumentProto* sig1_arg2 = sig1->add_arguments();
  sig1_arg2->mutable_type()->set_oid(TEXTOID);
  sig1_arg2->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg2->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  sig1_arg2->set_name("required_arg2");

  FunctionSignatureProto* sig2 = function.add_signatures();
  FunctionNamePathProto sig2_pg_name_path;
  (*sig2_pg_name_path.add_name_path()) = "pg";
  (*sig2_pg_name_path.add_name_path()) = "textregexeq";
  (*sig2->add_postgresql_name_paths()) = sig2_pg_name_path;
  sig2->mutable_return_type()->set_oid(BOOLOID);
  sig2->set_enable_in_emulator(true);
  sig2->set_deprecated(true);
  FunctionArgumentProto* sig2_arg1 = sig2->add_arguments();
  sig2_arg1->mutable_type()->set_oid(ANYOID);
  sig2_arg1->set_cardinality(zetasql::FunctionEnums::REPEATED);
  sig2_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);

  FunctionSignatureProto* sig3 = function.add_signatures();
  FunctionNamePathProto sig3_pg_name_path;
  (*sig3_pg_name_path.add_name_path()) = "pg";
  (*sig3_pg_name_path.add_name_path()) = "textregexeq";
  (*sig3->add_postgresql_name_paths()) = sig3_pg_name_path;
  sig3->mutable_return_type()->set_oid(BOOLOID);
  sig3->set_enable_in_emulator(true);
  FunctionArgumentProto* sig3_arg1 = sig3->add_arguments();
  sig3_arg1->mutable_type()->set_oid(TEXTOID);
  sig3_arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig3_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  FunctionArgumentProto* sig3_arg2 = sig3->add_arguments();
  sig3_arg2->mutable_type()->set_oid(TEXTOID);
  sig3_arg2->set_cardinality(zetasql::FunctionEnums::OPTIONAL);
  sig3_arg2->set_named_argument_kind(
      zetasql::FunctionEnums::POSITIONAL_OR_NAMED);
  sig3_arg2->set_name("optional_arg");

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PostgresFunctionArguments> result,
                       mapper_->ToPostgresFunctionArguments(function));
  ASSERT_THAT(result.size(), Eq(1));

  EXPECT_THAT(
      result[0],
      PostgresFunctionArgumentsEq(PostgresFunctionArguments(
          "textregexeq", "regexp_contains",
          {
              PostgresFunctionSignatureArguments(zetasql::FunctionSignature(
                  gsql_bool,
                  {{gsql_string,
                    zetasql::FunctionArgumentTypeOptions()
                        .set_argument_name(
                            "required_arg1",
                            zetasql::FunctionEnums::POSITIONAL_ONLY)
                        .set_cardinality(zetasql::FunctionEnums::REQUIRED)},
                   {gsql_string,
                    zetasql::FunctionArgumentTypeOptions()
                        .set_argument_name(
                            "required_arg2",
                            zetasql::FunctionEnums::POSITIONAL_ONLY)
                        .set_cardinality(zetasql::FunctionEnums::REQUIRED)}},
                  /*context_ptr=*/nullptr)),
              PostgresFunctionSignatureArguments(zetasql::FunctionSignature(
                  gsql_bool,
                  {{zetasql::SignatureArgumentKind::ARG_TYPE_ARBITRARY,
                    zetasql::FunctionEnums::REPEATED}},
                  /*context_id=*/0,
                  zetasql::FunctionSignatureOptions().set_is_deprecated(
                      true))),
              PostgresFunctionSignatureArguments(zetasql::FunctionSignature(
                  gsql_bool,
                  {gsql_string,
                   {gsql_string,
                    zetasql::FunctionArgumentTypeOptions()
                        .set_argument_name(
                            "optional_arg",
                            zetasql::FunctionEnums::POSITIONAL_OR_NAMED)
                        .set_cardinality(zetasql::FunctionEnums::OPTIONAL)}},
                  /*context_ptr=*/nullptr)),
          },
          zetasql::Function::SCALAR, "pg_catalog")));
}

TEST_F(TestSpangresFunctionMapper,
       MapsDistinctSignaturePgNamePathsToMultipleFunctions) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "$add";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig1 = function.add_signatures();
  FunctionNamePathProto sig1_pg_name_path;
  (*sig1_pg_name_path.add_name_path()) = "pg";
  (*sig1_pg_name_path.add_name_path()) = "float8pl";
  (*sig1->add_postgresql_name_paths()) = sig1_pg_name_path;
  sig1->mutable_return_type()->set_oid(FLOAT8OID);
  sig1->set_enable_in_emulator(true);
  FunctionArgumentProto* sig1_arg1 = sig1->add_arguments();
  sig1_arg1->mutable_type()->set_oid(FLOAT8OID);
  sig1_arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  FunctionArgumentProto* sig1_arg2 = sig1->add_arguments();
  sig1_arg2->mutable_type()->set_oid(FLOAT8OID);
  sig1_arg2->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg2->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);

  FunctionSignatureProto* sig2 = function.add_signatures();
  FunctionNamePathProto sig2_pg_name_path;
  (*sig2_pg_name_path.add_name_path()) = "pg";
  (*sig2_pg_name_path.add_name_path()) = "int8pl";
  (*sig2->add_postgresql_name_paths()) = sig2_pg_name_path;
  sig2->mutable_return_type()->set_oid(INT8OID);
  sig2->set_enable_in_emulator(true);
  FunctionArgumentProto* sig2_arg1 = sig2->add_arguments();
  sig2_arg1->mutable_type()->set_oid(INT8OID);
  sig2_arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig2_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  FunctionArgumentProto* sig2_arg2 = sig2->add_arguments();
  sig2_arg2->mutable_type()->set_oid(INT8OID);
  sig2_arg2->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig2_arg2->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PostgresFunctionArguments> result,
                       mapper_->ToPostgresFunctionArguments(function));
  ASSERT_THAT(result.size(), Eq(2));
  EXPECT_THAT(result,
              UnorderedElementsAre(
                  PostgresFunctionArgumentsEq(PostgresFunctionArguments(
                      "float8pl", "$add",
                      {
                          PostgresFunctionSignatureArguments(
                              zetasql::FunctionSignature(
                                  gsql_double, {gsql_double, gsql_double},
                                  /*context_ptr=*/nullptr)),
                      },
                      zetasql::Function::SCALAR, "pg_catalog")),
                  PostgresFunctionArgumentsEq(PostgresFunctionArguments(
                      "int8pl", "$add",
                      {
                          PostgresFunctionSignatureArguments(
                              zetasql::FunctionSignature(
                                  gsql_int64, {gsql_int64, gsql_int64},
                                  /*context_ptr=*/nullptr)),
                      },
                      zetasql::Function::SCALAR, "pg_catalog"))));
}

TEST_F(TestSpangresFunctionMapper, MapsSpannerNamespacedFunction) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "bit_reverse";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig1 = function.add_signatures();
  FunctionNamePathProto sig1_pg_name_path;
  (*sig1_pg_name_path.add_name_path()) = "spanner";
  (*sig1_pg_name_path.add_name_path()) = "bit_reverse";
  (*sig1->add_postgresql_name_paths()) = sig1_pg_name_path;
  sig1->set_oid(50001);
  sig1->mutable_return_type()->set_oid(INT8OID);
  sig1->set_enable_in_emulator(true);
  FunctionArgumentProto* sig1_arg1 = sig1->add_arguments();
  sig1_arg1->mutable_type()->set_oid(INT8OID);
  sig1_arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  FunctionArgumentProto* sig1_arg2 = sig1->add_arguments();
  sig1_arg2->mutable_type()->set_oid(BOOLOID);
  sig1_arg2->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig1_arg2->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);

  FunctionSignatureProto* sig2 = function.add_signatures();
  FunctionNamePathProto sig2_pg_name_path;
  (*sig2_pg_name_path.add_name_path()) = "spanner";
  (*sig2_pg_name_path.add_name_path()) = "bit_reverse";
  (*sig2->add_postgresql_name_paths()) = sig2_pg_name_path;
  sig2->set_oid(50002);
  sig2->mutable_return_type()->set_oid(INT8OID);
  sig2->set_enable_in_emulator(true);
  FunctionArgumentProto* sig2_arg1 = sig2->add_arguments();
  sig2_arg1->mutable_type()->set_oid(INT8OID);
  sig2_arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig2_arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PostgresFunctionArguments> result,
                       mapper_->ToPostgresFunctionArguments(function));
  ASSERT_THAT(result.size(), Eq(1));
  EXPECT_THAT(result[0],
              PostgresFunctionArgumentsEq(PostgresFunctionArguments(
                  "bit_reverse", "bit_reverse",
                  {
                      PostgresFunctionSignatureArguments(
                          zetasql::FunctionSignature(gsql_int64,
                                                       {gsql_int64, gsql_bool},
                                                       /*context_ptr=*/nullptr),
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"", 50001),
                      PostgresFunctionSignatureArguments(
                          zetasql::FunctionSignature(gsql_int64, {gsql_int64},
                                                       /*context_ptr=*/nullptr),
                          /*has_mapped_function=*/true,
                          /*explicit_mapped_function_name=*/"", 50002),
                  },
                  zetasql::Function::SCALAR, "spanner")));
}

TEST_F(TestSpangresFunctionMapper,
       ExcludesFunctionsDisabledInEmulatorWhenUsingEmulatorCatalog) {

  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "bit_reverse";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig1 = function.add_signatures();
  FunctionNamePathProto sig1_pg_name_path;
  (*sig1_pg_name_path.add_name_path()) = "spanner";
  (*sig1_pg_name_path.add_name_path()) = "bit_reverse";
  (*sig1->add_postgresql_name_paths()) = sig1_pg_name_path;
  sig1->mutable_return_type()->set_oid(FLOAT8OID);
  sig1->set_enable_in_emulator(false);

  FunctionSignatureProto* sig2 = function.add_signatures();
  FunctionNamePathProto sig2_pg_name_path;
  (*sig2_pg_name_path.add_name_path()) = "spanner";
  (*sig2_pg_name_path.add_name_path()) = "bit_reverse";
  (*sig2->add_postgresql_name_paths()) = sig2_pg_name_path;
  sig2->mutable_return_type()->set_oid(INT8OID);
  sig2->set_enable_in_emulator(true);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PostgresFunctionArguments> result,
                       mapper_->ToPostgresFunctionArguments(function));
  ASSERT_THAT(result.size(), Eq(1));
  EXPECT_THAT(
      result[0],
      PostgresFunctionArgumentsEq(PostgresFunctionArguments(
          "bit_reverse", "bit_reverse",
          {
              PostgresFunctionSignatureArguments(zetasql::FunctionSignature(
                  gsql_int64, {}, /*context_ptr=*/nullptr)),
          },
          zetasql::Function::SCALAR, "spanner")));
}

TEST_F(TestSpangresFunctionMapper, ReturnsErrorWhenFunctionHasNoSignatures) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "error";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  ASSERT_THAT(mapper_->ToPostgresFunctionArguments(function),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("must have at least one signature")));
}

TEST_F(TestSpangresFunctionMapper,
       ReturnsErrorWhenFunctionSignatureNamePathIsNotNamespaced) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "error";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig = function.add_signatures();
  FunctionNamePathProto sig_pg_name_path;
  (*sig_pg_name_path.add_name_path()) = "error";
  (*sig->add_postgresql_name_paths()) = sig_pg_name_path;
  sig->set_enable_in_emulator(true);

  ASSERT_THAT(
      mapper_->ToPostgresFunctionArguments(function),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "postgresql_name_path must have 2 parts (namespace and name)")));
}

TEST_F(TestSpangresFunctionMapper,
       ReturnsErrorWhenFunctionSignatureNamePathHasNestedNamespaces) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "error";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig = function.add_signatures();
  FunctionNamePathProto sig_pg_name_path;
  (*sig_pg_name_path.add_name_path()) = "spanner";
  (*sig_pg_name_path.add_name_path()) = "safe";
  (*sig_pg_name_path.add_name_path()) = "error";
  (*sig->add_postgresql_name_paths()) = sig_pg_name_path;
  sig->set_enable_in_emulator(true);

  ASSERT_THAT(
      mapper_->ToPostgresFunctionArguments(function),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "postgresql_name_path must have 2 parts (namespace and name)")));
}

TEST_F(TestSpangresFunctionMapper,
       ReturnsErrorWhenFunctionSignatureWithNamedArgumentHasNoName) {
  FunctionProto function;
  FunctionNamePathProto mapped_name_path;
  (*mapped_name_path.add_name_path()) = "error";
  (*function.mutable_mapped_name_path()) = mapped_name_path;

  FunctionSignatureProto* sig = function.add_signatures();
  FunctionNamePathProto sig_pg_name_path;
  (*sig_pg_name_path.add_name_path()) = "spanner";
  (*sig_pg_name_path.add_name_path()) = "error";
  (*sig->add_postgresql_name_paths()) = sig_pg_name_path;
  FunctionArgumentProto* sig_arg = sig->add_arguments();
  sig_arg->mutable_type()->set_oid(TEXTOID);
  sig_arg->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  sig_arg->set_named_argument_kind(
      zetasql::FunctionEnums::POSITIONAL_OR_NAMED);
  sig->set_enable_in_emulator(true);

  ASSERT_THAT(
      mapper_->ToPostgresFunctionArguments(function),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("name must be defined")));
}

}  // namespace

}  // namespace postgres_translator

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
