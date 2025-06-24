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

#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_type_d.h"
#include "third_party/spanner_pg/src/include/postgres_ext.h"

namespace postgres_translator {

namespace {

using ::zetasql_base::testing::IsOkAndHolds;

const zetasql::Type* gsql_bool = zetasql::TypeFactory().get_bool();
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
            << "].signature.arguments[" << j << "] "
            << other_args[j].type()->TypeName(zetasql::PRODUCT_EXTERNAL)
            << " != "
            << arg_args[j].type()->TypeName(zetasql::PRODUCT_EXTERNAL);
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

TEST_F(TestSpangresFunctionMapper, MapsToPostgresFunctionSuccessfully) {
  FunctionProto function;
  function.add_name_path("pg");
  function.add_name_path("textregexne");

  FunctionSignatureProto* sig = function.add_signatures();
  sig->mutable_return_type()->set_name("bool");
  sig->mutable_return_type()->set_oid(BOOLOID);
  sig->set_is_enabled_in_catalog(true);
  FunctionArgumentProto* arg1 = sig->add_arguments();
  arg1->mutable_type()->set_name("text");
  arg1->mutable_type()->set_oid(TEXTOID);
  arg1->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  arg1->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);
  FunctionArgumentProto* arg2 = sig->add_arguments();
  arg2->mutable_type()->set_name("text");
  arg2->mutable_type()->set_oid(TEXTOID);
  arg2->set_cardinality(zetasql::FunctionEnums::REQUIRED);
  arg2->set_named_argument_kind(zetasql::FunctionEnums::POSITIONAL_ONLY);

  ZETASQL_ASSERT_OK_AND_ASSIGN(PostgresFunctionArguments result,
                       mapper_->ToPostgresFunctionArguments(function));
  EXPECT_THAT(
      mapper_->ToPostgresFunctionArguments(function),
      IsOkAndHolds(PostgresFunctionArgumentsEq(PostgresFunctionArguments(
          "textregexne", "pg.textregexne",
          {PostgresFunctionSignatureArguments(
              zetasql::FunctionSignature(gsql_bool,
                                           {gsql_string, gsql_string},
                                           /*context_ptr=*/nullptr),
              /*has_mapped_function=*/true,
              /*explicit_mapped_function_name=*/"", InvalidOid)},
          zetasql::Function::SCALAR, "pg_catalog"))));
}

}  // namespace

}  // namespace postgres_translator

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
