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
#include "absl/strings/str_join.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "google/protobuf/text_format.h"

namespace postgres_translator {

namespace {

using ::testing::Eq;

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
  if (arg.query_features_names() != other.query_features_names()) {
    *result_listener << "where the expected query_features_names "
                     << absl::StrJoin(other.query_features_names(), ", ")
                     << " != "
                     << absl::StrJoin(arg.query_features_names(), ", ");
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

    if (arg_sig_args.query_features_names() !=
        other_sig_args.query_features_names()) {
      *result_listener
          << "where the expected signature_arguments[" << i
          << "].query_features_names "
          << absl::StrJoin(other_sig_args.query_features_names(), ", ")
          << " != " << absl::StrJoin(arg_sig_args.query_features_names(), ", ");
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
  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "test" }
        postgresql_name_paths: { name_path: "pg" name_path: "test" }
        signatures: {
          return_type: { oid: 16 }
          arguments: {
            type: { oid: 2276 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
          }
          arguments: {
            type: { oid: 2277 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
          }
          arguments: {
            type: { oid: 2283 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
          }
          postgresql_name_paths: { name_path: "pg" name_path: "test" }
        }
        mode: AGGREGATE
      )pb",
      &fn));

  ZETASQL_ASSERT_OK(mapper_->ToPostgresFunctionArguments(fn));
}

TEST_F(TestSpangresFunctionMapper, MapsFunctionWithMultipleSignatures) {
  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "regexp_contains" }
        postgresql_name_paths: { name_path: "pg" name_path: "textregexeq" }
        signatures: {
          return_type: { oid: 16 }
          arguments: {
            type: { oid: 25 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
            name: "required_arg1"
          }
          arguments: {
            type: { oid: 25 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
            name: "required_arg2"
          }
          postgresql_name_paths: { name_path: "pg" name_path: "textregexeq" }
        }
        signatures: {
          return_type: { oid: 16 }
          arguments: {
            type: { oid: 2276 }
            cardinality: REPEATED
            named_argument_kind: POSITIONAL_ONLY
          }
          postgresql_name_paths: { name_path: "pg" name_path: "textregexeq" }
          deprecated: true
        }
        signatures: {
          return_type: { oid: 16 }
          arguments: {
            type: { oid: 25 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
          }
          arguments: {
            type: { oid: 25 }
            cardinality: OPTIONAL
            named_argument_kind: POSITIONAL_OR_NAMED
            name: "optional_arg"
          }
          postgresql_name_paths: { name_path: "pg" name_path: "textregexeq" }
        }
        mode: SCALAR
      )pb",
      &fn));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PostgresFunctionArguments> result,
                       mapper_->ToPostgresFunctionArguments(fn));
  ASSERT_THAT(result.size(), Eq(1));

  EXPECT_THAT(
      result[0],
      PostgresFunctionArgumentsEq(PostgresFunctionArguments(
          "textregexeq", "regexp_contains",
          {PostgresFunctionSignatureArguments(zetasql::FunctionSignature(
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
               zetasql::FunctionSignatureOptions().set_is_deprecated(true))),
           PostgresFunctionSignatureArguments(zetasql::FunctionSignature(
               gsql_bool,
               {gsql_string,
                {gsql_string,
                 zetasql::FunctionArgumentTypeOptions()
                     .set_argument_name(
                         "optional_arg",
                         zetasql::FunctionEnums::POSITIONAL_OR_NAMED)
                     .set_cardinality(zetasql::FunctionEnums::OPTIONAL)}},
               /*context_ptr=*/nullptr))})));
}

TEST_F(TestSpangresFunctionMapper, MapsSpannerNamespacedFunction) {
  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "bit_reverse" }
        postgresql_name_paths: { name_path: "spanner" name_path: "bit_reverse" }
        signatures: {
          return_type: { oid: 20 }
          arguments: {
            type: { oid: 20 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
          }
          arguments: {
            type: { oid: 16 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
          }
          oid: 50001
          postgresql_name_paths: {
            name_path: "spanner"
            name_path: "bit_reverse"
          }
        }
        signatures: {
          return_type: { oid: 20 }
          arguments: {
            type: { oid: 20 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_ONLY
          }
          oid: 50002
          postgresql_name_paths: {
            name_path: "spanner"
            name_path: "bit_reverse"
          }
        }
        mode: SCALAR
      )pb",
      &fn));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PostgresFunctionArguments> result,
                       mapper_->ToPostgresFunctionArguments(fn));
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

}  // namespace

}  // namespace postgres_translator
