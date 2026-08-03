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

#include "backend/query/remote_udf/remote_udf_evaluator.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "googlesql/public/functions/date_time_util.h"
#include "googlesql/public/interval_value.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/numeric_value.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/uuid_value.h"
#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/base/no_destructor.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "tests/common/test.pb.h"
#include "httplib.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"

using ::emulator::tests::common::Simple;
using ::emulator::tests::common::TestEnum;
using ::testing::HasSubstr;
using ::googlesql_base::testing::IsOkAndHolds;
using ::googlesql_base::testing::StatusIs;

namespace google::spanner::emulator::backend {
namespace {

TEST(RemoteUdfEvaluatorTest, EvaluatePseudoRandomRemoteFunction) {
  absl::SetFlag(&FLAGS_remote_functions_host_port, "");
  auto evaluator = RemoteUdfEvaluator::BuildEvaluator(
      "some_endpoint", "SomeFunction", googlesql::types::Int64Type());
  EXPECT_THAT(evaluator({googlesql::Value::Int64(123)}),
              IsOkAndHolds(googlesql::Value::Int64(-3222588021317909685LL)));
}

TEST(RemoteUdfEvaluatorTest, EvaluateRemoteFunctionSuccess) {
  absl::StatusOr<googlesql::JSONValue> request_body;
  httplib::Server svr;
  svr.Post("/", [&request_body](const httplib::Request& req,
                                httplib::Response& res) {
    request_body = googlesql::JSONValue::ParseJSONString(req.body);
    res.status = 200;
    res.set_content(R"({"replies": [1]})", "application/json");
  });

  int port = svr.bind_to_any_port("localhost");
  std::thread server_thread([&svr]() { svr.listen_after_bind(); });
  svr.wait_until_ready();

  absl::SetFlag(&FLAGS_remote_functions_host_port,
                "localhost:" + std::to_string(port));

  auto evaluator = RemoteUdfEvaluator::BuildEvaluator(
      "some_endpoint", "SomeFunction", googlesql::types::Int64Type());
  EXPECT_THAT(evaluator({googlesql::Value::Int64(456)}),
              IsOkAndHolds(googlesql::Value::Int64(1)));
  EXPECT_THAT(evaluator({googlesql::Value::Int64(123)}),
              IsOkAndHolds(googlesql::Value::Int64(1)));

  svr.stop();
  server_thread.join();

  GOOGLESQL_ASSERT_OK(request_body);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::JSONValue expected_request_body,
                       googlesql::JSONValue::ParseJSONString(R"({
    "_spanner_schema_object":"SomeFunction",
    "_spanner_endpoint":"some_endpoint",
    "caller":"",
    "sessionUser":"",
    "userDefinedContext":{},
    "requestId":"00000000-0000-0000-0000-000000000000",
    "calls":[[123]]})"));
  EXPECT_EQ(request_body->GetConstRef().ToString(),
            expected_request_body.GetConstRef().ToString());
}

TEST(RemoteUdfEvaluatorTest, EvaluateRemoteFunctionFailure_badConnection) {
  absl::SetFlag(&FLAGS_remote_functions_host_port,
                "localhost:" + std::to_string(9999));
  auto evaluator = RemoteUdfEvaluator::BuildEvaluator(
      "some_endpoint", "SomeFunction", googlesql::types::Int64Type());
  EXPECT_THAT(evaluator({googlesql::Value::Int64(123)}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Remote function call failed. Error: 2.")));
}

TEST(RemoteUdfEvaluatorTest, EvaluateRemoteFunctionFailure_badStatus) {
  httplib::Server svr;
  svr.Post("/", [](const httplib::Request& req, httplib::Response& res) {
    res.status = 404;
  });

  int port = svr.bind_to_any_port("localhost");
  std::thread server_thread([&svr]() { svr.listen_after_bind(); });
  svr.wait_until_ready();
  absl::SetFlag(&FLAGS_remote_functions_host_port,
                "localhost:" + std::to_string(port));

  auto evaluator = RemoteUdfEvaluator::BuildEvaluator(
      "some_endpoint", "SomeFunction", googlesql::types::Int64Type());
  EXPECT_THAT(evaluator({googlesql::Value::Int64(123)}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Remote function call failed. Status: 404.")));

  svr.stop();
  server_thread.join();
}

TEST(RemoteUdfEvaluatorTest, EvaluateRemoteFunctionFailure_errorMessage) {
  absl::StatusOr<googlesql::JSONValue> request_body;
  httplib::Server svr;
  svr.Post("/", [&request_body](const httplib::Request& req,
                                httplib::Response& res) {
    request_body = googlesql::JSONValue::ParseJSONString(req.body);
    res.status = 200;
    res.set_content(R"({"errorMessage": "Function execution failed"})",
                    "application/json");
  });

  int port = svr.bind_to_any_port("localhost");
  std::thread server_thread([&svr]() { svr.listen_after_bind(); });
  svr.wait_until_ready();
  absl::SetFlag(&FLAGS_remote_functions_host_port,
                "localhost:" + std::to_string(port));

  auto evaluator = RemoteUdfEvaluator::BuildEvaluator(
      "some_endpoint", "SomeFunction", googlesql::types::Int64Type());
  EXPECT_THAT(evaluator({googlesql::Value::Int64(123)}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Function execution failed")));

  svr.stop();
  server_thread.join();

  GOOGLESQL_ASSERT_OK(request_body);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::JSONValue expected_request_body,
                       googlesql::JSONValue::ParseJSONString(R"({
    "_spanner_schema_object":"SomeFunction",
    "_spanner_endpoint":"some_endpoint",
    "caller":"",
    "sessionUser":"",
    "userDefinedContext":{},
    "requestId":"00000000-0000-0000-0000-000000000000",
    "calls":[[123]]})"));
  EXPECT_EQ(request_body->GetConstRef().ToString(),
            expected_request_body.GetConstRef().ToString());
}

TEST(RemoteUdfEvaluatorTest, EvaluateRemoteFunction_InvalidHostPort) {
  absl::SetFlag(&FLAGS_remote_functions_host_port, "invalid:8080");
  auto evaluator = RemoteUdfEvaluator::BuildEvaluator(
      "some_endpoint", "SomeFunction", googlesql::types::Int64Type());
  EXPECT_THAT(
      evaluator({googlesql::Value::Int64(123)}),
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          HasSubstr("Remote functions can connect only to localhost ports.")));
}

struct RemoteUdfEvaluatorConverterTestParam {
  // GoogleSQL value to convert to JSON.
  std::function<absl::StatusOr<googlesql::Value>()> value;
  // JSON string to convert to GoogleSQL value.
  std::string json_string;
  // Fingerprint of the value.
  uint64_t value_fingerprint;
  // Fingerprint of the JSON string.
  uint64_t json_fingerprint;
  // Value converted from the fingerprint.
  std::function<absl::StatusOr<googlesql::Value>()> value_from_fingerprint;
};

class RemoteUdfProtocolTest
    : public testing::TestWithParam<RemoteUdfEvaluatorConverterTestParam> {
 protected:
  void SetUp() override {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        pg_arena_, postgres_translator::interfaces::CreatePGArena(nullptr));
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(value_, GetParam().value());
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(value_from_fingerprint_,
                         GetParam().value_from_fingerprint());
  }

  std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena_;
  googlesql::Value value_;
  googlesql::Value value_from_fingerprint_;
};

absl::NoDestructor<googlesql::TypeFactory> factory;

absl::NoDestructor<const googlesql::EnumType*> enum_type{[]() {
  const ::googlesql::EnumType* enum_type = nullptr;
  ABSL_CHECK_OK(factory->MakeEnumType(
      ::emulator::tests::common::TestEnum_descriptor(), &enum_type));
  return enum_type;
}()};

absl::NoDestructor<const googlesql::ProtoType*> proto_type{[]() {
  const ::googlesql::ProtoType* proto_type = nullptr;
  ABSL_CHECK_OK(factory->MakeProtoType(
      ::emulator::tests::common::Simple::descriptor(), &proto_type));
  return proto_type;
}()};

absl::NoDestructor<const googlesql::Type*> map_type{[]() {
  googlesql::LanguageOptions language_options;
  language_options.SetEnabledLanguageFeatures({googlesql::FEATURE_MAP_TYPE});
  auto map_type = factory->MakeMapType(factory->get_int64(),
                                       factory->get_string(), language_options);
  ABSL_CHECK_OK(map_type);
  return map_type.value();
}()};

absl::NoDestructor<const googlesql::StructType*> struct_type{[]() {
  const googlesql::StructType* struct_type;
  ABSL_CHECK_OK(factory->MakeStructType(
      {{"a", factory->get_int64()}, {"b", factory->get_int64()}},
      &struct_type));
  return struct_type;
}()};

INSTANTIATE_TEST_SUITE_P(
    RemoteUdfProtocolTest, RemoteUdfProtocolTest,
    testing::ValuesIn(std::vector<RemoteUdfEvaluatorConverterTestParam>{
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Int32(123); },
            .json_string = R"(123)",
            .value_fingerprint = 15224156052391641931ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::Int32(
                      static_cast<int32_t>(15224156052391641931ULL));
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Int64(123); },
            .json_string = R"(123)",
            .value_fingerprint = 15224156052391641931ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::Int64(
                      static_cast<int64_t>(15224156052391641931ULL));
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Uint32(123); },
            .json_string = R"(123)",
            .value_fingerprint = 15224156052391641931ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::Uint32(
                      static_cast<uint32_t>(15224156052391641931ULL));
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Uint64(123); },
            .json_string = R"(123)",
            .value_fingerprint = 15224156052391641931ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::Uint64(
                      static_cast<uint64_t>(15224156052391641931ULL));
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Bool(true); },
            .json_string = R"(true)",
            .value_fingerprint = 10105606910506535461ULL,
            .json_fingerprint = 10105606910506535461ULL,
            .value_from_fingerprint =
                []() { return googlesql::Value::Bool(false); },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Float(123.0f); },
            .json_string = R"(123.0)",
            .value_fingerprint = 4781265650859502840ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() { return googlesql::Value::Float(4.781266e+18f); },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Double(123.0); },
            .json_string = R"(123.0)",
            .value_fingerprint = 4781265650859502840ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::Double(4.7812656508595026e+18);
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::String("test"); },
            .json_string = R"("test")",
            .value_fingerprint = 8581389452482819506ULL,
            .json_fingerprint = 8581389452482819506ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::String("8581389452482819506");
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::Value::Bytes("test"); },
            .json_string = R"("dGVzdA==")",
            .value_fingerprint = 8581389452482819506ULL,
            .json_fingerprint = 13984290191370268188ULL,
            .value_from_fingerprint =
                []() { return googlesql::Value::Bytes("8581389452482819506"); },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return googlesql::Value::Proto(*proto_type, []() {
                    Simple proto;
                    proto.set_field("test");
                    return proto.SerializeAsCord();
                  }());
                },
            .json_string = R"("CgR0ZXN0")",
            .value_fingerprint = 6005505207624538894ULL,
            .json_fingerprint = 13191501998843752632ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::Proto(*proto_type, absl::Cord());
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() { return googlesql::values::Date(123); },
            .json_string = R"("1970-05-04")",
            .value_fingerprint = 4781265650859502840ULL,
            .json_fingerprint = 11143350453112123108ULL,
            .value_from_fingerprint = []() -> absl::StatusOr<googlesql::Value> {
              int32_t date;
              GOOGLESQL_RETURN_IF_ERROR(googlesql::functions::ConvertStringToDate(
                  "5020-10-10", &date));
              return googlesql::values::Date(date);
            },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return googlesql::Value::Enum(*enum_type,
                                                TestEnum::TEST_ENUM_ONE);
                },
            .json_string = R"(1)",
            .value_fingerprint = 9304157803607034849ULL,
            .json_fingerprint = 10105606910506535461ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::Enum(*enum_type,
                                                TestEnum::TEST_ENUM_FOUR);
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return googlesql::values::TimestampFromUnixMicros(123);
                },
            .json_string = R"("1970-01-01T00:00:00.000123Z")",
            .value_fingerprint = 4781265650859502840ULL,
            .json_fingerprint = 11613737481388843643ULL,
            .value_from_fingerprint = []() -> absl::StatusOr<googlesql::Value> {
              absl::Time time;
              GOOGLESQL_RETURN_IF_ERROR(googlesql::functions::ConvertStringToTimestamp(
                  "1528-04-23 19:27:39.502855+00", absl::UTCTimeZone(),
                  googlesql::functions::TimestampScale::kMicroseconds,
                  /*allow_tz_in_str=*/true, &time));
              return googlesql::values::Timestamp(time);
            },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(
                  googlesql::NumericValue numeric_value,
                  googlesql::NumericValue::FromStringStrict("123.456"));
              return googlesql::Value::Numeric(numeric_value);
            },
            .json_string = R"("123.456")",
            .value_fingerprint = 14021931022696406972ULL,
            .json_fingerprint = 14021931022696406972ULL,
            .value_from_fingerprint = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(googlesql::NumericValue numeric_value,
                               googlesql::NumericValue::FromStringStrict(
                                   "-4424813051013144644"));
              return googlesql::Value::Numeric(numeric_value);
            },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(
                  googlesql::JSONValue json_value,
                  googlesql::JSONValue::ParseJSONString(R"({"foo": 123})"));
              return googlesql::Value::Json(std::move(json_value));
            },
            .json_string = R"({"foo": 123})",
            .value_fingerprint = 15815480037866307082ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue json_value,
                               googlesql::JSONValue::ParseJSONString(
                                   R"(15815480037866307082)"));
              return googlesql::Value::Json(std::move(json_value));
            },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return postgres_translator::spangres::datatypes::
                      CreatePgJsonbValue(R"({"foo": 123})");
                },
            .json_string = R"({"foo": 123})",
            .value_fingerprint = 3805850250135807630ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() {
                  return postgres_translator::spangres::datatypes::
                      CreatePgJsonbValue(R"(3805850250135807630)");
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return postgres_translator::spangres::datatypes::
                      CreatePgNumericValue("123.456");
                },
            .json_string = R"("123.456")",
            .value_fingerprint = 14021931022696406972ULL,
            .json_fingerprint = 14021931022696406972ULL,
            .value_from_fingerprint =
                []() {
                  return postgres_translator::spangres::datatypes::
                      CreatePgNumericValue("14021931022696406972");
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return postgres_translator::spangres::datatypes::
                      CreatePgOidValue(1);
                },
            .json_string = R"(1)",
            .value_fingerprint = 9304157803607034849ULL,
            .json_fingerprint = 10105606910506535461ULL,
            .value_from_fingerprint =
                []() {
                  return postgres_translator::spangres::datatypes::
                      CreatePgOidValue(304942049);
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue interval_value,
                               googlesql::IntervalValue::FromMicros(1000));
              return googlesql::Value::Interval(interval_value);
            },
            .json_string = R"("PT0.001S")",
            .value_fingerprint = 17869735116747004378ULL,
            .json_fingerprint = 17480006099935747595ULL,
            .value_from_fingerprint = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(
                  googlesql::IntervalValue interval_value,
                  googlesql::IntervalValue::ParseFromString(
                      "0-0 0 44775310:12:27.004378", /*allow_nanos=*/true));
              return googlesql::Value::Interval(interval_value);
            },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(googlesql::UuidValue uuid_value,
                               googlesql::UuidValue::FromString(
                                   "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));
              return googlesql::Value::Uuid(uuid_value);
            },
            .json_string = R"("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")",
            .value_fingerprint = 17648304764822539760ULL,
            .json_fingerprint = 17648304764822539760ULL,
            .value_from_fingerprint = []() -> absl::StatusOr<googlesql::Value> {
              GOOGLESQL_ASSIGN_OR_RETURN(googlesql::UuidValue uuid_value,
                               googlesql::UuidValue::FromString(
                                   "f4eb5fad-cb14-cdf0-f4eb-5fadcb14cdf0"));
              return googlesql::Value::Uuid(uuid_value);
            },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return googlesql::Value::MakeArray(
                      googlesql::types::Int64ArrayType(),
                      {googlesql::Value::Int64(123)});
                },
            .json_string = R"([123])",
            .value_fingerprint = 15224156052391641931ULL,
            .json_fingerprint = 4781265650859502840ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::MakeArray(
                      googlesql::types::Int64ArrayType(),
                      {googlesql::Value::Int64(
                          static_cast<int64_t>(15224156052391641931ULL))});
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return googlesql::Value::MakeMap(
                      *map_type, {{googlesql::Value::Int64(123),
                                   googlesql::Value::String("abc")}});
                },
            .json_string = R"([{"key": 123, "value": "abc"}])",
            .value_fingerprint = 16242630021597387067ULL,
            .json_fingerprint = 7421979909119664225ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::MakeMap(
                      *map_type,
                      {{googlesql::Value::Int64(-2204114052112164549),
                        googlesql::Value::String("16242630021597387067")}});
                },
        },
        RemoteUdfEvaluatorConverterTestParam{
            .value =
                []() {
                  return googlesql::Value::MakeStruct(
                      *struct_type, {googlesql::Value::Int64(123),
                                     googlesql::Value::Int64(456)});
                },
            .json_string = R"({"a": 123, "b": 456})",
            .value_fingerprint = 8071332180613873137ULL,
            .json_fingerprint = 12858441415027808065ULL,
            .value_from_fingerprint =
                []() {
                  return googlesql::Value::MakeStruct(
                      *struct_type,
                      {googlesql::Value::Int64(
                           static_cast<int64_t>(8071332180613873137ULL)),
                       googlesql::Value::Int64(
                           static_cast<int64_t>(8071332180613873137ULL))});
                },
        }}));

TEST_P(RemoteUdfProtocolTest, ValueFingerprint) {
  EXPECT_THAT(RemoteUdfProtocol::Fingerprint(value_),
              IsOkAndHolds(GetParam().value_fingerprint))
      << value_.DebugString(/*verbose=*/true);
}

TEST_P(RemoteUdfProtocolTest, ValueFingerprint_Null) {
  EXPECT_THAT(
      RemoteUdfProtocol::Fingerprint(googlesql::Value::Null(value_.type())),
      IsOkAndHolds(0))
      << value_.DebugString(/*verbose=*/true);
}

TEST_P(RemoteUdfProtocolTest, JSONFingerprint) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::JSONValue json_value,
      googlesql::JSONValue::ParseJSONString(GetParam().json_string));
  EXPECT_THAT(RemoteUdfProtocol::Fingerprint(json_value.GetConstRef()),
              IsOkAndHolds(GetParam().json_fingerprint))
      << value_.DebugString(/*verbose=*/true)
      << " json: " << GetParam().json_string;
}

TEST_P(RemoteUdfProtocolTest, ToValueFromFingerprint) {
  EXPECT_THAT(
      RemoteUdfProtocol::ToValue(GetParam().value_fingerprint, value_.type()),
      IsOkAndHolds(value_from_fingerprint_))
      << value_.DebugString(/*verbose=*/true)
      << " value fingerprint: " << GetParam().value_fingerprint;
}

TEST_P(RemoteUdfProtocolTest, ToValue) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::JSONValue json_value,
      googlesql::JSONValue::ParseJSONString(GetParam().json_string));
  EXPECT_THAT(
      RemoteUdfProtocol::ToValue(json_value.GetConstRef(), value_.type()),
      IsOkAndHolds(value_))
      << value_.DebugString(/*verbose=*/true)
      << " json: " << GetParam().json_string;
}

TEST_P(RemoteUdfProtocolTest, ToValue_Null) {
  googlesql::JSONValue json_value;
  json_value.GetRef().SetNull();
  EXPECT_THAT(
      RemoteUdfProtocol::ToValue(json_value.GetConstRef(), value_.type()),
      IsOkAndHolds(googlesql::Value::Null(value_.type())))
      << value_.DebugString(/*verbose=*/true)
      << " json: " << GetParam().json_string;
}

TEST_P(RemoteUdfProtocolTest, ToJson) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::JSONValue json_value,
                       RemoteUdfProtocol::ToJson(value_));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::JSONValue expected_json_value,
      googlesql::JSONValue::ParseJSONString(GetParam().json_string));
  EXPECT_EQ(json_value.GetConstRef().ToString(),
            expected_json_value.GetConstRef().ToString())
      << value_.DebugString(/*verbose=*/true)
      << " json: " << GetParam().json_string;
}

TEST_P(RemoteUdfProtocolTest, ToJson_Null) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::JSONValue json_value,
      RemoteUdfProtocol::ToJson(googlesql::Value::Null(value_.type())));

  googlesql::JSONValue expected_json_value;
  expected_json_value.GetRef().SetNull();
  EXPECT_EQ(json_value.GetConstRef().ToString(),
            expected_json_value.GetConstRef().ToString())
      << value_.DebugString(/*verbose=*/true)
      << " json: " << GetParam().json_string;
}

}  // namespace
}  // namespace google::spanner::emulator::backend
