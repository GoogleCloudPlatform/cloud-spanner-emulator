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

#include "backend/query/ml/model_evaluator.h"

#include <memory>
#include <string>
#include <thread>  // NOLINT

#include "googlesql/public/json_value.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/case.h"
#include "backend/query/queryable_model.h"
#include "backend/query/remote_udf/remote_udf_evaluator.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/catalog/schema.h"
#include "tests/common/schema_constructor.h"
#include "httplib.h"

using ::testing::HasSubstr;
using ::googlesql_base::testing::StatusIs;

namespace google::spanner::emulator::backend {
namespace {

absl::StatusOr<std::unique_ptr<const Schema>> CreateModelSchema() {
  googlesql::TypeFactory type_factory;
  return test::CreateSchemaFromDDL({R"(
        CREATE MODEL test_model
        INPUT (
          in_a INT64,
          in_b STRING(MAX) OPTIONS (required = false)
        )
        OUTPUT (
          out_c INT64,
          out_d STRING(MAX)
        )
        REMOTE OPTIONS (
          endpoint = 'test_endpoint'
        )
      )"},
                                   &type_factory);
}

class ModelEvaluatorTest : public testing::Test {
 public:
  void SetUp() override {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema_, CreateModelSchema());
    model_ = schema_->FindModel("test_model");
    ASSERT_NE(model_, nullptr);
    queryable_model_ = std::make_unique<QueryableModel>(model_);

    model_inputs_ = {{"in_a", &in_a_val_}, {"in_b", &in_b_val_}};
    model_params_ = {{"p", &param_val_}};
    model_outputs_ = {{"out_c", &out_c_val_}, {"out_d", &out_d_val_}};
  }

 protected:
  googlesql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;
  const backend::Model* model_;
  std::unique_ptr<QueryableModel> queryable_model_;

  googlesql::Value in_a_val_ = googlesql::Value::Int64(123);
  googlesql::Value in_b_val_ = googlesql::Value::String("test");
  googlesql::Value out_c_val_;
  googlesql::Value out_d_val_;
  googlesql::Value param_val_ = googlesql::Value::Int64(456);

  CaseInsensitiveStringMap<const googlesql::Value*> model_inputs_;
  CaseInsensitiveStringMap<const googlesql::Value*> model_params_;
  CaseInsensitiveStringMap<googlesql::Value*> model_outputs_;
};  // namespace

TEST_F(ModelEvaluatorTest, DefaultPredict) {
  absl::SetFlag(&FLAGS_remote_functions_host_port, "");

  GOOGLESQL_EXPECT_OK(ModelEvaluator::Predict(queryable_model_.get(), model_inputs_,
                                    model_params_, model_outputs_));
  EXPECT_EQ(out_c_val_, googlesql::Value::Int64(5358801431164909821ULL));
  EXPECT_EQ(out_d_val_, googlesql::Value::String("5358801431164909821"));
}

TEST_F(ModelEvaluatorTest, RemotePredictSuccess) {
  absl::StatusOr<googlesql::JSONValue> request_body;
  httplib::Server svr;
  svr.Post("/", [&request_body](const httplib::Request& req,
                                httplib::Response& res) {
    request_body = googlesql::JSONValue::ParseJSONString(req.body);
    res.set_content(R"({"replies": [{"out_c": 456, "out_d": "result"}]})",
                    "application/json");
  });
  int port = svr.bind_to_any_port("localhost");
  std::thread server_thread([&svr]() { svr.listen_after_bind(); });
  svr.wait_until_ready();

  absl::SetFlag(&FLAGS_remote_functions_host_port,
                "localhost:" + std::to_string(port));

  GOOGLESQL_EXPECT_OK(ModelEvaluator::Predict(queryable_model_.get(), model_inputs_,
                                    model_params_, model_outputs_));
  EXPECT_EQ(out_c_val_, googlesql::Value::Int64(456));
  EXPECT_EQ(out_d_val_, googlesql::Value::String("result"));

  svr.stop();
  server_thread.join();

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::JSONValue expected_request_body,
                       googlesql::JSONValue::ParseJSONString(R"json({
    "_spanner_schema_object":"test_model",
    "_spanner_endpoint":"test_endpoint",
    "caller":"",
    "sessionUser":"",
    "userDefinedContext":{},
    "requestId":"00000000-0000-0000-0000-000000000000",
    "calls":[[{"in_a":123,"in_b":"test"}, {"p": 456}]]
  })json"));

  GOOGLESQL_ASSERT_OK(request_body);
  EXPECT_EQ(request_body->GetConstRef().ToString(),
            expected_request_body.GetConstRef().ToString());
}

TEST_F(ModelEvaluatorTest, RemotePredictFailure) {
  httplib::Server svr;
  svr.Post("/", [](const httplib::Request& req, httplib::Response& res) {
    res.set_content(R"({"errorMessage": "Prediction failed"})",
                    "application/json");
  });

  int port = svr.bind_to_any_port("localhost");
  std::thread server_thread([&svr]() { svr.listen_after_bind(); });
  svr.wait_until_ready();

  absl::SetFlag(&FLAGS_remote_functions_host_port,
                "localhost:" + std::to_string(port));

  EXPECT_THAT(ModelEvaluator::Predict(queryable_model_.get(), model_inputs_,
                                      model_params_, model_outputs_),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Prediction failed")));

  svr.stop();
  server_thread.join();
}

TEST_F(ModelEvaluatorTest, PredictMissingRequiredInput) {
  model_inputs_.erase("in_a");
  EXPECT_THAT(ModelEvaluator::Predict(queryable_model_.get(), model_inputs_,
                                      model_params_, model_outputs_),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Missing input column: in_a")));
}

TEST_F(ModelEvaluatorTest, PredictMissingOptionalInput) {
  model_inputs_.erase("in_b");
  GOOGLESQL_EXPECT_OK(ModelEvaluator::Predict(queryable_model_.get(), model_inputs_,
                                    model_params_, model_outputs_));
  EXPECT_EQ(out_c_val_, googlesql::Value::Int64(-3222588021317909685LL));
  EXPECT_EQ(out_d_val_, googlesql::Value::String("15224156052391641931"));
}

TEST_F(ModelEvaluatorTest, PgPredictDefault) {
  absl::SetFlag(&FLAGS_remote_functions_host_port, "");

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::JSONValue instance,
      googlesql::JSONValue::ParseJSONString(R"({"in_a": 123})"));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::JSONValue parameters,
                       googlesql::JSONValue::ParseJSONString(R"({})"));
  googlesql::JSONValue prediction;

  GOOGLESQL_EXPECT_OK(ModelEvaluator::PgPredict("test_endpoint", instance.GetConstRef(),
                                      parameters.GetConstRef(),
                                      prediction.GetRef()));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::JSONValue expected_prediction,
      googlesql::JSONValue::ParseJSONString(R"({"Outcome": true})"));
  EXPECT_EQ(prediction.GetConstRef().ToString(),
            expected_prediction.GetConstRef().ToString());
}

TEST_F(ModelEvaluatorTest, PgPredictRemote) {
  httplib::Server svr;

  absl::StatusOr<googlesql::JSONValue> request_body;
  svr.Post("/", [&request_body](const httplib::Request& req,
                                httplib::Response& res) {
    request_body = googlesql::JSONValue::ParseJSONString(req.body);
    res.set_content(R"({"replies": [{"Outcome": true}]})", "application/json");
  });

  int port = svr.bind_to_any_port("localhost");
  std::thread server_thread([&svr]() { svr.listen_after_bind(); });
  svr.wait_until_ready();

  absl::SetFlag(&FLAGS_remote_functions_host_port,
                "localhost:" + std::to_string(port));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::JSONValue instance,
      googlesql::JSONValue::ParseJSONString(R"({"in_a": 123})"));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::JSONValue parameters,
                       googlesql::JSONValue::ParseJSONString(R"({})"));
  googlesql::JSONValue prediction;

  GOOGLESQL_EXPECT_OK(ModelEvaluator::PgPredict("test_endpoint", instance.GetConstRef(),
                                      parameters.GetConstRef(),
                                      prediction.GetRef()));

  EXPECT_TRUE(prediction.GetConstRef().IsObject());
  EXPECT_TRUE(prediction.GetConstRef().HasMember("Outcome"));
  EXPECT_TRUE(prediction.GetConstRef().GetMember("Outcome").GetBoolean());

  svr.stop();
  server_thread.join();

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::JSONValue expected_request_body,
                       googlesql::JSONValue::ParseJSONString(R"({
    "_spanner_schema_object":"",
    "_spanner_endpoint":"test_endpoint",
    "caller":"",
    "sessionUser":"",
    "userDefinedContext":{},
    "requestId":"00000000-0000-0000-0000-000000000000",
    "calls":[[{"in_a":123}, {}]]
  })"));

  GOOGLESQL_ASSERT_OK(request_body);
  EXPECT_EQ(request_body->GetConstRef().ToString(),
            expected_request_body.GetConstRef().ToString());
}

}  // namespace
}  // namespace google::spanner::emulator::backend
