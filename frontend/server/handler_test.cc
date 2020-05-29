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

#include "frontend/server/handler.h"

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

// Example handler to test unary gRPC method registration.
absl::Status CreateSession(
    RequestContext* ctx,
    const google::spanner::v1::CreateSessionRequest* request,
    google::spanner::v1::Session* response) {
  response->set_name("Hello, World!");
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, CreateSession);

TEST(HandlerRegisterer, RegistersUnaryGRPCHandler) {
  // Check that we can get a handler.
  auto handler =
      dynamic_cast<UnaryGRPCHandler<google::spanner::v1::CreateSessionRequest,
                                    google::spanner::v1::Session>*>(
          GetHandler("Spanner", "CreateSession"));
  ASSERT_NE(nullptr, handler);

  // Check that the handler is the one we registered.
  RequestContext ctx(nullptr, nullptr);
  google::spanner::v1::CreateSessionRequest request;
  google::spanner::v1::Session response;
  ZETASQL_EXPECT_OK(handler->Run(&ctx, &request, &response));
  EXPECT_EQ("Hello, World!", response.name());
}

// Test ServerWriter which just captures the sent messages.
template <class MessageT>
class TestServerWriter : public grpc::ServerWriterInterface<MessageT> {
 public:
  void SendInitialMetadata() override {}
  bool Write(const MessageT& msg, grpc::WriteOptions options) override {
    messages_.push_back(msg);
    return true;
  }
  const std::vector<MessageT>& messages() { return messages_; }

 private:
  std::vector<MessageT> messages_;
};

// Example handler to test server streaming gRPC method registration.
absl::Status StreamingRead(
    RequestContext* ctx, const google::spanner::v1::ReadRequest* request,
    ServerStream<google::spanner::v1::PartialResultSet>* stream) {
  google::spanner::v1::PartialResultSet prs;
  prs.set_resume_token("Hello");
  stream->Send(prs);
  prs.set_resume_token("World");
  stream->Send(prs);

  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, StreamingRead);

TEST(HandlerRegisterer, RegistersServerStreamingGRPCHandler) {
  // Check that we can get a handler.
  auto handler = dynamic_cast<
      ServerStreamingGRPCHandler<google::spanner::v1::ReadRequest,
                                 google::spanner::v1::PartialResultSet>*>(
      GetHandler("Spanner", "StreamingRead"));
  ASSERT_NE(nullptr, handler);

  // Check that the handler is the one we registered.
  RequestContext ctx(nullptr, nullptr);
  google::spanner::v1::ReadRequest request;
  TestServerWriter<google::spanner::v1::PartialResultSet> writer;

  ZETASQL_EXPECT_OK(handler->Run(&ctx, &request, &writer));
  ASSERT_EQ(2, writer.messages().size());
  EXPECT_EQ("Hello", writer.messages().at(0).resume_token());
  EXPECT_EQ("World", writer.messages().at(1).resume_token());
}

TEST(HandlerRegisterer, ReturnsNullptrForUnrecognizedHandlers) {
  ASSERT_EQ(nullptr, GetHandler("UnknownServer", "UnknownMethod"));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
