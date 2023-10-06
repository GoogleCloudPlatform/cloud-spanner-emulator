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

#include "third_party/spanner_pg/shims/ereport_shim.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/log/scoped_mock_log.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator {
namespace test {
namespace {

using ::testing::Optional;
using ::zetasql_base::testing::StatusIs;

// Checks that most fields on an ErrorData are empty.
// DELIBERATELY SKIPS elevel, message, and sqlerrcode
// so that tests can check those commonly-set fields separately.
void ValidateMostlyEmptyErrorData(const ErrorData &error_data) {
  EXPECT_EQ(error_data.output_to_server, false);
  EXPECT_EQ(error_data.output_to_client, false);
  EXPECT_EQ(error_data.hide_stmt, false);
  EXPECT_EQ(error_data.hide_ctx, false);
  EXPECT_EQ(error_data.filename, nullptr);
  EXPECT_EQ(error_data.lineno, 0);
  EXPECT_EQ(error_data.funcname, nullptr);
  EXPECT_EQ(error_data.domain, nullptr);
  EXPECT_EQ(error_data.context_domain, nullptr);
  EXPECT_EQ(error_data.detail, nullptr);
  EXPECT_EQ(error_data.detail_log, nullptr);
  EXPECT_EQ(error_data.hint, nullptr);
  EXPECT_EQ(error_data.context, nullptr);
  EXPECT_EQ(error_data.message_id, nullptr);
  EXPECT_EQ(error_data.schema_name, nullptr);
  EXPECT_EQ(error_data.table_name, nullptr);
  EXPECT_EQ(error_data.column_name, nullptr);
  EXPECT_EQ(error_data.datatype_name, nullptr);
  EXPECT_EQ(error_data.constraint_name, nullptr);
  EXPECT_EQ(error_data.cursorpos, 0);
  EXPECT_EQ(error_data.internalpos, 0);
  EXPECT_EQ(error_data.internalquery, nullptr);
  EXPECT_EQ(error_data.saved_errno, 0);
  EXPECT_EQ(error_data.assoc_context, nullptr);
}

TEST(PostgresEreportException, EmptyEreport) {
  ErrorData error_data = {};

  bool caught_exception = false;

  try {
    throw PostgresEreportException(&error_data);
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    ValidateMostlyEmptyErrorData(exc.error_data());
    EXPECT_EQ(exc.error_data().elevel, 0);
    EXPECT_EQ(exc.error_data().message, nullptr);
    EXPECT_EQ(exc.error_data().sqlerrcode, 0);
    EXPECT_EQ(exc.error_status(), absl::nullopt);
  }

  EXPECT_TRUE(caught_exception);
}

TEST(PostgresEreportException, ThrowWithStatus) {
  absl::Status err = absl::InvalidArgumentError("Error Status");
  std::string error_message = "Error Message";
  // Relies on std::string::data() being null-terminated.
  // (Can't use .c_str() because .message isn't const.)
  ErrorData error_data = {
    .elevel = 42,
    .sqlerrcode = 21,
    .message = error_message.data(),
  };

  bool caught_exception = false;

  try {
    throw PostgresEreportException(&error_data, err);
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    ValidateMostlyEmptyErrorData(exc.error_data());
    EXPECT_EQ(exc.error_data().elevel, 42);
    EXPECT_EQ(exc.error_data().message, error_message);
    // Message should be copied so pointers aren't the same
    EXPECT_NE(exc.error_data().message, error_message.data());
    EXPECT_EQ(exc.error_data().sqlerrcode, 21);
    EXPECT_THAT(
        exc.error_status(),
        Optional(StatusIs(absl::StatusCode::kInvalidArgument, "Error Status")));
  }

  EXPECT_TRUE(caught_exception);
}

TEST(PostgresEreportException, EmptyThrow) {
  ErrorData error_data = {};

  std::string error_message = "Error Message";
  // Relies on std::string::data() being null-terminated.
  // (Can't use .c_str() because .message isn't const.)
  error_data.message = error_message.data();

  error_data.elevel = 42;
  error_data.sqlerrcode = 21;

  bool caught_exception = false;

  try {
    spangres_throw_exception(&error_data);
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    ValidateMostlyEmptyErrorData(exc.error_data());
    EXPECT_EQ(exc.error_data().elevel, 42);
    EXPECT_EQ(exc.error_data().message, error_message);
    // Message should be copied so pointers aren't the same
    EXPECT_NE(exc.error_data().message, error_message.data());
    EXPECT_EQ(exc.error_data().sqlerrcode, 21);
    EXPECT_EQ(exc.error_status(), absl::nullopt);
  }

  EXPECT_TRUE(caught_exception);
}

using PostgresEreportExceptionWithMemory =
    ::postgres_translator::test::ValidMemoryContext;
TEST_F(PostgresEreportExceptionWithMemory, LegacyElog) {
  bool caught_exception = false;
  try {
    // Workaround:  Define global constant used internally by elog() macro
    constexpr int ERROR = PgErrorLevel::PG_ERROR;
    elog(ERROR, "Error Message %s", "pg.unknown_func");
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    EXPECT_EQ(exc.error_data().elevel, PgErrorLevel::PG_ERROR);
    EXPECT_STREQ(exc.error_data().message, "Error Message pg.unknown_func");
    EXPECT_STREQ(exc.error_data().message_id, "Error Message %s");
    EXPECT_STREQ(exc.what(), "[ERROR] Error Message pg.unknown_func");
  }

  EXPECT_TRUE(caught_exception);
}

TEST_F(PostgresEreportExceptionWithMemory, ErrorMessageStrings) {
  bool caught_exception = false;
  try {
    // Workaround:  Define global constant used internally by ereport() macro
    constexpr int ERROR = PgErrorLevel::PG_ERROR;
    ereport(
        ERROR,
        (errmsg("%s", "Error Message"), errdetail("%s", "Error Detail"),
         errdetail_log("%s", "Error Detail Log"), errhint("%s", "Error Hint"),
         errcontext_msg("%s", "Error Context Msg")));
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    EXPECT_EQ(exc.error_data().elevel, PgErrorLevel::PG_ERROR);
    EXPECT_STREQ(exc.error_data().message, "Error Message");
    EXPECT_STREQ(exc.error_data().message_id, "%s");
    EXPECT_STREQ(exc.error_data().detail, "Error Detail");
    EXPECT_STREQ(exc.error_data().detail_log, "Error Detail Log");
    EXPECT_STREQ(exc.error_data().hint, "Error Hint");
    EXPECT_STREQ(exc.error_data().context, "Error Context Msg");
    EXPECT_STREQ(exc.what(), R"([ERROR] Error Message
Detail: Error Detail
Detail Log: Error Detail Log
Hint: Error Hint
Context Message: Error Context Msg)");
  }

  EXPECT_TRUE(caught_exception);
}

TEST_F(PostgresEreportExceptionWithMemory, ErrorMessagePluralStrings) {
  bool caught_exception = false;

  int n_widgets = 1;

  try {
    // Workaround:  Define global constant used internally by ereport() macro
    constexpr int ERROR = PgErrorLevel::PG_ERROR;
    ereport(ERROR,
            (errmsg_plural("%s", "%ss", n_widgets, "Error Message"),
             errdetail_plural("%s", "%ss", n_widgets, "Error Detail"),
             errdetail_log_plural("%s", "%ss", n_widgets, "Error Detail Log")));
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    EXPECT_EQ(exc.error_data().elevel, PgErrorLevel::PG_ERROR);
    EXPECT_STREQ(exc.error_data().message, "Error Message");
    EXPECT_STREQ(exc.error_data().message_id, "%s");
    EXPECT_STREQ(exc.error_data().detail, "Error Detail");
    EXPECT_STREQ(exc.error_data().detail_log, "Error Detail Log");
  }

  EXPECT_TRUE(caught_exception);

  caught_exception = false;
  n_widgets = 2;

  try {
    // Workaround:  Define global constant used internally by ereport() macro
    constexpr int ERROR = PgErrorLevel::PG_ERROR;
    ereport(ERROR,
            (errmsg_plural("%s", "%ss", n_widgets, "Error Message"),
             errdetail_plural("%s", "%ss", n_widgets, "Error Detail"),
             errdetail_log_plural("%s", "%ss", n_widgets, "Error Detail Log")));
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    EXPECT_EQ(exc.error_data().elevel, PgErrorLevel::PG_ERROR);
    EXPECT_STREQ(exc.error_data().message, "Error Messages");
    EXPECT_STREQ(exc.error_data().message_id, "%s");
    EXPECT_STREQ(exc.error_data().detail, "Error Details");
    EXPECT_STREQ(exc.error_data().detail_log, "Error Detail Logs");
  }
}

TEST_F(PostgresEreportExceptionWithMemory, ErrorMessageInternalStrings) {
  // "*_internal()" error-message functions skip translation and localization.
  // We don't support those in Spangres right now anyway.
  // Update this test (and the tests above) if we add support.
  bool caught_exception = false;
  try {
    // Workaround:  Define global constant used internally by ereport() macro
    constexpr int ERROR = PgErrorLevel::PG_ERROR;
    ereport(ERROR, (errmsg_internal("%s", "Error Message"),
                    errdetail_internal("%s", "Error Detail")));
  } catch (const PostgresEreportException &exc) {
    caught_exception = true;
    EXPECT_EQ(exc.error_data().elevel, PgErrorLevel::PG_ERROR);
    EXPECT_STREQ(exc.error_data().message, "Error Message");
    EXPECT_STREQ(exc.error_data().message_id, "%s");
    EXPECT_STREQ(exc.error_data().detail, "Error Detail");
  }

  EXPECT_TRUE(caught_exception);
}

using ::testing::HasSubstr;
TEST_F(PostgresEreportExceptionWithMemory, WarningLogsButDoesntThrow) {
  constexpr char kTestMessage[] = "test error message";
  // Workaround:  Define global constant used internally by ereport() macro
  constexpr int ERROR = PgErrorLevel::PG_ERROR;
  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  EXPECT_CALL(log, Log(absl::LogSeverity::kWarning, testing::_,
                       HasSubstr(kTestMessage)))
      .Times(1);
  log.StartCapturingLogs();

  EXPECT_NO_THROW(ereport(PG_WARNING, (errmsg(kTestMessage))));
}

TEST_F(PostgresEreportExceptionWithMemory, NoticeLogsButDoesntThrow) {
  constexpr char kTestMessage[] = "test error message";
  // Workaround:  Define global constant used internally by ereport() macro
  constexpr int ERROR = PgErrorLevel::PG_ERROR;
  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  EXPECT_CALL(
      log, Log(absl::LogSeverity::kInfo, testing::_, HasSubstr(kTestMessage)))
      .Times(1);
  log.StartCapturingLogs();

  EXPECT_NO_THROW(ereport(PG_NOTICE, (errmsg(kTestMessage))));
}

}  // namespace
}  // namespace test
}  // namespace postgres_translator
