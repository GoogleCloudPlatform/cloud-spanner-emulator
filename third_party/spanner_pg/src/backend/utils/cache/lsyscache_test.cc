#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/googletest/googletest/include/gtest/gtest.h"
#include "third_party/spanner_pg/interface/ereport.h"
#include "third_party/spanner_pg/postgres_includes/all.h"  // IWYU pragma: keep
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator {
namespace {
using GetTypsubscriptTest = ::postgres_translator::test::ValidMemoryContext;

TEST_F(GetTypsubscriptTest, GetJsonbOid) {
  // Check to make sure we can subscript the JSONBOID type.
  Oid typid = JSONBOID;
  Oid typelemp;
  RegProcedure handler = get_typsubscript(typid, &typelemp);
  EXPECT_TRUE(OidIsValid(handler));
}

TEST_F(GetTypsubscriptTest, GetInvalidOid) {
  Oid typid = InvalidOid;
  Oid typelemp;
  EXPECT_THROW(get_typsubscript(typid, &typelemp), PostgresEreportException);
}

}  // namespace
}  // namespace postgres_translator
