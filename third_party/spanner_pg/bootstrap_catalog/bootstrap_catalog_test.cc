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

#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace {

using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsSupersetOf;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;
using ::zetasql_base::testing::StatusIs;

using BootstrapCatalogWithShimsTest =
    ::postgres_translator::test::ValidMemoryContext;

absl::StatusOr<std::vector<Oid>> GetProcOids(absl::string_view name) {
  ZETASQL_ASSIGN_OR_RETURN(absl::Span<const FormData_pg_proc* const> procs,
                   PgBootstrapCatalog::Default()->GetProcsByName(name));

  std::vector<Oid> proc_oids;
  for (const FormData_pg_proc* proc : procs) {
    proc_oids.push_back(proc->oid);
  }
  return proc_oids;
}

TEST(BootstrapCatalog, GetCollationByName) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_collation* collation,
      PgBootstrapCatalog::Default()->GetCollationByName("default"));
  EXPECT_EQ(collation->oid, DEFAULT_COLLATION_OID);
}

TEST(BootstrapCatalog, FailedGetCollationByName) {
  EXPECT_THAT(
      PgBootstrapCatalog::Default()->GetCollationByName("FAKECOLLATION"),
      StatusIs(absl::StatusCode::kNotFound));
}

TEST(BootstrapCatalog, GetCollationOid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Oid collation_oid,
                       PgBootstrapCatalog::Default()->GetCollationOid("C"));
  EXPECT_EQ(collation_oid, C_COLLATION_OID);
}

TEST(BootStrapCatalog, FailedGetCollationOid) {
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetCollationOid("FAKECOLLATION"),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST(BootstrapCatalog, GetNamespaceRoundtrip) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid namespace_oid,
      PgBootstrapCatalog::Default()->GetNamespaceOid("pg_catalog"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const char* namespace_name,
      PgBootstrapCatalog::Default()->GetNamespaceName(namespace_oid));
  EXPECT_STREQ(namespace_name, "pg_catalog");
}

TEST(BootstrapCatalog, GetTypeBool) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_type* data,
                       PgBootstrapCatalog::Default()->GetType(BOOLOID));

  ASSERT_NE(data, nullptr);
  EXPECT_STREQ(NameStr(data->typname), "bool");
}

TEST_F(BootstrapCatalogWithShimsTest, GetTypeNameBool) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const char* type_name,
                       PgBootstrapCatalog::Default()->GetTypeName(BOOLOID));
  EXPECT_STREQ(type_name, "bool");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const char* formatted_type_name,
      PgBootstrapCatalog::Default()->GetFormattedTypeName(BOOLOID));
  EXPECT_STREQ(formatted_type_name, "boolean");
}

TEST_F(BootstrapCatalogWithShimsTest, GetTypeNameArray) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const char* type_name,
      PgBootstrapCatalog::Default()->GetTypeName(TEXTARRAYOID));
  EXPECT_STREQ(type_name, "_text");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const char* formatted_type_name,
      PgBootstrapCatalog::Default()->GetFormattedTypeName(TEXTARRAYOID));
  EXPECT_STREQ(formatted_type_name, "text[]");
}

TEST(BootstrapCatalog, GetTypeNameUnknown) {
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetTypeName(0),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST(BootstrapCatalog, GetTypesByNameBool) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Span<const FormData_pg_type* const> type_list,
                       PgBootstrapCatalog::Default()->GetTypesByName("bool"));
  EXPECT_EQ(type_list.size(), 1);
  EXPECT_EQ(type_list.data()[0]->oid, BOOLOID);
}

TEST(BootstrapCatalog, GetTypesByNameUnknown) {
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetTypesByName(""),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST(BootstrapCatalog, GetProcInt8Sum) {
  const Oid sum_oid = 1842;
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_proc* data,
                       PgBootstrapCatalog::Default()->GetProc(sum_oid));

  ASSERT_NE(data, nullptr);
  EXPECT_STREQ(NameStr(data->proname), "int8_sum");
}

TEST(BootstrapCatalog, GetProcProtoInt8Sum) {
  const Oid sum_oid = 1842;
  ZETASQL_ASSERT_OK_AND_ASSIGN(const PgProcData* proc_proto,
                       PgBootstrapCatalog::Default()->GetProcProto(sum_oid));
  ASSERT_NE(proc_proto, nullptr);
  EXPECT_EQ(proc_proto->oid(), sum_oid);
  EXPECT_EQ(proc_proto->proname(), "int8_sum");
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_proc* data,
                       PgBootstrapCatalog::Default()->GetProc(sum_oid));

  // Check that the data in the proto matches the data in the struct.
  ASSERT_NE(data, nullptr);
  EXPECT_STREQ(proc_proto->proname().c_str(), NameStr(data->proname));
  EXPECT_EQ(proc_proto->pronamespace(), data->pronamespace);
  EXPECT_EQ(proc_proto->proowner(), data->proowner);
  EXPECT_EQ(proc_proto->prolang(), data->prolang);
  EXPECT_EQ(proc_proto->procost(), data->procost);
  EXPECT_EQ(proc_proto->provariadic(), data->provariadic);
  EXPECT_EQ(proc_proto->prosupport(), data->prosupport);
  EXPECT_EQ(proc_proto->prokind(), absl::StrFormat("%c", data->prokind));
  EXPECT_EQ(proc_proto->prosecdef(), data->prosecdef);
  EXPECT_EQ(proc_proto->proleakproof(), data->proleakproof);
  EXPECT_EQ(proc_proto->proisstrict(), data->proisstrict);
  EXPECT_EQ(proc_proto->proretset(), data->proretset);
  EXPECT_EQ(proc_proto->provolatile(),
            absl::StrFormat("%c", data->provolatile));
  EXPECT_EQ(proc_proto->proparallel(),
            absl::StrFormat("%c", data->proparallel));
  EXPECT_EQ(proc_proto->pronargs(), data->pronargs);
  EXPECT_EQ(proc_proto->pronargdefaults(), data->pronargdefaults);
  EXPECT_EQ(proc_proto->prorettype(), data->prorettype);
  for (int i = 0; i < data->proargtypes.vl_len_; ++i) {
    EXPECT_EQ(proc_proto->proargtypes(i), data->proargtypes.values[i]);
  }
}

TEST(BootstrapCatalog, GetProcOidInt8Sum) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> int8_sum_oid, GetProcOids("int8_sum"));
  EXPECT_THAT(int8_sum_oid, ElementsAre(1842));
}

TEST(BootstrapCatalog, GetProcOidsLog) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> log_oids, GetProcOids("log"));
  EXPECT_THAT(log_oids, ElementsAre(1340, 1736, 1741));
}

TEST(BootstrapCatalog, GetProcProtoSubstring) {
  const Oid substring_oid = 936;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const PgProcData* proc_proto,
      PgBootstrapCatalog::Default()->GetProcProto(substring_oid));
  ASSERT_NE(proc_proto, nullptr);
  EXPECT_EQ(proc_proto->oid(), substring_oid);
  EXPECT_EQ(proc_proto->proname(), "substring");
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_proc* data,
                       PgBootstrapCatalog::Default()->GetProc(substring_oid));

  // Check that the signature was modified.
  EXPECT_EQ(proc_proto->prorettype(), TEXTOID);
  EXPECT_THAT(proc_proto->proargtypes(),
              ElementsAre(TEXTOID, INT8OID, INT8OID));

  // Check that the data in the proto matches the data in the struct.
  ASSERT_NE(data, nullptr);
  EXPECT_STREQ(proc_proto->proname().c_str(), NameStr(data->proname));
  EXPECT_EQ(proc_proto->pronamespace(), data->pronamespace);
  EXPECT_EQ(proc_proto->proowner(), data->proowner);
  EXPECT_EQ(proc_proto->prolang(), data->prolang);
  EXPECT_EQ(proc_proto->procost(), data->procost);
  EXPECT_EQ(proc_proto->provariadic(), data->provariadic);
  EXPECT_EQ(proc_proto->prosupport(), data->prosupport);
  EXPECT_EQ(proc_proto->prokind(), absl::StrFormat("%c", data->prokind));
  EXPECT_EQ(proc_proto->prosecdef(), data->prosecdef);
  EXPECT_EQ(proc_proto->proleakproof(), data->proleakproof);
  EXPECT_EQ(proc_proto->proisstrict(), data->proisstrict);
  EXPECT_EQ(proc_proto->proretset(), data->proretset);
  EXPECT_EQ(proc_proto->provolatile(),
            absl::StrFormat("%c", data->provolatile));
  EXPECT_EQ(proc_proto->proparallel(),
            absl::StrFormat("%c", data->proparallel));
  EXPECT_EQ(proc_proto->pronargs(), data->pronargs);
  EXPECT_EQ(proc_proto->pronargdefaults(), data->pronargdefaults);
  EXPECT_EQ(proc_proto->prorettype(), data->prorettype);
  for (int i = 0; i < data->proargtypes.vl_len_; ++i) {
    EXPECT_EQ(proc_proto->proargtypes(i), data->proargtypes.values[i]);
  }
}

TEST(BootstrapCatalog, GetProcOidSubstring) {
  std::vector<Oid> argument_types{TEXTOID, INT8OID, INT8OID};
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid substring_oid,
                       PgBootstrapCatalog::Default()->GetProcOid(
                           "pg_catalog", "substring", argument_types));
  EXPECT_EQ(substring_oid, 936);
}

TEST_F(BootstrapCatalogWithShimsTest, GetProcOidInvalidSignature) {
  // Valid types with an invalid  signatures.
  std::vector<Oid> argument_types{TEXTOID, BOOLOID};
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetProcOid(
                  "pg_catalog", "substring", argument_types),
              StatusIs(absl::StatusCode::kNotFound, HasSubstr("text, bool")));

  // Invalid types.
  std::vector<Oid> invalid_types{TEXTOID, 99};
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetProcOid(
                  "pg_catalog", "substring", invalid_types),
              StatusIs(absl::StatusCode::kNotFound, HasSubstr("25, 99")));
}

TEST_F(BootstrapCatalogWithShimsTest, GetProcOidInvalidNamespace) {
  std::vector<Oid> argument_types{TEXTOID, INT8OID, INT8OID};
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetProcOid("public", "substring",
                                                        argument_types),
              StatusIs(absl::StatusCode::kNotFound, HasSubstr("public")));
}

TEST(BootstrapCatalog, GetCastInt4ToInt8) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_cast* int4_to_int8,
      PgBootstrapCatalog::Default()->GetCast(INT4OID, INT8OID));

  ASSERT_NE(int4_to_int8, nullptr);
  EXPECT_EQ(int4_to_int8->castsource, INT4OID);
  EXPECT_EQ(int4_to_int8->casttarget, INT8OID);
  // Casting to larger size is implicit.
  EXPECT_EQ(int4_to_int8->castcontext, 'i');
}

TEST(BootstrapCatalog, GetCastFloat8ToFloat4) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_cast* float8_to_float4,
      PgBootstrapCatalog::Default()->GetCast(FLOAT8OID, FLOAT4OID));

  ASSERT_NE(float8_to_float4, nullptr);
  EXPECT_EQ(float8_to_float4->castsource, FLOAT8OID);
  EXPECT_EQ(float8_to_float4->casttarget, FLOAT4OID);
  // Casting to smaller size is assignment only.
  EXPECT_EQ(float8_to_float4->castcontext, 'a');
}

TEST(BootstrapCatalog, GetCastByFunctionOidInt4ToInt8) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_cast* int4_to_int8,
      PgBootstrapCatalog::Default()->GetCastByFunctionOid(481));
  ASSERT_NE(int4_to_int8, nullptr);
  EXPECT_EQ(int4_to_int8->castsource, INT4OID);
  EXPECT_EQ(int4_to_int8->casttarget, INT8OID);
}

TEST(BootstrapCatalog, GetCastByFunctionOidNotFound) {
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetCastByFunctionOid(0),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST(BootstrapCatalog, GetOperatorEquals) {
  // Comes from pg_operator.h, which does not directly define this value.
  // Rather, it is bootstrap data for the postgres database that we use here for
  // a test value. In postgres, this value never exists as a C value, only a
  // database entry.
  constexpr Oid equals_oid = 96;

  // Get the Oid of the proc named "int4eq" for comparison below.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> proc_oid_list, GetProcOids("int4eq"));
  ASSERT_EQ(proc_oid_list.size(), 1);
  const Oid equals_proc_oid = proc_oid_list[0];

  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_operator* equals_data,
                       PgBootstrapCatalog::Default()->GetOperator(equals_oid));

  ASSERT_NE(equals_data, nullptr);
  EXPECT_STREQ(NameStr(equals_data->oprname), "=");
  EXPECT_EQ(equals_data->oprkind, 'b');   // Binary operator.
  EXPECT_TRUE(equals_data->oprcanmerge);  // Equals is usable for merge join.
  EXPECT_TRUE(equals_data->oprcanhash);   // Equals is usable for hash join.
  EXPECT_EQ(equals_data->oprleft, INT4OID);
  EXPECT_EQ(equals_data->oprright, INT4OID);
  EXPECT_EQ(equals_data->oprresult, BOOLOID);
  EXPECT_EQ(equals_data->oprcode, equals_proc_oid);
}

TEST(BootstrapCatalog, GetOperatorNegation) {
  // Comes from pg_operator.h, which does not directly define this value.
  // Rather, it is bootstrap data for the postgres database that we use here for
  // a test value. In postgres, this value never exists as a C value, only a
  // database entry.
  constexpr Oid negation_oid = 484;

  // Get the Oid of the proc named "int8um" for comparison below.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> proc_oid_list, GetProcOids("int8um"));
  ASSERT_EQ(proc_oid_list.size(), 1);
  const Oid negation_proc_oid = proc_oid_list[0];

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_operator* negation_data,
      PgBootstrapCatalog::Default()->GetOperator(negation_oid));

  ASSERT_NE(negation_data, nullptr);
  EXPECT_STREQ(NameStr(negation_data->oprname), "-");
  EXPECT_EQ(negation_data->oprkind, 'l');  // Left unary operator.
  EXPECT_FALSE(
      negation_data->oprcanmerge);  // Negation is usable for merge join.
  EXPECT_FALSE(negation_data->oprcanhash);  // Negation is usable for hash join.
  EXPECT_EQ(negation_data->oprleft, 0);
  EXPECT_EQ(negation_data->oprright, INT8OID);
  EXPECT_EQ(negation_data->oprresult, INT8OID);
  EXPECT_EQ(negation_data->oprcode, negation_proc_oid);
}

TEST(BootstrapCatalog, GetOperatorOidsPercent) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Span<const Oid> percent_oids,
                       PgBootstrapCatalog::Default()->GetOperatorOids("%"));
  EXPECT_THAT(percent_oids, UnorderedElementsAre(439, 529, 530, 1762));
}

TEST(BootstrapCatalog, GetOperatorOidsEquals) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Span<const Oid> equals_oids,
                       PgBootstrapCatalog::Default()->GetOperatorOids("="));
  // There are lots of equals oids. Just check count and a few examples.
  EXPECT_THAT(equals_oids, SizeIs(63));
  EXPECT_THAT(equals_oids, IsSupersetOf({91, 92, 410, 670, 1804, 3240}));
}

TEST(BootstrapCatalog, GetOperatorOidsBoxOverlap) {
  // Get the "&&" operator for box_overlap.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_operator* box_overlap_new,
                       PgBootstrapCatalog::Default()->GetOperator(500));
  // Get the "?#" operator for box_overlap.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_operator* box_overlap_old,
                       PgBootstrapCatalog::Default()->GetOperator(802));

  // The oprcode for both operators should be the function oid for box_overlap.
  EXPECT_EQ(box_overlap_new->oprcode, 125);
  EXPECT_EQ(box_overlap_old->oprcode, 125);

  // Check that the reverse lookup from oprcode to operator oid finds both oids.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Span<const Oid> box_overlap_oids_new,
      PgBootstrapCatalog::Default()->GetOperatorOidsByOprcode(125));
  EXPECT_THAT(box_overlap_oids_new, UnorderedElementsAre(500, 802));
}

// Test verifies that the expected operator is returned when INT8OIDs are both
// the left and right values.
TEST(BootstrapCatalog, GetOperatorOidByOprLeftRightEqualsInt) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid matching_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight("=", INT8OID,
                                                                  INT8OID));
  EXPECT_EQ(matching_oid, 410);
}

// Test verifies that the exper expected operator is returned when BOXOIF and
// POINTOID are the left and right values.
TEST(BootstrapCatalog, GetOperatorOidByOprLeftRightBoxAdd) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid matching_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight("+", BOXOID,
                                                                  POINTOID));
  EXPECT_EQ(matching_oid, 804);
}

TEST(BootstrapCatalog, GetAggregateSum) {
  // Comes from pg_aggregate.h, which does not directly define this value.
  // Rather, it is bootstrap data for the postgres database that we use here for
  // a test value. In postgres, this value never exists as a C value, only a
  // database entry.
  constexpr Oid int4_sum_oid = 2108;

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_aggregate* agg_data,
      PgBootstrapCatalog::Default()->GetAggregate(int4_sum_oid));

  // Verify a sampling of fields.
  ASSERT_NE(agg_data, nullptr);
  EXPECT_EQ(agg_data->aggkind, AGGKIND_NORMAL);
  EXPECT_EQ(agg_data->aggnumdirectargs, 0);
  EXPECT_EQ(agg_data->aggtransfn, F_INT4_SUM);
  EXPECT_EQ(agg_data->aggfinalfn, InvalidOid);
  EXPECT_EQ(agg_data->aggcombinefn, F_INT8PL);
  EXPECT_EQ(agg_data->aggserialfn, InvalidOid);
  EXPECT_EQ(agg_data->aggdeserialfn, InvalidOid);
}

TEST(BootstrapCatalog, GetAggregateFailure) {
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetAggregate(InvalidOid),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST(BootstrapCatalog, Int8TypByVal) {
  // The value of 'typbyval' for INT8 is set by a constant rather than a
  // literal.  Verify that gen_catalog_info.pl passes it through correctly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_type* form_data,
                       PgBootstrapCatalog::Default()->GetType(INT8OID));
  EXPECT_TRUE(form_data->typbyval);
}

TEST(BootstrapCatalog, PrunedInt4Functions) {
  // The int8_t to int4 casting function should be removed.
  EXPECT_THAT(PgBootstrapCatalog::Default()->GetProc(480),
              StatusIs(absl::StatusCode::kNotFound));

  EXPECT_THAT(PgBootstrapCatalog::Default()->GetProcProto(480),
              StatusIs(absl::StatusCode::kNotFound));

  EXPECT_THAT(PgBootstrapCatalog::Default()->GetCast(INT8OID, INT4OID),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST(BootstrapCatalog, OpClassByAccessMethod) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Span<const Oid> btree_opclasses,
      PgBootstrapCatalog::Default()->GetOpclassesByAm(BTREE_AM_OID));

  ASSERT_GT(btree_opclasses.length(), 0);
  for (const Oid& opclass_id : btree_opclasses) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_opclass* opclass,
                         PgBootstrapCatalog::Default()->GetOpclass(opclass_id));
    EXPECT_EQ(opclass->opcmethod, BTREE_AM_OID);
  }
}

TEST(BootstrapCatalog, AmopByFamily) {
  // Get a sample opfamily.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int8_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT8_BTREE_OPS_OID));
  ASSERT_NE(int8_opclass, nullptr);
  Oid int8_opfamily = int8_opclass->opcfamily;

  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_amop* amop,
                       PgBootstrapCatalog::Default()->GetAmopByFamily(
                           int8_opfamily,
                           /*lefttype =*/INT8OID,
                           /*righttype =*/INT8OID,
                           /*strategy =*/BTLessStrategyNumber));

  ASSERT_NE(amop, nullptr);
  EXPECT_EQ(amop->amopfamily, int8_opfamily);
  EXPECT_EQ(amop->amoplefttype, INT8OID);
  EXPECT_EQ(amop->amoprighttype, INT8OID);
  EXPECT_EQ(amop->amopstrategy, BTLessStrategyNumber);
  EXPECT_EQ(amop->amoppurpose, AMOP_SEARCH);  // Default value.
  EXPECT_EQ(amop->amopopr, Int8LessOperator);
  EXPECT_EQ(amop->amopmethod, BTREE_AM_OID);
  EXPECT_EQ(amop->amopsortfamily, 0);  // Not applicable for AMOP_SEARCH.
}

TEST(BootstrapCatalog, AmopsByOprOid) {
  // Get a sample operator oid.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid int8_eq_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight(
          "=", INT8OID, INT8OID));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Span<const FormData_pg_amop* const> amops,
      PgBootstrapCatalog::Default()->GetAmopsByAmopOpId(int8_eq_oid));

  ASSERT_EQ(amops.size(), 5);
  EXPECT_EQ(amops[0]->amopopr, int8_eq_oid);
}

TEST(BootstrapCatalog, AmprocByFamily) {
  // Get a sample opfamily.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int8_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT8_BTREE_OPS_OID));
  ASSERT_NE(int8_opclass, nullptr);
  Oid int8_opfamily = int8_opclass->opcfamily;

  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_amproc* amproc,
                       PgBootstrapCatalog::Default()->GetAmprocByFamily(
                           int8_opfamily,
                           /*lefttype =*/INT8OID,
                           /*righttype =*/INT8OID,
                           /*index =*/BTORDER_PROC));

  ASSERT_NE(amproc, nullptr);
  EXPECT_EQ(amproc->amprocfamily, int8_opfamily);
  EXPECT_EQ(amproc->amproclefttype, INT8OID);
  EXPECT_EQ(amproc->amprocrighttype, INT8OID);
  EXPECT_EQ(amproc->amprocnum, BTORDER_PROC);
  // Find the expected function Oid.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> proc_oids, GetProcOids("btint8cmp"));
  ASSERT_EQ(proc_oids.size(), 1);
  EXPECT_EQ(amproc->amproc, proc_oids[0]);
}

TEST(BootstrapCatalog, SpannerPendingCommitTimestampProc) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace_oid,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Span<const FormData_pg_proc* const> procs,
                       PgBootstrapCatalog::Default()->GetProcsByName(
                           "pending_commit_timestamp"));

  ASSERT_EQ(procs.size(), 1);
  EXPECT_EQ(procs[0]->pronamespace, namespace_oid);
}

TEST(BootstrapCatalog, SpannerBitReverseProc) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace_oid,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Span<const FormData_pg_proc* const> procs,
      PgBootstrapCatalog::Default()->GetProcsByName("bit_reverse"));

  ASSERT_EQ(procs.size(), 1);
  EXPECT_EQ(procs[0]->pronamespace, namespace_oid);
  EXPECT_STREQ(NameStr(procs[0]->proname), "bit_reverse");
}

TEST(BootstrapCatalog, SpannerFarmFingerprintProc) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace_oid,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Span<const FormData_pg_proc* const> procs,
      PgBootstrapCatalog::Default()->GetProcsByName("farm_fingerprint"));

  ASSERT_EQ(procs.size(), 2);
  for (auto* proc : procs) {
    EXPECT_EQ(proc->pronamespace, namespace_oid);
    EXPECT_STREQ(NameStr(proc->proname), "farm_fingerprint");
  }
}

TEST(BootstrapCatalog, SpannerGetInternalSequenceStateProc) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace_oid,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Span<const FormData_pg_proc* const> procs,
      PgBootstrapCatalog::Default()->GetProcsByName(
          "get_internal_sequence_state"));

  ASSERT_EQ(procs.size(), 1);
  EXPECT_EQ(procs[0]->pronamespace, namespace_oid);
  EXPECT_STREQ(NameStr(procs[0]->proname), "get_internal_sequence_state");
}

TEST(BootstrapCatalog, SpannerGetTableColumnIdentityStateProc) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace_oid,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Span<const FormData_pg_proc* const> procs,
                       PgBootstrapCatalog::Default()->GetProcsByName(
                           "get_table_column_identity_state"));

  ASSERT_EQ(procs.size(), 1);
  EXPECT_EQ(procs[0]->pronamespace, namespace_oid);
  EXPECT_STREQ(NameStr(procs[0]->proname), "get_table_column_identity_state");
}

TEST(BootstrapCatalog, SpannerGenerateUuidProc) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace_oid,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Span<const FormData_pg_proc* const> procs,
      PgBootstrapCatalog::Default()->GetProcsByName("generate_uuid"));

  ASSERT_EQ(procs.size(), 1);
  EXPECT_EQ(procs[0]->pronamespace, namespace_oid);
  EXPECT_STREQ(NameStr(procs[0]->proname), "generate_uuid");
}

TEST(BootstrapCatalog, TimestampFromUnixMicrosProc) {
  for (Oid input_arg : {INT8OID, TIMESTAMPTZOID}) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Oid function_oid,
        PgBootstrapCatalog::Default()->GetProcOid(
            "spanner", "timestamp_from_unix_micros", {input_arg}));
    ASSERT_NE(function_oid, InvalidOid);
  }
}

TEST(BootstrapCatalog, TimestampFromUnixMillisProc) {
  for (Oid input_arg : {INT8OID, TIMESTAMPTZOID}) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Oid function_oid,
        PgBootstrapCatalog::Default()->GetProcOid(
            "spanner", "timestamp_from_unix_millis", {input_arg}));
    ASSERT_NE(function_oid, InvalidOid);
  }
}

}  // namespace
}  // namespace postgres_translator
