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

// Portions derived from source copyright Postgres Global Development Group in
// accordance with the following notice:
//------------------------------------------------------------------------------
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
// Portions Copyright 2022 Google LLC
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

#include "third_party/spanner_pg/shims/catalog_shim.h"

#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/catalog_shim_cc_wrappers.h"

// Get the type OID to pass to I/O functions.
// Has a different signature than the PG getTypeIOParam, which takes in a
// HeapTuple.
// This is a special case where the original function is still supported and
// used inside of the PG source code, but an alternate version is used by the
// shimmed PG functions.
static Oid getTypeIOParamSpangres(Oid typid, const FormData_pg_type* typeStruct)
{
	/*
	 * Array types get their typelem as parameter; everybody else gets their
	 * own type OID as parameter.
	 */
	if (OidIsValid(typeStruct->typelem))
		return typeStruct->typelem;
	else
		return typid;
}

Oid getBaseTypeAndTypmod(Oid typid, int32_t *typmod) {
	return typid;
}

// This version is identical to the PostgreSQL version except that it uses
// bootstrap_catalog instead of the system cache.
Oid get_typcollation(Oid typid) {
	const FormData_pg_type* typtup = GetTypeFromBootstrapCatalog(typid);
	if (typtup == NULL) {
		return InvalidOid;
	} else {
		return typtup->typcollation;
	}
}

// Look up the table name from the catalog adapter and then get column
// information from the thread-local googlesql catalog.
// REQUIRES: This table has already been translated and is known to the catalog
// adapter.
// RETURNS a palloc'd copy of the name string in the thread-local
// CurrentMemoryContext or NULL if not found.
char* get_attname(Oid relid, AttrNumber attnum, bool missing_ok) {
	return GetAttributeNameC(relid, attnum, missing_ok);
}

/*
 * Given the type OID, fetch its category and preferred-type status.
 *  Throws error on failure.
 * Spangres version uses bootstrap_catalog instead of syscache.
 */
void get_type_category_preferred(Oid typid, char* typcategory,
										  bool* typispreferred) {
	const FormData_pg_type* type_data;

	type_data = GetTypeFromBootstrapCatalog(typid);
	if (type_data == NULL) {
		elog(ERROR, "bootstrap catalog lookup failed for type %u", typid);
	}
	*typcategory = type_data->typcategory;
	*typispreferred = type_data->typispreferred;
}

/*
 * Given procedure id, return the function's provariadic field.
 */
Oid get_func_variadictype(Oid funcid) {
	const FormData_pg_proc* proc_data = GetProcByOid(funcid);
	if (proc_data == NULL) {
		elog(ERROR, "cache lookup failed for function %u", funcid);
  } else {
    return proc_data->provariadic;
  }
}

/* Given a procedure id, gets pg_proc from bootstrap catalog and returns the
 * field. Errors if procedure is not found.
 */
bool get_func_retset(Oid funcid) {
	const FormData_pg_proc* proc_data = GetProcByOid(funcid);
	if (proc_data == NULL) {
		elog(ERROR, "catalog lookup failed for function %u", funcid);
	} else {
		return proc_data->proretset;
	}
}

// Given a relation OID, return its name in a palloc'd string. Spangres version
// uses the catalog adapter instead of syscache.
char* get_rel_name(Oid relid)
{
	return GetTableNameC(relid);
}

// Given the type OID, find if it is a basic type, a complex type, etc. This
// returns the null char if the cache lookup fails.
char get_typtype(Oid typid) {
	const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(typid);
	if (type_data == NULL) {
		return '\0';
	} else {
		return type_data->typtype;
	}
}

// Given the type OID, fetch its typOutput and typIsVarlena status. Throws
// error on failure. Spangres version uses bootstrap_catalog instead of
// syscache.
void getTypeOutputInfo(Oid typid, Oid* typOutput, bool* typIsVarlena) {
	const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(typid);

	if (type_data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type id %s is unsupported", typid)));
	if (!type_data->typisdefined)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type %s is only a shell", format_type_be(typid))));
	if (!OidIsValid(type_data->typoutput))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("no output function available for type %s",
						format_type_be(typid))));

	*typOutput = type_data->typoutput;
	*typIsVarlena = (!type_data->typbyval) && (type_data->typlen == -1);
}

Oid get_element_type(Oid typid) {
	const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(typid);

	if (type_data != NULL && type_data->typlen == -1)
	{
		return type_data->typelem;
	}

	return InvalidOid;
}

void getTypeInputInfo(Oid type, Oid* typInput, Oid* typIOParam) {
	const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(type);
	if (type_data == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type id %u is unsupported", type)));
	}

	if (!type_data->typisdefined) {
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
										errmsg("type %s is only a shell", format_type_be(type))));
	}
	if (!OidIsValid(type_data->typinput)) {
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
										errmsg("no input function available for type %s",
													 format_type_be(type))));
	}

	*typInput = type_data->typinput;
	if (OidIsValid(type_data->typelem)) {
		*typIOParam = type_data->typelem;
	} else {
		*typIOParam = type;
	}
}

List* get_op_btree_interpretation(Oid opno) {
	List	   *result = NIL;
	OpBtreeInterpretation *thisresult;

	/*
	 * Find all the pg_amop entries containing the operator.
	 */
	const FormData_pg_amop* const* amop_list;
	size_t amop_count;
	GetAmopsByAmopOpIdFromBootstrapCatalog(opno, &amop_list, &amop_count);

	for (int amop_index = 0; amop_index < amop_count; ++amop_index) {
		const FormData_pg_amop* op_form = amop_list[amop_index];
		StrategyNumber op_strategy;

		/* must be btree */
		if (op_form->amopmethod != BTREE_AM_OID)
			continue;

		/* Get the operator's btree strategy number */
		op_strategy = (StrategyNumber) op_form->amopstrategy;
		Assert(op_strategy >= 1 && op_strategy <= 5);

		thisresult = (OpBtreeInterpretation *)
			palloc(sizeof(OpBtreeInterpretation));
		thisresult->opfamily_id = op_form->amopfamily;
		thisresult->strategy = op_strategy;
		thisresult->oplefttype = op_form->amoplefttype;
		thisresult->oprighttype = op_form->amoprighttype;
		result = lappend(result, thisresult);
	}

	/*
	 * If we didn't find any btree opfamily containing the operator, perhaps
	 * it is a <> operator.  See if it has a negator that is in an opfamily.
	 */
	if (result == NIL)
	{
		Oid			op_negator = get_negator(opno);

		if (OidIsValid(op_negator))
		{
			GetAmopsByAmopOpIdFromBootstrapCatalog(op_negator, &amop_list,
																						 &amop_count);

			for (int amop_index = 0; amop_index < amop_count; ++amop_index) {
				const FormData_pg_amop* op_form = amop_list[amop_index];
				StrategyNumber op_strategy;

				/* must be btree */
				if (op_form->amopmethod != BTREE_AM_OID)
					continue;

				/* Get the operator's btree strategy number */
				op_strategy = (StrategyNumber) op_form->amopstrategy;
				Assert(op_strategy >= 1 && op_strategy <= 5);

				/* Only consider negators that are = */
				if (op_strategy != BTEqualStrategyNumber)
					continue;

				/* OK, report it with "strategy" ROWCOMPARE_NE */
				thisresult = (OpBtreeInterpretation *)
					palloc(sizeof(OpBtreeInterpretation));
				thisresult->opfamily_id = op_form->amopfamily;
				thisresult->strategy = ROWCOMPARE_NE;
				thisresult->oplefttype = op_form->amoplefttype;
				thisresult->oprighttype = op_form->amoprighttype;
				result = lappend(result, thisresult);
			}
		}
	}

	return result;
}

Oid get_opclass_family(Oid opclass) {
  const FormData_pg_opclass* cla_tup;
  Oid result;

  cla_tup = GetOpclassFromBootstrapCatalog(opclass);
  if (cla_tup == NULL)
    elog(ERROR, "bootstrap catalog lookup failed for opclass %u", opclass);

  result = cla_tup->opcfamily;
  return result;
}

Oid get_opclass_input_type(Oid opclass) {
	const FormData_pg_opclass* cla_tup;
	Oid result;

	cla_tup = GetOpclassFromBootstrapCatalog(opclass);
	if (cla_tup == NULL)
		elog(ERROR, "bootstrap catalog lookup failed for opclass %u", opclass);

	result = cla_tup->opcintype;
	return result;
}

/*
 * get_opfamily_member
 *		Get the OID of the operator that implements the specified
 *strategy with the specified datatypes for the specified opfamily.
 *
 * Returns InvalidOid if there is no pg_amop entry for the given keys.
 * SPANGRES: Use an index on bootstrap's pg_amop table to look this up.
 */
Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype,
						int16_t strategy) {
	const FormData_pg_amop* amop_tup;
	Oid result;

	amop_tup = GetAmopByFamilyFromBootstrapCatalog(opfamily, lefttype, righttype,
												   strategy);
	if (amop_tup == NULL)
		return InvalidOid;

	result = amop_tup->amopopr;
	return result;
}

/*
 * get_ordering_op_properties
 *		Given the OID of an ordering operator (a btree "<" or ">" operator),
 *		determine its opfamily, its declared input datatype, and its
 *		strategy number (BTLessStrategyNumber or BTGreaterStrategyNumber).
 *
 * Returns true if successful, false if no matching pg_amop entry exists.
 * (This indicates that the operator is not a valid ordering operator.)
 *
 * Note: the operator could be registered in multiple families, for example
 * if someone were to build a "reverse sort" opfamily.  This would result in
 * uncertainty as to whether "ORDER BY USING op" would default to NULLS FIRST
 * or NULLS LAST, as well as inefficient planning due to failure to match up
 * pathkeys that should be the same.  So we want a determinate result here.
 * Because of the way the syscache search works, we'll use the interpretation
 * associated with the opfamily with smallest OID, which is probably
 * determinate enough.  Since there is no longer any particularly good reason
 * to build reverse-sort opfamilies, it doesn't seem worth expending any
 * additional effort on ensuring consistency.
 */
bool get_ordering_op_properties(Oid opno, Oid *opfamily, Oid *opcintype,
																int16_t *strategy) {
	bool		result = false;

	/* ensure outputs are initialized on failure */
	*opfamily = InvalidOid;
	*opcintype = InvalidOid;
	*strategy = 0;

	/*
	 * Search pg_amop to see if the target operator is registered as the "<"
	 * or ">" operator of any btree opfamily.
	 */
	const FormData_pg_amop* const* amop_list;
	size_t amop_count;
	GetAmopsByAmopOpIdFromBootstrapCatalog(opno, &amop_list, &amop_count);

	for (int amop_index = 0; amop_index < amop_count; ++amop_index)
	{
		const FormData_pg_amop* aform = amop_list[amop_index];

		/* must be btree */
		if (aform->amopmethod != BTREE_AM_OID)
			continue;

		if (aform->amopstrategy == BTLessStrategyNumber ||
			aform->amopstrategy == BTGreaterStrategyNumber)
		{
			/* Found it ... should have consistent input types */
			if (aform->amoplefttype == aform->amoprighttype)
			{
				/* Found a suitable opfamily, return info */
				*opfamily = aform->amopfamily;
				*opcintype = aform->amoplefttype;
				*strategy = aform->amopstrategy;
				result = true;
				break;
			}
		}
	}

	return result;
}

/*
 * get_opfamily_proc
 *		Get the OID of the specified support function
 *		for the specified opfamily and datatypes.
 *
 * Returns InvalidOid if there is no pg_amproc entry for the given keys.
 * SPANGRES: Use an index on bootstrap's pg_amproc table to look this up.
 */
Oid get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16_t procnum)
{
	const FormData_pg_amproc* amproc_tup;
	RegProcedure result;

	amproc_tup = GetAmprocByFamilyFromBootstrapCatalog(opfamily, lefttype,
													   righttype, procnum);
	if (amproc_tup == NULL)
		return InvalidOid;
	result = amproc_tup->amproc;
	return result;
}

/*
 * get_opcode
 *
 * Returns the regproc id of the routine used to implement an
 * operator given the operator oid.
 * SPANGRES: Use bootstrap catalog for pg_operator lookup.
 */
RegProcedure get_opcode(Oid opno) {
	const FormData_pg_operator* optup = GetOperatorFromBootstrapCatalog(opno);
	if (optup != NULL)
		return optup->oprcode;
	else
		return InvalidOid;
}

/*
 * get_commutator
 *
 * Returns the corresponding commutator of an operator.
 * SPANGRES: Use bootstrap catalog for pg_operator lookup.
 */
Oid get_commutator(Oid opno) {
	const FormData_pg_operator* optup = GetOperatorFromBootstrapCatalog(opno);
	if (optup != NULL) {
		return optup->oprcom;
	} else {
		return InvalidOid;
	}
}

/*
 * get_negator
 *
 * Returns the corresponding negator of an operator.
 */
Oid get_negator(Oid opno) {
	const FormData_pg_operator* optup = GetOperatorFromBootstrapCatalog(opno);
	if (optup != NULL) {
		return optup->oprnegate;
	} else {
		return InvalidOid;
	}
}

/*
 * get_range_subtype
 *		Returns the subtype of a given range type
 *
 * Returns InvalidOid if the type is not a range type.
 * SPANGRES: No range support yet; always return InvalidOid.
 */
Oid get_range_subtype(Oid rangeOid) {
	// TODO: Support Range types.
	return InvalidOid;
}

/*
 * get_base_element_type
 *		Given the type OID, get the typelem, looking "through" any
 *domain to its underlying array type.
 *
 * This is equivalent to get_element_type(getBaseType(typid)), but
 * avoids an extra cache lookup.  Note that it fails to provide any information
 * about the typmod of the array.
 *
 * SPANGRES: We lean on that equivalency and just make the equivalent call since
 * our bootstrap_catalog lookups are cheap and currently we don't even do the
 * lookup for domain base types.
 */
Oid get_base_element_type(Oid typid) {
	return get_element_type(getBaseType(typid));
}

void
get_type_io_data(Oid typid,
				 IOFuncSelector which_func,
				 int16_t *typlen,
				 bool *typbyval,
				 char *typalign,
				 char *typdelim,
				 Oid *typioparam,
				 Oid *func)
{
	// Spangres: skip bootstrap processing mode. Use the bootstrap catalog to
	// get the type data instead of the syscache.
	const FormData_pg_type* typeStruct = GetTypeFromBootstrapCatalog(typid);
	if (typeStruct == NULL) {
		elog(ERROR, "bootstrap catalog lookup failed for type %u", typid);
	}

	*typlen = typeStruct->typlen;
	*typbyval = typeStruct->typbyval;
	*typalign = typeStruct->typalign;
	*typdelim = typeStruct->typdelim;
	// Spangres: use Spangres version of getTypeIOParam.
	*typioparam = getTypeIOParamSpangres(typid, typeStruct);
	switch (which_func)
	{
		case IOFunc_input:
			*func = typeStruct->typinput;
			break;
		case IOFunc_output:
			*func = typeStruct->typoutput;
			break;
		case IOFunc_receive:
			*func = typeStruct->typreceive;
			break;
		case IOFunc_send:
			*func = typeStruct->typsend;
			break;
	}
}

// get_array_type
//
//    Given the type OID, get the corresponding "true" array type.
//    Returns InvalidOid if no array type can be found.
//
Oid
get_array_type(Oid typid)
{
	const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(typid);

	Oid      result = InvalidOid;

	if (type_data != NULL)
	{
		result = type_data->typarray;
	}
	return result;
}

Oid
get_relname_relid(const char* relname, Oid namespace_oid) {
        return GetOrGenerateOidFromNamespaceOidAndRelationNameC(namespace_oid,
                                                               relname);
}

/*
 * get_typlenbyvalalign
 *
 *		A three-fer: given the type OID, return typlen, typbyval, typalign.
 */
void
get_typlenbyvalalign(Oid typid, int16_t *typlen, bool *typbyval,
					 char *typalign)
{
	const FormData_pg_type* typtup = GetTypeFromBootstrapCatalog(typid);

	if (typtup == NULL)
		elog(ERROR, "cache lookup failed for type %u", typid);
	*typlen = typtup->typlen;
	*typbyval = typtup->typbyval;
	*typalign = typtup->typalign;
}

/*
 * get_typsubscript
 *
 *		Given the type OID, return the type's subscripting handler's OID,
 *		if it has one.
 *
 * If typelemp isn't NULL, we also store the type's typelem value there.
 * This saves some callers an extra catalog lookup.
 */
RegProcedure
get_typsubscript(Oid typid, Oid *typelemp)
{
	const FormData_pg_type* typform = GetTypeFromBootstrapCatalog(typid);
	if (typform == NULL) {
		elog(ERROR, "catalog lookup failed for type %u", typid);
	}
	
	if (typform->typelem == InvalidOid) {
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot subscript type %s because it is not an array",
						format_type_be(typid))));
	}

	RegProcedure handler = typform->typsubscript;
	if (typelemp) {
		*typelemp = typform->typelem;
	}

	return handler;
}

/*
 * get_multirange_range
 *		Returns the range type of a given multirange
 *
 * Returns InvalidOid if the type is not a multirange.
 */
Oid
get_multirange_range(Oid multirangeOid)
{
	const FormData_pg_type* typform = GetTypeFromBootstrapCatalog(multirangeOid);
	if (typform == NULL) {
		return InvalidOid;
	}
	
	return typform->oid;
}
