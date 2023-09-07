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

// This function was made non-static in format_type.c, forward declare here to
// limit the scope
char *printTypmod(const char *typname, int32_t typmod, Oid typmodout);

// Mostly identical to the PostgreSQL version, except that we use
// bootstrap_catalog instead of the Heap.
char* format_type_extended(Oid type_oid, int32_t typemod, bits16 flags) {
	const FormData_pg_type* typeform;
	Oid			array_base_type;
	bool		is_array;
	char		 *buf;
	bool		with_typemod;

	if (type_oid == InvalidOid && (flags & FORMAT_TYPE_ALLOW_INVALID) != 0)
		return pstrdup("-");

	typeform = GetTypeFromBootstrapCatalog(type_oid);
	if (typeform == NULL) {
		ereport(ERROR,
						(errmsg("Typeoid not found in bootstrap catalog: %d", type_oid),
						 errcode(ERRCODE_INTERNAL_ERROR)));
	}

	/*
	 * Check if it's a regular (variable length) array type.	Fixed-length
	 * array types such as "name" shouldn't get deconstructed.	As of Postgres
	 * 8.1, rather than checking typlen we check the toast property, and don't
	 * deconstruct "plain storage" array types --- this is because we don't
	 * want to show oidvector as oid[].
	 */
	array_base_type = typeform->typelem;

	if (array_base_type != InvalidOid && typeform->typstorage != TYPSTORAGE_PLAIN)
	{
		/* Switch our attention to the array element type */
		typeform = GetTypeFromBootstrapCatalog(array_base_type);
		if (typeform == NULL) {
			ereport(ERROR,
							(errmsg("Array base typeoid not found in bootstrap catalog: %d",
											array_base_type),
							 errcode(ERRCODE_INTERNAL_ERROR)));
		}
		type_oid = array_base_type;
		is_array = true;
	}
	else
		is_array = false;

	with_typemod = (flags & FORMAT_TYPE_TYPEMOD_GIVEN) != 0 && (typemod >= 0);

	/*
	 * See if we want to special-case the output for certain built-in types.
	 * Note that these special cases should all correspond to special
	 * productions in gram.y, to ensure that the type name will be taken as a
	 * system type, not a user type of the same name.
	 *
	 * If we do not provide a special-case output here, the type name will be
	 * handled the same way as a user type name --- in particular, it will be
	 * double-quoted if it matches any lexer keyword.  This behavior is
	 * essential for some cases, such as types "bit" and "char".
	 */
	buf = NULL;					/* flag for no special case */

	switch (type_oid)
	{
		case BITOID:
			if (with_typemod)
				buf = printTypmod("bit", typemod, typeform->typmodout);
			else if ((flags & FORMAT_TYPE_TYPEMOD_GIVEN) != 0)
			{
				/*
				 * bit with typmod -1 is not the same as BIT, which means
				 * BIT(1) per SQL spec.  Report it as the quoted typename so
				 * that parser will not assign a bogus typmod.
				 */
			}
			else
				buf = pstrdup("bit");
			break;

		case BOOLOID:
			buf = pstrdup("boolean");
			break;

		case BPCHAROID:
			if (with_typemod)
				buf = printTypmod("character", typemod, typeform->typmodout);
			else if ((flags & FORMAT_TYPE_TYPEMOD_GIVEN) != 0)
			{
				/*
				 * bpchar with typmod -1 is not the same as CHARACTER, which
				 * means CHARACTER(1) per SQL spec.  Report it as bpchar so
				 * that parser will not assign a bogus typmod.
				 */
			}
			else
				buf = pstrdup("character");
			break;

		case FLOAT4OID:
			buf = pstrdup("real");
			break;

		case FLOAT8OID:
			buf = pstrdup("double precision");
			break;

		case INT2OID:
			buf = pstrdup("smallint");
			break;

		case INT4OID:
			buf = pstrdup("integer");
			break;

		case INT8OID:
			buf = pstrdup("bigint");
			break;

		case NUMERICOID:
			if (with_typemod)
				buf = printTypmod("numeric", typemod, typeform->typmodout);
			else
				buf = pstrdup("numeric");
			break;

		case INTERVALOID:
			if (with_typemod)
				buf = printTypmod("interval", typemod, typeform->typmodout);
			else
				buf = pstrdup("interval");
			break;

		case TIMEOID:
			if (with_typemod)
				buf = printTypmod("time", typemod, typeform->typmodout);
			else
				buf = pstrdup("time without time zone");
			break;

		case TIMETZOID:
			if (with_typemod)
				buf = printTypmod("time", typemod, typeform->typmodout);
			else
				buf = pstrdup("time with time zone");
			break;

		case TIMESTAMPOID:
			if (with_typemod)
				buf = printTypmod("timestamp", typemod, typeform->typmodout);
			else
				buf = pstrdup("timestamp without time zone");
			break;

		case TIMESTAMPTZOID:
			if (with_typemod)
				buf = printTypmod("timestamp", typemod, typeform->typmodout);
			else
				buf = pstrdup("timestamp with time zone");
			break;

		case VARBITOID:
			if (with_typemod)
				buf = printTypmod("bit varying", typemod, typeform->typmodout);
			else
				buf = pstrdup("bit varying");
			break;

		case VARCHAROID:
			if (with_typemod)
				buf = printTypmod("character varying", typemod,
																	typeform->typmodout);
			else
				buf = pstrdup("character varying");
			break;
	}

	if (buf == NULL) {
		/*
		 * Default handling: report the name as it appears in the catalog.
		 * Here, we must qualify the name if it is not visible in the search
		 * path or if caller requests it; and we must double-quote it if it's
		 * not a standard identifier or if it matches any keyword.
		 */
		char const *nspname;
		char const *typname;

		if ((flags & FORMAT_TYPE_FORCE_QUALIFY) == 0 &&
			TypeIsVisible(type_oid))
			nspname = NULL;
		else
			nspname = get_namespace_name_or_temp(typeform->typnamespace);

		typname = NameStr(typeform->typname);

		buf = quote_qualified_identifier(nspname, typname);

		if (with_typemod)
			buf = printTypmod(buf, typemod, typeform->typmodout);
	}

	if (is_array)
		buf = psprintf("%s[]", buf);

	return buf;
}
