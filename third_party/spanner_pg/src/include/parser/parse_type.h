/*-------------------------------------------------------------------------
 *
 * parse_type.h
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_type.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_TYPE_H
#define PARSE_TYPE_H

#include "access/htup.h"
#include "parser/parse_node.h"


typedef HeapTuple Type;

extern Type LookupTypeName(ParseState *pstate, const TypeName *typeName,
			   int32 *typmod_p, bool missing_ok);
extern Type LookupTypeNameExtended_UNUSED_SPANGRES(ParseState *pstate,
 								   const TypeName *typeName, int32 *typmod_p,
 								   bool temp_ok, bool missing_ok);

extern Oid LookupTypeNameOid(ParseState *pstate, const TypeName *typeName,
				  bool missing_ok);
extern Type typenameType(ParseState *pstate, const TypeName *typeName,
			 int32 *typmod_p);
extern Oid	typenameTypeId(ParseState *pstate, const TypeName *typeName);
extern void typenameTypeIdAndMod(ParseState *pstate, const TypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p);
extern char *TypeNameToString(const TypeName *typeName);
extern char *TypeNameListToString(List *typenames);

extern Oid	LookupCollation(ParseState *pstate, List *collnames, int location);
extern Oid	GetColumnDefCollation(ParseState *pstate, ColumnDef *coldef, Oid typeOid);

extern Type typeidType_UNUSED_SPANGRES(Oid id);

extern Oid	typeTypeId(Type tp);
extern int16 typeLen(Type t);
extern bool typeByVal(Type t);
extern char *typeTypeName(Type t);
extern Oid	typeTypeRelid(Type typ);
extern Oid	typeTypeCollation(Type typ);
extern Datum stringTypeDatum(Type tp, char *string, int32 atttypmod);

extern Oid	typeidTypeRelid_UNUSED_SPANGRES(Oid type_id);
extern Oid	typeOrDomainTypeRelid_UNUSED_SPANGRES(Oid type_id);

extern TypeName *typeStringToTypeName(const char *str);
extern void parseTypeString(const char *str, Oid *typeid_p, int32 *typmod_p, bool missing_ok);

/* SPANGRES BEGIN */
// NOTE: typeidTypeRelid has no declaration in this header. Since this
// is a macro, only callsites need include the declaration (in catalog_shim.h).
// Currently that's 3 places: parse_coerce.c, parse_expr.c, and parse_func.c.
// Those all include the declaration already. Future call sites would also need
// catalog_shim.h for this expansion to be compile.
#define ISCOMPLEX(typeid) (typeOrDomainTypeRelid(typeid) != InvalidOid)
/* SPANGRES END */

#endif							/* PARSE_TYPE_H */
