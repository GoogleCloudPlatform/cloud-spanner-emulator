/*-------------------------------------------------------------------------
 *
 * c.h
 *	  Fundamental C definitions.  This is included by every .c file in
 *	  PostgreSQL (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *	  Note that the definitions here are not intended to be exposed to clients
 *	  of the frontend interface libraries --- so we don't worry much about
 *	  polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * datetime_parsing/c.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DATETIME_PARSING_C_H_
#define DATETIME_PARSING_C_H_

#include <ctype.h>
#include <stddef.h>
#include <sys/types.h>

// SPANNER_PG: copied from src/include/c.h

typedef int int32_t;
typedef long int int64_t;
#define INT64CONST(x)  (x##L)


/*
 * We require C99, hence the compiler should understand flexible array
 * members.  However, for documentation purposes we still consider it to be
 * project style to write "field[FLEXIBLE_ARRAY_MEMBER]" not just "field[]".
 * When computing the size of such an object, use "offsetof(struct s, f)"
 * for portability.  Don't use "offsetof(struct s, f[0])", as this doesn't
 * work with MSVC and with C++ compilers.
 */
#define FLEXIBLE_ARRAY_MEMBER	/* empty */

#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)

/*
 * Size
 *		Size of any memory resident object, as returned by sizeof.
 */
typedef size_t Size;

/* msb for char */
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/* ----------------------------------------------------------------
 *				Section 9: system-specific hacks
 *
 *		This should be limited to things that absolutely have to be
 *		included in every source file.  The port-specific header file
 *		is usually a better place for this sort of thing.
 * ----------------------------------------------------------------
 */
#define PG_BINARY	0
#define PG_BINARY_A "a"
#define PG_BINARY_R "r"
#define PG_BINARY_W "w"

#endif  // DATETIME_PARSING_C_H_
