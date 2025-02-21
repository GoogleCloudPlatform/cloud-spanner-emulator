/*-------------------------------------------------------------------------
 *
 * parser.h
 *		Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

#include "nodes/parsenodes.h"


/*
 * RawParseMode determines the form of the string that raw_parser() accepts:
 *
 * RAW_PARSE_DEFAULT: parse a semicolon-separated list of SQL commands,
 * and return a List of RawStmt nodes.
 *
 * RAW_PARSE_TYPE_NAME: parse a type name, and return a one-element List
 * containing a TypeName node.
 *
 * RAW_PARSE_PLPGSQL_EXPR: parse a PL/pgSQL expression, and return
 * a one-element List containing a RawStmt node.
 *
 * RAW_PARSE_PLPGSQL_ASSIGNn: parse a PL/pgSQL assignment statement,
 * and return a one-element List containing a RawStmt node.  "n"
 * gives the number of dotted names comprising the target ColumnRef.
 */
typedef enum
{
	RAW_PARSE_DEFAULT = 0,
	RAW_PARSE_TYPE_NAME,
	RAW_PARSE_PLPGSQL_EXPR,
	RAW_PARSE_PLPGSQL_ASSIGN1,
	RAW_PARSE_PLPGSQL_ASSIGN2,
	RAW_PARSE_PLPGSQL_ASSIGN3
} RawParseMode;

/* Values for the backslash_quote GUC */
typedef enum
{
	BACKSLASH_QUOTE_OFF,
	BACKSLASH_QUOTE_ON,
	BACKSLASH_QUOTE_SAFE_ENCODING
}			BackslashQuoteType;

/* GUC variables in scan.l (every one of these is a bad idea :-() */
extern PGDLLIMPORT int backslash_quote;
extern PGDLLIMPORT bool escape_string_warning;
extern PGDLLIMPORT bool standard_conforming_strings;


/* Primary entry point for the raw parsing functions */
extern List *raw_parser(const char *str, RawParseMode mode);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List *SystemFuncName(char *name);
extern TypeName *SystemTypeName(char *name);


// SPANGRES BEGIN
// Defined in src/spangres/parser.h because we need C++ (not C)
struct SpangresTokenLocations;

// Like raw_parser from PostgreSQL, but also fills in token locations.
struct List* raw_parser_spangres(const char* str,
                                 RawParseMode mode,
                                 struct SpangresTokenLocations* locations);

// If locations is non-null, add location as both the (exclusive) end of the
// previous token (if any) and the start of the next one.  Defined in
// parser_shim.cc.
void add_token_end_start_spangres(int location,
                                  struct SpangresTokenLocations* locations);

// SPANGRES END

#endif							/* PARSER_H */
