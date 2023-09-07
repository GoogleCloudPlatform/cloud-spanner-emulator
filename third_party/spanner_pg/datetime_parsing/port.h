/*-------------------------------------------------------------------------
 *
 * port.h
 *	  Header for src/port/ compatibility functions.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * datetime_parsing/port.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATETIME_PARSING_PORT_H
#define DATETIME_PARSING_PORT_H

#include <ctype.h>
#include <stddef.h>

#include "third_party/spanner_pg/datetime_parsing/c.h"

// SPANNER_PG: copied from src/include/port.h

/* Portable SQL-like case-independent comparisons and conversions */
int	pg_strcasecmp(const char *s1, const char *s2);
int	pg_strncasecmp(const char *s1, const char *s2, size_t n);
unsigned char pg_toupper(unsigned char ch);
unsigned char pg_tolower(unsigned char ch);
unsigned char pg_ascii_toupper(unsigned char ch);
unsigned char pg_ascii_tolower(unsigned char ch);

size_t strlcpy(char *dst, const char *src, size_t siz);

#endif							/* DATETIME_PARSING_PORT_H */
