/*-------------------------------------------------------------------------
 *
 * pgtime.h
 *	  PostgreSQL internal timezone library
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  datetime_parsing/pgtime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATETIME_PARSING_PGTIME_H
#define DATETIME_PARSING_PGTIME_H

#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datetime_parsing/c.h"

// SPANNER_PG: copied from src/include/pgtime.h

/*
 * The API of this library is generally similar to the corresponding
 * C library functions, except that we use pg_time_t which (we hope) is
 * 64 bits wide, and which is most definitely signed not unsigned.
 */

typedef int64_t pg_time_t;

struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* origin 1, not 0! */
	int			tm_year;		/* relative to 1900 */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	const char *tm_zone;
};

typedef struct pg_tz pg_tz;

/* Maximum length of a timezone name (not including trailing null) */
#define TZ_STRLEN_MAX 255

extern int	pg_next_dst_boundary(const pg_time_t *timep,
								 long int *before_gmtoff,
								 int *before_isdst,
								 pg_time_t *boundary,
								 long int *after_gmtoff,
								 int *after_isdst,
								 const pg_tz *tz);
pg_tz *pg_tzset(absl::string_view name);

#endif							/* DATETIME_PARSING_PGTIME_H */
