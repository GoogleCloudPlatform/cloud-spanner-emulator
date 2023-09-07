/*-------------------------------------------------------------------------
 *
 * timestamp.h
 *	  Definitions for the SQL "timestamp" and "interval" types.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * datetime_parsing/timestamp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATETIME_PARSING_TIMESTAMP_H
#define DATETIME_PARSING_TIMESTAMP_H

#include <ctype.h>
#include <stdbool.h>

#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp_macros.h"

// SPANNER_PG: copied from src/include/utils/timestamp.h

#define INTERVAL_MASK(b) (1 << (b))

/* Macros to handle packing and unpacking the typmod field for intervals */
#define INTERVAL_FULL_RANGE (0x7FFF)

/* Internal routines (not fmgr-callable) */
/* SPANNER_PG: rename first parameter to match name in function implementation */
void dt2time(Timestamp jd, int *hour, int *min, int *sec, fsec_t *fsec);
/* SPANNER_PG: rename last parameter to match name in function implementation */
int	tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result);

int tm2interval(struct pg_tm *tm, fsec_t fsec, Interval *span);

#endif  // DATETIME_PARSING_TIMESTAMP_H
