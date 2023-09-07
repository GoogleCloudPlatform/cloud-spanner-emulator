/*-------------------------------------------------------------------------
 *
 * date.h
 *	  Definitions for the SQL "date" and "time" types.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * datetime_parsing/date.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATETIME_PARSING_DATE_H
#define DATETIME_PARSING_DATE_H

// SPANNER_PG: copied from src/include/utils/date.h

#include <math.h>

#include "absl/strings/string_view.h"
// c.h must be included before other PG files
#include "third_party/spanner_pg/datetime_parsing/c.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp_macros.h"

typedef int32_t DateADT;

typedef int64_t TimeADT;

/*
 * Infinity and minus infinity must be the max and min values of DateADT.
 */
#define DATEVAL_NOBEGIN		((DateADT) PG_INT32_MIN)
#define DATEVAL_NOEND		((DateADT) PG_INT32_MAX)

#define DATE_NOBEGIN(j)		((j) = DATEVAL_NOBEGIN)
#define DATE_IS_NOBEGIN(j)	((j) == DATEVAL_NOBEGIN)
#define DATE_NOEND(j)		((j) = DATEVAL_NOEND)
#define DATE_IS_NOEND(j)	((j) == DATEVAL_NOEND)
#define DATE_NOT_FINITE(j)	(DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j))

/* date.c */
int	time2tm(TimeADT time, struct pg_tm *tm, fsec_t *fsec);
bool time_overflows(int hour, int min, int sec, fsec_t fsec);

#endif							/* DATETIME_PARSING_DATE_H */
