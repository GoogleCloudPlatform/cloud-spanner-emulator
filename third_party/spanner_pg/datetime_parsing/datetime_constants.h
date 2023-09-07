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

#ifndef DATETIME_PARSING_DATETIME_CONSTANTS_H_
#define DATETIME_PARSING_DATETIME_CONSTANTS_H_

#include "third_party/spanner_pg/datetime_parsing/datetime.h"

// SPANNER_PG: this file includes constants from datetime.h/datetime.cc.
// These constants may have only appeared in datetime.c but now we want to
// access them in a test file. Or they may have been extern consts in
// datetime.h and caused a symbol collision with src.
// Putting the constants in this file allows them to be accessed outside of
// datetime.cc while avoiding symbol collisions with src.

constexpr int day_tab[2][13] =
{
	{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0},
	{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0}
};

/*
 * datetktbl holds date/time keywords.
 *
 * Note that this table must be strictly alphabetically ordered to allow an
 * O(ln(N)) search algorithm to be used.
 *
 * The token field must be NUL-terminated; we truncate entries to TOKMAXLEN
 * characters to fit.
 *
 * The static table contains no TZ, DTZ, or DYNTZ entries; rather those
 * are loaded from configuration files and stored in zoneabbrevtbl, whose
 * abbrevs[] field has the same format as the static datetktbl.
 */
constexpr datetkn datetktbl[] = {
	/* token, type, value */
	{EARLY, RESERV, DTK_EARLY}, /* "-infinity" reserved for "early time" */
	{DA_D, ADBC, AD},			/* "ad" for years > 0 */
	{"allballs", RESERV, DTK_ZULU}, /* 00:00:00 */
	{"am", AMPM, AM},
	{"apr", MONTH, 4},
	{"april", MONTH, 4},
	{"at", IGNORE_DTF, 0},		/* "at" (throwaway) */
	{"aug", MONTH, 8},
	{"august", MONTH, 8},
	{DB_C, ADBC, BC},			/* "bc" for years <= 0 */
	{"d", UNITS, DTK_DAY},		/* "day of month" for ISO input */
	{"dec", MONTH, 12},
	{"december", MONTH, 12},
	{"dow", UNITS, DTK_DOW},	/* day of week */
	{"doy", UNITS, DTK_DOY},	/* day of year */
	{"dst", DTZMOD, SECS_PER_HOUR},
	{EPOCH, RESERV, DTK_EPOCH}, /* "epoch" reserved for system epoch time */
	{"feb", MONTH, 2},
	{"february", MONTH, 2},
	{"fri", DOW, 5},
	{"friday", DOW, 5},
	{"h", UNITS, DTK_HOUR},		/* "hour" */
	{LATE, RESERV, DTK_LATE},	/* "infinity" reserved for "late time" */
	{"isodow", UNITS, DTK_ISODOW},	/* ISO day of week, Sunday == 7 */
	{"isoyear", UNITS, DTK_ISOYEAR},	/* year in terms of the ISO week date */
	{"j", UNITS, DTK_JULIAN},
	{"jan", MONTH, 1},
	{"january", MONTH, 1},
	{"jd", UNITS, DTK_JULIAN},
	{"jul", MONTH, 7},
	{"julian", UNITS, DTK_JULIAN},
	{"july", MONTH, 7},
	{"jun", MONTH, 6},
	{"june", MONTH, 6},
	{"m", UNITS, DTK_MONTH},	/* "month" for ISO input */
	{"mar", MONTH, 3},
	{"march", MONTH, 3},
	{"may", MONTH, 5},
	{"mm", UNITS, DTK_MINUTE},	/* "minute" for ISO input */
	{"mon", DOW, 1},
	{"monday", DOW, 1},
	{"nov", MONTH, 11},
	{"november", MONTH, 11},
	{NOW, RESERV, DTK_NOW},		/* current transaction time */
	{"oct", MONTH, 10},
	{"october", MONTH, 10},
	{"on", IGNORE_DTF, 0},		/* "on" (throwaway) */
	{"pm", AMPM, PM},
	{"s", UNITS, DTK_SECOND},	/* "seconds" for ISO input */
	{"sat", DOW, 6},
	{"saturday", DOW, 6},
	{"sep", MONTH, 9},
	{"sept", MONTH, 9},
	{"september", MONTH, 9},
	{"sun", DOW, 0},
	{"sunday", DOW, 0},
	{"t", ISOTIME, DTK_TIME},	/* Filler for ISO time fields */
	{"thu", DOW, 4},
	{"thur", DOW, 4},
	{"thurs", DOW, 4},
	{"thursday", DOW, 4},
	{TODAY, RESERV, DTK_TODAY}, /* midnight */
	{TOMORROW, RESERV, DTK_TOMORROW},	/* tomorrow midnight */
	{"tue", DOW, 2},
	{"tues", DOW, 2},
	{"tuesday", DOW, 2},
	{"wed", DOW, 3},
	{"wednesday", DOW, 3},
	{"weds", DOW, 3},
	{"y", UNITS, DTK_YEAR},		/* "year" for ISO input */
	{YESTERDAY, RESERV, DTK_YESTERDAY}	/* yesterday midnight */
};

constexpr int szdatetktbl = sizeof datetktbl / sizeof datetktbl[0];

/*
 * deltatktbl: same format as datetktbl, but holds keywords used to represent
 * time units (eg, for intervals, and for EXTRACT).
 */
static const datetkn deltatktbl[] = {
	/* token, type, value */
	{"@", IGNORE_DTF, 0},		/* postgres relative prefix */
	{DAGO, AGO, 0},				/* "ago" indicates negative time offset */
	{"c", UNITS, DTK_CENTURY},	/* "century" relative */
	{"cent", UNITS, DTK_CENTURY},	/* "century" relative */
	{"centuries", UNITS, DTK_CENTURY},	/* "centuries" relative */
	{DCENTURY, UNITS, DTK_CENTURY}, /* "century" relative */
	{"d", UNITS, DTK_DAY},		/* "day" relative */
	{DDAY, UNITS, DTK_DAY},		/* "day" relative */
	{"days", UNITS, DTK_DAY},	/* "days" relative */
	{"dec", UNITS, DTK_DECADE}, /* "decade" relative */
	{DDECADE, UNITS, DTK_DECADE},	/* "decade" relative */
	{"decades", UNITS, DTK_DECADE}, /* "decades" relative */
	{"decs", UNITS, DTK_DECADE},	/* "decades" relative */
	{"h", UNITS, DTK_HOUR},		/* "hour" relative */
	{DHOUR, UNITS, DTK_HOUR},	/* "hour" relative */
	{"hours", UNITS, DTK_HOUR}, /* "hours" relative */
	{"hr", UNITS, DTK_HOUR},	/* "hour" relative */
	{"hrs", UNITS, DTK_HOUR},	/* "hours" relative */
	{"m", UNITS, DTK_MINUTE},	/* "minute" relative */
	{"microsecon", UNITS, DTK_MICROSEC},	/* "microsecond" relative */
	{"mil", UNITS, DTK_MILLENNIUM}, /* "millennium" relative */
	{"millennia", UNITS, DTK_MILLENNIUM},	/* "millennia" relative */
	{DMILLENNIUM, UNITS, DTK_MILLENNIUM},	/* "millennium" relative */
	{"millisecon", UNITS, DTK_MILLISEC},	/* relative */
	{"mils", UNITS, DTK_MILLENNIUM},	/* "millennia" relative */
	{"min", UNITS, DTK_MINUTE}, /* "minute" relative */
	{"mins", UNITS, DTK_MINUTE},	/* "minutes" relative */
	{DMINUTE, UNITS, DTK_MINUTE},	/* "minute" relative */
	{"minutes", UNITS, DTK_MINUTE}, /* "minutes" relative */
	{"mon", UNITS, DTK_MONTH},	/* "months" relative */
	{"mons", UNITS, DTK_MONTH}, /* "months" relative */
	{DMONTH, UNITS, DTK_MONTH}, /* "month" relative */
	{"months", UNITS, DTK_MONTH},
	{"ms", UNITS, DTK_MILLISEC},
	{"msec", UNITS, DTK_MILLISEC},
	{DMILLISEC, UNITS, DTK_MILLISEC},
	{"mseconds", UNITS, DTK_MILLISEC},
	{"msecs", UNITS, DTK_MILLISEC},
	{"qtr", UNITS, DTK_QUARTER},	/* "quarter" relative */
	{DQUARTER, UNITS, DTK_QUARTER}, /* "quarter" relative */
	{"s", UNITS, DTK_SECOND},
	{"sec", UNITS, DTK_SECOND},
	{DSECOND, UNITS, DTK_SECOND},
	{"seconds", UNITS, DTK_SECOND},
	{"secs", UNITS, DTK_SECOND},
	{DTIMEZONE, UNITS, DTK_TZ}, /* "timezone" time offset */
	{"timezone_h", UNITS, DTK_TZ_HOUR}, /* timezone hour units */
	{"timezone_m", UNITS, DTK_TZ_MINUTE},	/* timezone minutes units */
	{"us", UNITS, DTK_MICROSEC},	/* "microsecond" relative */
	{"usec", UNITS, DTK_MICROSEC},	/* "microsecond" relative */
	{DMICROSEC, UNITS, DTK_MICROSEC},	/* "microsecond" relative */
	{"useconds", UNITS, DTK_MICROSEC},	/* "microseconds" relative */
	{"usecs", UNITS, DTK_MICROSEC}, /* "microseconds" relative */
	{"w", UNITS, DTK_WEEK},		/* "week" relative */
	{DWEEK, UNITS, DTK_WEEK},	/* "week" relative */
	{"weeks", UNITS, DTK_WEEK}, /* "weeks" relative */
	{"y", UNITS, DTK_YEAR},		/* "year" relative */
	{DYEAR, UNITS, DTK_YEAR},	/* "year" relative */
	{"years", UNITS, DTK_YEAR}, /* "years" relative */
	{"yr", UNITS, DTK_YEAR},	/* "year" relative */
	{"yrs", UNITS, DTK_YEAR}	/* "years" relative */
};

static const int szdeltatktbl = sizeof deltatktbl / sizeof deltatktbl[0];

/*
 * TimezoneAbbrevTable holds timezone abbreviations
 *
 * Note that this table must be strictly alphabetically ordered to allow an
 * O(ln(N)) search algorithm to be used.
 *
 * The token field must be NUL-terminated; we truncate entries to TOKMAXLEN
 * characters to fit.
 *
 */
constexpr datetkn TimezoneAbbrevTable[] = {
	{"z", TZ, 0}
};

static const int TimezoneAbbrevTableSize =
	sizeof TimezoneAbbrevTable / sizeof TimezoneAbbrevTable[0];

#endif  // DATETIME_PARSING_DATETIME_CONSTANTS_H_
