/*-------------------------------------------------------------------------
 *
 * datetime.h
 *	  Definitions for date/time support code.
 *	  The support code is shared with other date data types,
 *	   including date, and time.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * datetime_parsing/datetime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATETIME_PARSING_DATETIME_H_
#define DATETIME_PARSING_DATETIME_H_

#include <stddef.h>

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
// c.h must be included before other PG files
#include "third_party/spanner_pg/datetime_parsing/c.h"
#include "third_party/spanner_pg/datetime_parsing/pgtz.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp_macros.h"

// SPANNER_PG: copied from src/include/utils/datetime.h

/* ----------------------------------------------------------------
 *				time types + support macros
 *
 * String definitions for standard time quantities.
 *
 * These strings are the defaults used to form output time strings.
 * Other alternative forms are hardcoded into token tables in datetime.c.
 * ----------------------------------------------------------------
 */

#define DAGO			"ago"
#define DCURRENT		"current"
#define EPOCH			"epoch"
#define INVALID			"invalid"
#define EARLY			"-infinity"
#define LATE			"infinity"
#define NOW				"now"
#define TODAY			"today"
#define TOMORROW		"tomorrow"
#define YESTERDAY		"yesterday"
#define ZULU			"zulu"

#define DMICROSEC		"usecond"
#define DMILLISEC		"msecond"
#define DSECOND			"second"
#define DMINUTE			"minute"
#define DHOUR			"hour"
#define DDAY			"day"
#define DWEEK			"week"
#define DMONTH			"month"
#define DQUARTER		"quarter"
#define DYEAR			"year"
#define DDECADE			"decade"
#define DCENTURY		"century"
#define DMILLENNIUM		"millennium"
#define DA_D			"ad"
#define DB_C			"bc"
#define DTIMEZONE		"timezone"

/*
 * Fundamental time field definitions for parsing.
 *
 *	Meridian:  am, pm, or 24-hour style.
 *	Millennium: ad, bc
 */

#define AM		0
#define PM		1
#define HR24	2

#define AD		0
#define BC		1

/*
 * Field types for time decoding.
 *
 * Can't have more of these than there are bits in an unsigned int
 * since these are turned into bit masks during parsing and decoding.
 *
 * Furthermore, the values for YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
 * must be in the range 0..14 so that the associated bitmasks can fit
 * into the left half of an INTERVAL's typmod value.  Since those bits
 * are stored in typmods, you can't change them without initdb!
 */

#define RESERV	0
#define MONTH	1
#define YEAR	2
#define DAY		3
#define JULIAN	4
#define TZ		5				/* fixed-offset timezone abbreviation */
#define DTZ		6				/* fixed-offset timezone abbrev, DST */
#define DYNTZ	7				/* dynamic timezone abbreviation */
#define IGNORE_DTF	8
#define AMPM	9
#define HOUR	10
#define MINUTE	11
#define SECOND	12
#define MILLISECOND 13
#define MICROSECOND 14
#define DOY		15
#define DOW		16
#define UNITS	17
#define ADBC	18
/* these are only for relative dates */
#define AGO		19
#define ABS_BEFORE		20
#define ABS_AFTER		21
/* generic fields to help with parsing */
#define ISODATE 22
#define ISOTIME 23
/* these are only for parsing intervals */
#define WEEK		24
#define DECADE		25
#define CENTURY		26
#define MILLENNIUM	27
/* hack for parsing two-word timezone specs "MET DST" etc */
#define DTZMOD	28				/* "DST" as a separate word */
/* reserved for unrecognized string values */
#define UNKNOWN_FIELD	31

/*
 * Token field definitions for time parsing and decoding.
 *
 * Some field type codes (see above) use these as the "value" in datetktbl[].
 * These are also used for bit masks in DecodeDateTime and friends
 *	so actually restrict them to within [0,31] for now.
 * - thomas 97/06/19
 * Not all of these fields are used for masks in DecodeDateTime
 *	so allow some larger than 31. - thomas 1997-11-17
 *
 * Caution: there are undocumented assumptions in the code that most of these
 * values are not equal to IGNORE_DTF nor RESERV.  Be very careful when
 * renumbering values in either of these apparently-independent lists :-(
 */

#define DTK_NUMBER		0
#define DTK_STRING		1

#define DTK_DATE		2
#define DTK_TIME		3
#define DTK_TZ			4
#define DTK_AGO			5

#define DTK_SPECIAL		6
#define DTK_EARLY		9
#define DTK_LATE		10
#define DTK_EPOCH		11
#define DTK_NOW			12
#define DTK_YESTERDAY	13
#define DTK_TODAY		14
#define DTK_TOMORROW	15
#define DTK_ZULU		16

#define DTK_DELTA		17
#define DTK_SECOND		18
#define DTK_MINUTE		19
#define DTK_HOUR		20
#define DTK_DAY			21
#define DTK_WEEK		22
#define DTK_MONTH		23
#define DTK_QUARTER		24
#define DTK_YEAR		25
#define DTK_DECADE		26
#define DTK_CENTURY		27
#define DTK_MILLENNIUM	28
#define DTK_MILLISEC	29
#define DTK_MICROSEC	30
#define DTK_JULIAN		31

#define DTK_DOW			32
#define DTK_DOY			33
#define DTK_TZ_HOUR		34
#define DTK_TZ_MINUTE	35
#define DTK_ISOYEAR		36
#define DTK_ISODOW		37


/*
 * Bit mask definitions for time parsing.
 */

#define DTK_M(t)		(0x01 << (t))

/* Convenience: a second, plus any fractional component */
#define DTK_ALL_SECS_M	(DTK_M(SECOND) | DTK_M(MILLISECOND) | DTK_M(MICROSECOND))
#define DTK_DATE_M		(DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY))
#define DTK_TIME_M		(DTK_M(HOUR) | DTK_M(MINUTE) | DTK_ALL_SECS_M)

/*
 * Working buffer size for input and output of interval, timestamp, etc.
 * Inputs that need more working space will be rejected early.  Longer outputs
 * will overrun buffers, so this must suffice for all possible output.  As of
 * this writing, interval_out() needs the most space at ~90 bytes.
 */
#define MAXDATELEN		128
/* maximum possible number of fields in a date string */
#define MAXDATEFIELDS	25
/* only this many chars are stored in datetktbl */
#define TOKMAXLEN		10

/* keep this struct small; it gets used a lot */
typedef struct
{
	char		token[TOKMAXLEN + 1];	/* always NUL-terminated */
	char		type;			/* see field type codes above */
	int32_t		value;			/* meaning depends on type */
} datetkn;

/*
 * These are the rules for the Gregorian calendar, which was adopted in 1582.
 * However, we use this calculation for all prior years as well because the
 * SQL standard specifies use of the Gregorian calendar.  This prevents the
 * date 1500-02-29 from being stored, even though it is valid in the Julian
 * calendar.
 */
#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))


/*
 * Datetime input parsing routines (ParseDateTime, DecodeDateTime, etc)
 * return zero or a positive value on success.  On failure, they return
 * one of these negative code values.  DateTimeParseError may be used to
 * produce a correct ereport.
 */
#define DTERR_BAD_FORMAT		(-1)
#define DTERR_FIELD_OVERFLOW	(-2)
#define DTERR_MD_FIELD_OVERFLOW (-3)	/* triggers hint about DateStyle */
#define DTERR_INTERVAL_OVERFLOW (-4)
#define DTERR_TZDISP_OVERFLOW	(-5)

void j2date(int jd, int *year, int *month, int *day);
int	date2j(int year, int month, int day);

int	ParseDateTime(absl::string_view timestr, char *workbuf, size_t buflen,
						  char **field, int *ftype,
						  int maxfields, int *numfields);
/*
 * SPANNER_PG: include a default timezone and an optional error string for error
 * messages that are more specific than the error code that is returned.
 */
int	DecodeDateTime(char **field, int *ftype,
						int nf, int *dtype,
						struct pg_tm *tm, fsec_t *fsec, int *tzp,
						absl::string_view default_timezone, std::string* error);
int	DecodeTimezone(char *str, int *tzp);

int DecodeInterval(char **field, int *ftype, int nf, int range, int *dtype,
                   struct pg_tm *tm, fsec_t *fsec);
int	DecodeISO8601Interval(char *str,
								  int *dtype, struct pg_tm *tm, fsec_t *fsec);

/* SPANNER_PG: return an absl::Status instead of using ereport */
absl::Status DateTimeParseError(int dterr, absl::string_view str,
                                       const char *datatype);
int	DetermineTimeZoneOffset(struct pg_tm *tm, pg_tz *tzp);

int	ValidateDate(int fmask, bool isjulian, bool is2digits, bool bc,
						 struct pg_tm *tm);
int	DecodeTimezoneAbbrev(int field, char *lowtoken, int *offset, pg_tz **tz);
int	DecodeSpecial(int field, char *lowtoken, int *val);
int DecodeUnits(int field, char *lowtoken, int *val);

#endif  // DATETIME_PARSING_DATETIME_H_
