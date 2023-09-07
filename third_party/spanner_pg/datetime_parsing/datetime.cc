/*-------------------------------------------------------------------------
 *
 * datetime.cc
 *	  Support functions for date/time types.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  datetime_parsing/datetime.cc
 *
 *-------------------------------------------------------------------------
 */
#include "third_party/spanner_pg/datetime_parsing/datetime.h"

#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datetime_parsing/date.h"
#include "third_party/spanner_pg/datetime_parsing/datetime_constants.h"
#include "third_party/spanner_pg/datetime_parsing/miscadmin.h"
#include "third_party/spanner_pg/datetime_parsing/pgtime.h"
#include "third_party/spanner_pg/datetime_parsing/port.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp_macros.h"

// SPANNER_PG: copied from src/backend/utils/adt/datetime.c
// clang-format off
// This file is intentionally formatted according to PG style.

static int	DecodeNumber(int flen, char *field, bool haveTextMonth,
						 int fmask, int *tmask,
						 struct pg_tm *tm, fsec_t *fsec, bool *is2digits);
static int	DecodeNumberField(int len, char *str,
							  int fmask, int *tmask,
							  struct pg_tm *tm, fsec_t *fsec, bool *is2digits);
static int	DecodeTime(char *str, int fmask, int range,
					   int *tmask, struct pg_tm *tm, fsec_t *fsec);
static const datetkn *datebsearch(const char *key, const datetkn *base, int nel);
static int	DecodeDate(char *str, int fmask, int *tmask, bool *is2digits,
					   struct pg_tm *tm);
static int	DetermineTimeZoneOffsetInternal(struct pg_tm *tm, pg_tz *tzp,
											pg_time_t *tp);

/*
 * SPANNER_PG: replace special PG functions with their non-special semi
 * equivalents.
 */
int
strtoint(const char * str, char ** endptr, int base) {
	return static_cast<int>(strtol(str, endptr, base));
}


/*****************************************************************************
 *	 PRIVATE ROUTINES														 *
 *****************************************************************************/

/* Caches of recent lookup results in the above tables */

/* SPANGRES BEGIN */
// Make these caches thread-local.
// Note that unlike other PostgreSQL caches, these are backed by static data and
// remain valid across invocations, so they do not need to be cleared after use.
static __thread const datetkn *datecache[MAXDATEFIELDS] = {NULL};
static __thread const datetkn *deltacache[MAXDATEFIELDS] = {NULL};
static __thread const datetkn *abbrevcache[MAXDATEFIELDS] = {NULL};
/* SPANGRES END */


/*
 * Calendar time to Julian date conversions.
 * Julian date is commonly used in astronomical applications,
 *	since it is numerically accurate and computationally simple.
 * The algorithms here will accurately convert between Julian day
 *	and calendar date for all non-negative Julian days
 *	(i.e. from Nov 24, -4713 on).
 *
 * Rewritten to eliminate overflow problems. This now allows the
 * routines to work correctly for all Julian day counts from
 * 0 to 2147483647	(Nov 24, -4713 to Jun 3, 5874898) assuming
 * a 32-bit integer. Longer types should also work to the limits
 * of their precision.
 *
 * Actually, date2j() will work reasonably, in the sense of producing
 * valid negative Julian dates, significantly before Nov 24, -4713.
 * We rely on it to do so back to Nov 1, -4713; see IS_VALID_JULIAN()
 * and associated commentary in timestamp.h.
 */

int
date2j(int y, int m, int d)
{
	int			julian;
	int			century;

	if (m > 2)
	{
		m += 1;
		y += 4800;
	}
	else
	{
		m += 13;
		y += 4799;
	}

	century = y / 100;
	julian = y * 365 - 32167;
	julian += y / 4 - century + century / 4;
	julian += 7834 * m / 256 + d;

	return julian;
}								/* date2j() */

void
j2date(int jd, int *year, int *month, int *day)
{
	unsigned int julian;
	unsigned int quad;
	unsigned int extra;
	int			y;

	julian = jd;
	julian += 32044;
	quad = julian / 146097;
	extra = (julian - quad * 146097) * 4 + 3;
	julian += 60 + quad * 3 + extra / 146097;
	quad = julian / 1461;
	julian -= quad * 1461;
	y = julian * 4 / 1461;
	julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
		+ 123;
	y += quad * 4;
	*year = y - 4800;
	quad = julian * 2141 / 65536;
	*day = julian - 7834 * quad / 256;
	*month = (quad + 10) % MONTHS_PER_YEAR + 1;
}								/* j2date() */


/*
 * j2day - convert Julian date to day-of-week (0..6 == Sun..Sat)
 *
 * Note: various places use the locution j2day(date - 1) to produce a
 * result according to the convention 0..6 = Mon..Sun.  This is a bit of
 * a crock, but will work as long as the computation here is just a modulo.
 */
int
j2day(int date)
{
	date += 1;
	date %= 7;
	/* Cope if division truncates towards zero, as it probably does */
	if (date < 0)
		date += 7;

	return date;
}								/* j2day() */


/* Fetch a fractional-second value with suitable error checking */
static int
ParseFractionalSecond(char *cp, fsec_t *fsec)
{
	double		frac;

	/* Caller should always pass the start of the fraction part */
	assert(*cp == '.');
	errno = 0;
	frac = strtod(cp, &cp);
	/* check for parse failure */
	if (*cp != '\0' || errno != 0)
		return DTERR_BAD_FORMAT;
	*fsec = rint(frac * 1000000);
	return 0;
}


/* ParseDateTime()
 *	Break string into tokens based on a date/time context.
 *	Returns 0 if successful, DTERR code if bogus input detected.
 *
 * timestr - the input string
 * workbuf - workspace for field string storage. This must be
 *	 larger than the largest legal input for this datetime type --
 *	 some additional space will be needed to NUL terminate fields.
 * buflen - the size of workbuf
 * field[] - pointers to field strings are returned in this array
 * ftype[] - field type indicators are returned in this array
 * maxfields - dimensions of the above two arrays
 * *numfields - set to the actual number of fields detected
 *
 * The fields extracted from the input are stored as separate,
 * null-terminated strings in the workspace at workbuf. Any text is
 * converted to lower case.
 *
 * Several field types are assigned:
 *	DTK_NUMBER - digits and (possibly) a decimal point
 *	DTK_DATE - digits and two delimiters, or digits and text
 *	DTK_TIME - digits, colon delimiters, and possibly a decimal point
 *	DTK_STRING - text (no digits or punctuation)
 *	DTK_SPECIAL - leading "+" or "-" followed by text
 *	DTK_TZ - leading "+" or "-" followed by digits (also eats ':', '.', '-')
 *
 * Note that some field types can hold unexpected items:
 *	DTK_NUMBER can hold date fields (yy.ddd)
 *	DTK_STRING can hold months (January) and time zones (PST)
 *	DTK_DATE can hold time zone names (America/New_York, GMT-8)
 */
int
ParseDateTime(absl::string_view timestr, char *workbuf, size_t buflen,
			  char **field, int *ftype, int maxfields, int *numfields)
{
	int			nf = 0;
	const char *cp = timestr.data();
	const char* cp_end = timestr.data() + timestr.size();
	char	   *bufp = workbuf;
	const char *bufend = workbuf + buflen;

	/*
	 * Set the character pointed-to by "bufptr" to "newchar", and increment
	 * "bufptr". "end" gives the end of the buffer -- we return an error if
	 * there is no space left to append a character to the buffer. Note that
	 * "bufptr" is evaluated twice.
	 */
#define APPEND_CHAR(bufptr, end, newchar)		\
	do											\
	{											\
		if (((bufptr) + 1) >= (end))			\
			return DTERR_BAD_FORMAT;			\
		*(bufptr)++ = newchar;					\
	} while (0)

	/* outer loop through fields */
	while (cp != cp_end)
	{
		/* Ignore spaces between fields */
		if (isspace((unsigned char) *cp))
		{
			cp++;
			continue;
		}

		/* Record start of current field */
		if (nf >= maxfields)
			return DTERR_BAD_FORMAT;
		field[nf] = bufp;

		/* leading digit? then date or time */
		if (isdigit((unsigned char) *cp))
		{
			APPEND_CHAR(bufp, bufend, *cp++);
			while (cp != cp_end && isdigit((unsigned char) *cp))
				APPEND_CHAR(bufp, bufend, *cp++);

			/* time field? */
			if (cp != cp_end && *cp == ':')
			{
				ftype[nf] = DTK_TIME;
				APPEND_CHAR(bufp, bufend, *cp++);
				while (cp != cp_end && (isdigit((unsigned char) *cp) ||
					(*cp == ':') || (*cp == '.')))
					APPEND_CHAR(bufp, bufend, *cp++);
			}
			/* date field? allow embedded text month */
			else if (cp != cp_end && (*cp == '-' || *cp == '/' || *cp == '.'))
			{
				/* save delimiting character to use later */
				char		delim = *cp;

				APPEND_CHAR(bufp, bufend, *cp++);
				/* second field is all digits? then no embedded text month */
				if (cp != cp_end && isdigit((unsigned char) *cp))
				{
					ftype[nf] = ((delim == '.') ? DTK_NUMBER : DTK_DATE);
					while (cp != cp_end && isdigit((unsigned char) *cp))
						APPEND_CHAR(bufp, bufend, *cp++);

					/*
					 * insist that the delimiters match to get a three-field
					 * date.
					 */
					if (cp != cp_end && *cp == delim)
					{
						ftype[nf] = DTK_DATE;
						APPEND_CHAR(bufp, bufend, *cp++);
						while (cp != cp_end &&
									(isdigit((unsigned char) *cp) || *cp == delim))
							APPEND_CHAR(bufp, bufend, *cp++);
					}
				}
				else
				{
					ftype[nf] = DTK_DATE;
					while (cp != cp_end && (isalnum((unsigned char) *cp) || *cp == delim))
						APPEND_CHAR(bufp, bufend, pg_tolower((unsigned char) *cp++));
				}
			}

			/*
			 * otherwise, number only and will determine year, month, day, or
			 * concatenated fields later...
			 */
			else
				ftype[nf] = DTK_NUMBER;
		}
		/* Leading decimal point? Then fractional seconds... */
		else if (*cp == '.')
		{
			APPEND_CHAR(bufp, bufend, *cp++);
			while (cp != cp_end && isdigit((unsigned char) *cp))
				APPEND_CHAR(bufp, bufend, *cp++);

			ftype[nf] = DTK_NUMBER;
		}

		/*
		 * text? then date string, month, day of week, special, or timezone
		 */
		else if (isalpha((unsigned char) *cp))
		{
			bool		is_date;

			ftype[nf] = DTK_STRING;
			APPEND_CHAR(bufp, bufend, pg_tolower((unsigned char) *cp++));
			while (cp != cp_end && isalpha((unsigned char) *cp))
				APPEND_CHAR(bufp, bufend, pg_tolower((unsigned char) *cp++));

			/*
			 * Dates can have embedded '-', '/', or '.' separators.  It could
			 * also be a timezone name containing embedded '/', '+', '-', '_',
			 * or ':' (but '_' or ':' can't be the first punctuation). If the
			 * next character is a digit or '+', we need to check whether what
			 * we have so far is a recognized non-timezone keyword --- if so,
			 * don't believe that this is the start of a timezone.
			 */
			is_date = false;
			if (cp != cp_end && (*cp == '-' || *cp == '/' || *cp == '.'))
				is_date = true;
			else if (cp != cp_end && (*cp == '+' || isdigit((unsigned char) *cp)))
			{
				*bufp = '\0';	/* null-terminate current field value */
				/* we need search only the core token table, not TZ names */
				if (datebsearch(field[nf], datetktbl, szdatetktbl) == NULL)
					is_date = true;
			}
			if (is_date)
			{
				ftype[nf] = DTK_DATE;
				do
				{
					APPEND_CHAR(bufp, bufend, pg_tolower((unsigned char) *cp++));
				} while (cp != cp_end && (*cp == '+' || *cp == '-' ||
						 *cp == '/' || *cp == '_' ||
						 *cp == '.' || *cp == ':' ||
						 isalnum((unsigned char) *cp)));
			}
		}
		/* sign? then special or numeric timezone */
		else if (*cp == '+' || *cp == '-')
		{
			APPEND_CHAR(bufp, bufend, *cp++);
			/* soak up leading whitespace */
			while (cp != cp_end && isspace((unsigned char) *cp))
				cp++;
			/* numeric timezone? */
			/* note that "DTK_TZ" could also be a signed float or yyyy-mm */
			if (cp != cp_end && (isdigit((unsigned char) *cp)))
			{
				ftype[nf] = DTK_TZ;
				APPEND_CHAR(bufp, bufend, *cp++);
				while (cp != cp_end && (isdigit((unsigned char) *cp) ||
						*cp == ':' || *cp == '.' || *cp == '-'))
					APPEND_CHAR(bufp, bufend, *cp++);
			}
			/* special? */
			else if (cp != cp_end && (isalpha((unsigned char) *cp)))
			{
				ftype[nf] = DTK_SPECIAL;
				APPEND_CHAR(bufp, bufend, pg_tolower((unsigned char) *cp++));
				while (cp != cp_end && isalpha((unsigned char) *cp))
					APPEND_CHAR(bufp, bufend, pg_tolower((unsigned char) *cp++));
			}
			/* otherwise something wrong... */
			else
				return DTERR_BAD_FORMAT;
		}
		/* ignore other punctuation but use as delimiter */
		else if (ispunct((unsigned char) *cp))
		{
			cp++;
			continue;
		}
		/* otherwise, something is not right... */
		else
			return DTERR_BAD_FORMAT;

		/* force in a delimiter after each field */
		*bufp++ = '\0';
		nf++;
	}

	*numfields = nf;

	return 0;
}


/* DecodeDateTime()
 * Interpret previously parsed fields for general date and time.
 * Return 0 if full date, 1 if only time, and negative DTERR code if problems.
 * (Currently, all callers treat 1 as an error return too.)
 *
 *		External format(s):
 *				"<weekday> <month>-<day>-<year> <hour>:<minute>:<second>"
 *				"Fri Feb-7-1997 15:23:27"
 *				"Feb-7-1997 15:23:27"
 *				"2-7-1997 15:23:27"
 *				"1997-2-7 15:23:27"
 *				"1997.038 15:23:27"		(day of year 1-366)
 *		Also supports input in compact time:
 *				"970207 152327"
 *				"97038 152327"
 *				"20011225T040506.789-07"
 *
 * Use the system-provided functions to get the current time zone
 * if not specified in the input string.
 *
 * If the date is outside the range of pg_time_t (in practice that could only
 * happen if pg_time_t is just 32 bits), then assume UTC time zone - thomas
 * 1997-05-27
 */
int
DecodeDateTime(char **field, int *ftype, int nf,
			int *dtype, struct pg_tm *tm, fsec_t *fsec, int *tzp,
			absl::string_view default_timezone, std::string* error)
{
	int			fmask = 0,
				tmask,
				type;
	int			ptype = 0;		/* "prefix type" for ISO y2001m02d04 format */
	int			i;
	int			val;
	int			dterr;
	int			mer = HR24;
	bool		haveTextMonth = false;
	bool		isjulian = false;
	bool		is2digits = false;
	bool		bc = false;
	pg_tz	   *namedTz = NULL;
	pg_tz	   *abbrevTz = NULL;
	pg_tz	   *valtz = NULL;
	char	   *abbrev = NULL;

	/*
	 * We'll insist on at least all of the date fields, but initialize the
	 * remaining fields in case they are not set later...
	 */
	*dtype = DTK_DATE;
	tm->tm_hour = 0;
	tm->tm_min = 0;
	tm->tm_sec = 0;
	*fsec = 0;
	/* don't know daylight savings time status apriori */
	tm->tm_isdst = -1;
	if (tzp != NULL)
		*tzp = 0;

	for (i = 0; i < nf; i++)
	{
		switch (ftype[i])
		{
			case DTK_DATE:

				/*
				 * Integral julian day with attached time zone? All other
				 * forms with JD will be separated into distinct fields, so we
				 * handle just this case here.
				 */
				if (ptype == DTK_JULIAN)
				{
					char	   *cp;
					int			val;

					if (tzp == NULL)
						return DTERR_BAD_FORMAT;

					errno = 0;
					val = strtoint(field[i], &cp, 10);
					if (errno == ERANGE || val < 0)
						return DTERR_FIELD_OVERFLOW;

					j2date(val, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
					isjulian = true;

					/* Get the time zone from the end of the string */
					dterr = DecodeTimezone(cp, tzp);
					if (dterr)
						return dterr;

					tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
					ptype = 0;
					break;
				}

				/*
				 * Already have a date? Then this might be a time zone name
				 * with embedded punctuation (e.g. "America/New_York") or a
				 * run-together time with trailing time zone (e.g. hhmmss-zz).
				 * - thomas 2001-12-25
				 *
				 * We consider it a time zone if we already have month & day.
				 * This is to allow the form "mmm dd hhmmss tz year", which
				 * we've historically accepted.
				 */
				else if (ptype != 0 ||
						 ((fmask & (DTK_M(MONTH) | DTK_M(DAY))) ==
						  (DTK_M(MONTH) | DTK_M(DAY))))
				{
					/* No time zone accepted? Then quit... */
					if (tzp == NULL)
						return DTERR_BAD_FORMAT;

					if (isdigit((unsigned char) *field[i]) || ptype != 0)
					{
						char	   *cp;

						if (ptype != 0)
						{
							/* Validity check; should not fail this test */
							if (ptype != DTK_TIME)
								return DTERR_BAD_FORMAT;
							ptype = 0;
						}

						/*
						 * Starts with a digit but we already have a time
						 * field? Then we are in trouble with a date and time
						 * already...
						 */
						if ((fmask & DTK_TIME_M) == DTK_TIME_M)
							return DTERR_BAD_FORMAT;

						if ((cp = strchr(field[i], '-')) == NULL)
							return DTERR_BAD_FORMAT;

						/* Get the time zone from the end of the string */
						dterr = DecodeTimezone(cp, tzp);
						if (dterr)
							return dterr;
						*cp = '\0';

						/*
						 * Then read the rest of the field as a concatenated
						 * time
						 */
						dterr = DecodeNumberField(strlen(field[i]), field[i],
												  fmask,
												  &tmask, tm,
												  fsec, &is2digits);
						if (dterr < 0)
							return dterr;

						/*
						 * modify tmask after returning from
						 * DecodeNumberField()
						 */
						tmask |= DTK_M(TZ);
					}
					else
					{
						namedTz = pg_tzset(field[i]);
						if (!namedTz)
						{
							/*
							 * We should return an error code instead of
							 * ereport'ing directly, but then there is no way
							 * to report the bad time zone name.
							 */
							  /* SPANNER_PG: set an error message using of using ereport */
							  *error = "time zone \"" + std::string(field[i]) +
								   "\" not recognized";
							  return DTERR_BAD_FORMAT;;
						}
						/* we'll apply the zone setting below */
						tmask = DTK_M(TZ);
					}
				}
				else
				{
					dterr = DecodeDate(field[i], fmask,
									   &tmask, &is2digits, tm);
					if (dterr)
						return dterr;
				}
				break;

			case DTK_TIME:

				/*
				 * This might be an ISO time following a "t" field.
				 */
				if (ptype != 0)
				{
					/* Validity check; should not fail this test */
					if (ptype != DTK_TIME)
						return DTERR_BAD_FORMAT;
					ptype = 0;
				}
				dterr = DecodeTime(field[i], fmask, INTERVAL_FULL_RANGE,
								   &tmask, tm, fsec);
				if (dterr)
					return dterr;

				/* check for time overflow */
				if (time_overflows(tm->tm_hour, tm->tm_min, tm->tm_sec,
								   *fsec))
					return DTERR_FIELD_OVERFLOW;
				break;

			case DTK_TZ:
				{
					int			tz;

					if (tzp == NULL)
						return DTERR_BAD_FORMAT;

					dterr = DecodeTimezone(field[i], &tz);
					if (dterr)
						return dterr;
					*tzp = tz;
					tmask = DTK_M(TZ);
				}
				break;

			case DTK_NUMBER:

				/*
				 * Was this an "ISO date" with embedded field labels? An
				 * example is "y2001m02d04" - thomas 2001-02-04
				 */
				if (ptype != 0)
				{
					char	   *cp;
					int			val;

					errno = 0;
					val = strtoint(field[i], &cp, 10);
					if (errno == ERANGE)
						return DTERR_FIELD_OVERFLOW;

					/*
					 * only a few kinds are allowed to have an embedded
					 * decimal
					 */
					if (*cp == '.')
						switch (ptype)
						{
							case DTK_JULIAN:
							case DTK_TIME:
							case DTK_SECOND:
								break;
							default:
								return DTERR_BAD_FORMAT;
								break;
						}
					else if (*cp != '\0')
						return DTERR_BAD_FORMAT;

					switch (ptype)
					{
						case DTK_YEAR:
							tm->tm_year = val;
							tmask = DTK_M(YEAR);
							break;

						case DTK_MONTH:

							/*
							 * already have a month and hour? then assume
							 * minutes
							 */
							if ((fmask & DTK_M(MONTH)) != 0 &&
								(fmask & DTK_M(HOUR)) != 0)
							{
								tm->tm_min = val;
								tmask = DTK_M(MINUTE);
							}
							else
							{
								tm->tm_mon = val;
								tmask = DTK_M(MONTH);
							}
							break;

						case DTK_DAY:
							tm->tm_mday = val;
							tmask = DTK_M(DAY);
							break;

						case DTK_HOUR:
							tm->tm_hour = val;
							tmask = DTK_M(HOUR);
							break;

						case DTK_MINUTE:
							tm->tm_min = val;
							tmask = DTK_M(MINUTE);
							break;

						case DTK_SECOND:
							tm->tm_sec = val;
							tmask = DTK_M(SECOND);
							if (*cp == '.')
							{
								dterr = ParseFractionalSecond(cp, fsec);
								if (dterr)
									return dterr;
								tmask = DTK_ALL_SECS_M;
							}
							break;

						case DTK_TZ:
							tmask = DTK_M(TZ);
							dterr = DecodeTimezone(field[i], tzp);
							if (dterr)
								return dterr;
							break;

						case DTK_JULIAN:
							/* previous field was a label for "julian date" */
							if (val < 0)
								return DTERR_FIELD_OVERFLOW;
							tmask = DTK_DATE_M;
							j2date(val, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
							isjulian = true;

							/* fractional Julian Day? */
							if (*cp == '.')
							{
								double		time;

								errno = 0;
								time = strtod(cp, &cp);
								if (*cp != '\0' || errno != 0)
									return DTERR_BAD_FORMAT;
								time *= USECS_PER_DAY;
								dt2time(time,
										&tm->tm_hour, &tm->tm_min,
										&tm->tm_sec, fsec);
								tmask |= DTK_TIME_M;
							}
							break;

						case DTK_TIME:
							/* previous field was "t" for ISO time */
							dterr = DecodeNumberField(strlen(field[i]), field[i],
													  (fmask | DTK_DATE_M),
													  &tmask, tm,
													  fsec, &is2digits);
							if (dterr < 0)
								return dterr;
							if (tmask != DTK_TIME_M)
								return DTERR_BAD_FORMAT;
							break;

						default:
							return DTERR_BAD_FORMAT;
							break;
					}

					ptype = 0;
					*dtype = DTK_DATE;
				}
				else
				{
					char	   *cp;
					int			flen;

					flen = strlen(field[i]);
					cp = strchr(field[i], '.');

					/* Embedded decimal and no date yet? */
					if (cp != NULL && !(fmask & DTK_DATE_M))
					{
						dterr = DecodeDate(field[i], fmask,
										   &tmask, &is2digits, tm);
						if (dterr)
							return dterr;
					}
					/* embedded decimal and several digits before? */
					else if (cp != NULL && flen - strlen(cp) > 2)
					{
						/*
						 * Interpret as a concatenated date or time Set the
						 * type field to allow decoding other fields later.
						 * Example: 20011223 or 040506
						 */
						dterr = DecodeNumberField(flen, field[i], fmask,
												  &tmask, tm,
												  fsec, &is2digits);
						if (dterr < 0)
							return dterr;
					}

					/*
					 * Is this a YMD or HMS specification, or a year number?
					 * YMD and HMS are required to be six digits or more, so
					 * if it is 5 digits, it is a year.  If it is six or more
					 * digits, we assume it is YMD or HMS unless no date and
					 * no time values have been specified.  This forces 6+
					 * digit years to be at the end of the string, or to use
					 * the ISO date specification.
					 */
					else if (flen >= 6 && (!(fmask & DTK_DATE_M) ||
										   !(fmask & DTK_TIME_M)))
					{
						dterr = DecodeNumberField(flen, field[i], fmask,
												  &tmask, tm,
												  fsec, &is2digits);
						if (dterr < 0)
							return dterr;
					}
					/* otherwise it is a single date/time field... */
					else
					{
						dterr = DecodeNumber(flen, field[i],
											 haveTextMonth, fmask,
											 &tmask, tm,
											 fsec, &is2digits);
						if (dterr)
							return dterr;
					}
				}
				break;

			case DTK_STRING:
			case DTK_SPECIAL:
				/* timezone abbrevs take precedence over built-in tokens */
				type = DecodeTimezoneAbbrev(i, field[i], &val, &valtz);

				if (type == UNKNOWN_FIELD)
					type = DecodeSpecial(i, field[i], &val);
				if (type == IGNORE_DTF)
					continue;

				tmask = DTK_M(type);
				switch (type)
				{
					case RESERV:
						switch (val)
						{
							case DTK_NOW:
							case DTK_YESTERDAY:
							case DTK_TODAY:
							case DTK_TOMORROW:
							  /* SPANNER_PG: return an error for relative timestamps */
							  *error = "Current/relative datetimes are not supported";
							  return DTERR_BAD_FORMAT;

							case DTK_ZULU:
								tmask = (DTK_TIME_M | DTK_M(TZ));
								*dtype = DTK_DATE;
								tm->tm_hour = 0;
								tm->tm_min = 0;
								tm->tm_sec = 0;
								if (tzp != NULL)
									*tzp = 0;
								break;

							default:
								*dtype = val;
						}

						break;

					case MONTH:

						/*
						 * already have a (numeric) month? then see if we can
						 * substitute...
						 */
						if ((fmask & DTK_M(MONTH)) && !haveTextMonth &&
							!(fmask & DTK_M(DAY)) && tm->tm_mon >= 1 &&
							tm->tm_mon <= 31)
						{
							tm->tm_mday = tm->tm_mon;
							tmask = DTK_M(DAY);
						}
						haveTextMonth = true;
						tm->tm_mon = val;
						break;

					case DTZMOD:

						/*
						 * daylight savings time modifier (solves "MET DST"
						 * syntax)
						 */
						tmask |= DTK_M(DTZ);
						tm->tm_isdst = 1;
						if (tzp == NULL)
							return DTERR_BAD_FORMAT;
						*tzp -= val;
						break;

					case DTZ:

						/*
						 * set mask for TZ here _or_ check for DTZ later when
						 * getting default timezone
						 */
						tmask |= DTK_M(TZ);
						tm->tm_isdst = 1;
						if (tzp == NULL)
							return DTERR_BAD_FORMAT;
						*tzp = -val;
						break;

					case TZ:
						tm->tm_isdst = 0;
						if (tzp == NULL)
							return DTERR_BAD_FORMAT;
						*tzp = -val;
						break;

					case DYNTZ:
						tmask |= DTK_M(TZ);
						if (tzp == NULL)
							return DTERR_BAD_FORMAT;
						/* we'll determine the actual offset later */
						abbrevTz = valtz;
						abbrev = field[i];
						break;

					case AMPM:
						mer = val;
						break;

					case ADBC:
						bc = (val == BC);
						break;

					case DOW:
						tm->tm_wday = val;
						break;

					case UNITS:
						tmask = 0;
						ptype = val;
						break;

					case ISOTIME:

						/*
						 * This is a filler field "t" indicating that the next
						 * field is time. Try to verify that this is sensible.
						 */
						tmask = 0;

						/* No preceding date? Then quit... */
						if ((fmask & DTK_DATE_M) != DTK_DATE_M)
							return DTERR_BAD_FORMAT;

						/***
						 * We will need one of the following fields:
						 *	DTK_NUMBER should be hhmmss.fff
						 *	DTK_TIME should be hh:mm:ss.fff
						 *	DTK_DATE should be hhmmss-zz
						 ***/
						if (i >= nf - 1 ||
							(ftype[i + 1] != DTK_NUMBER &&
							 ftype[i + 1] != DTK_TIME &&
							 ftype[i + 1] != DTK_DATE))
							return DTERR_BAD_FORMAT;

						ptype = val;
						break;

					case UNKNOWN_FIELD:

						/*
						 * Before giving up and declaring error, check to see
						 * if it is an all-alpha timezone name.
						 */
						namedTz = pg_tzset(field[i]);
						if (!namedTz)
							return DTERR_BAD_FORMAT;
						/* we'll apply the zone setting below */
						tmask = DTK_M(TZ);
						break;

					default:
						return DTERR_BAD_FORMAT;
				}
				break;

			default:
				return DTERR_BAD_FORMAT;
		}

		if (tmask & fmask)
			return DTERR_BAD_FORMAT;
		fmask |= tmask;
	}							/* end loop over fields */

	/* do final checking/adjustment of Y/M/D fields */
	dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
	if (dterr)
		return dterr;

	/* handle AM/PM */
	if (mer != HR24 && tm->tm_hour > HOURS_PER_DAY / 2)
		return DTERR_FIELD_OVERFLOW;
	if (mer == AM && tm->tm_hour == HOURS_PER_DAY / 2)
		tm->tm_hour = 0;
	else if (mer == PM && tm->tm_hour != HOURS_PER_DAY / 2)
		tm->tm_hour += HOURS_PER_DAY / 2;

	/* do additional checking for full date specs... */
	if (*dtype == DTK_DATE)
	{
		if ((fmask & DTK_DATE_M) != DTK_DATE_M)
		{
			if ((fmask & DTK_TIME_M) == DTK_TIME_M)
				return 1;
			return DTERR_BAD_FORMAT;
		}

		/*
		 * If we had a full timezone spec, compute the offset (we could not do
		 * it before, because we need the date to resolve DST status).
		 */
		if (namedTz != NULL)
		{
			/* daylight savings time modifier disallowed with full TZ */
			if (fmask & DTK_M(DTZMOD))
				return DTERR_BAD_FORMAT;

			*tzp = DetermineTimeZoneOffset(tm, namedTz);
		}

		/*
		 * Likewise, if we had a dynamic timezone abbreviation, resolve it
		 * now.
		 */
		/* SPANNER_PG: timezone abbreviations are not supported */
		// if (abbrevTz != NULL)
		// {
		// 	/* daylight savings time modifier disallowed with dynamic TZ */
		// 	if (fmask & DTK_M(DTZMOD))
		// 		return DTERR_BAD_FORMAT;

		// 	*tzp = DetermineTimeZoneAbbrevOffset(tm, abbrev, abbrevTz);
		// }

		/* timezone not specified? then use session timezone */
		if (tzp != NULL && !(fmask & DTK_M(TZ)))
		{
			/*
			 * daylight savings time modifier but no standard timezone? then
			 * error
			 */
			if (fmask & DTK_M(DTZMOD))
				return DTERR_BAD_FORMAT;

			pg_tz* loaded_default_timezone = pg_tzset(default_timezone);
			if (!loaded_default_timezone) {
				*error = absl::StrFormat("default timezone \"%s\" not recognized",
							default_timezone);
				return DTERR_BAD_FORMAT;
			}
			*tzp = DetermineTimeZoneOffset(tm, loaded_default_timezone);
		}
	}

	return 0;
}

/* DetermineTimeZoneOffset()
 *
 * Given a struct pg_tm in which tm_year, tm_mon, tm_mday, tm_hour, tm_min,
 * and tm_sec fields are set, and a zic-style time zone definition, determine
 * the applicable GMT offset and daylight-savings status at that time.
 * Set the struct pg_tm's tm_isdst field accordingly, and return the GMT
 * offset as the function result.
 *
 * Note: if the date is out of the range we can deal with, we return zero
 * as the GMT offset and set tm_isdst = 0.  We don't throw an error here,
 * though probably some higher-level code will.
 */
int
DetermineTimeZoneOffset(struct pg_tm *tm, pg_tz *tzp)
{
	pg_time_t	t;

	return DetermineTimeZoneOffsetInternal(tm, tzp, &t);
}


/* DetermineTimeZoneOffsetInternal()
 *
 * As above, but also return the actual UTC time imputed to the date/time
 * into *tp.
 *
 * In event of an out-of-range date, we punt by returning zero into *tp.
 * This is okay for the immediate callers but is a good reason for not
 * exposing this worker function globally.
 *
 * Note: it might seem that we should use mktime() for this, but bitter
 * experience teaches otherwise.  This code is much faster than most versions
 * of mktime(), anyway.
 */
static int
DetermineTimeZoneOffsetInternal(struct pg_tm *tm, pg_tz *tzp, pg_time_t *tp)
{
	int			date,
				sec;
	pg_time_t	day,
				mytime,
				prevtime,
				boundary,
				beforetime,
				aftertime;
	long int	before_gmtoff,
				after_gmtoff;
	int			before_isdst,
				after_isdst;
	int			res;

	/*
	 * First, generate the pg_time_t value corresponding to the given
	 * y/m/d/h/m/s taken as GMT time.  If this overflows, punt and decide the
	 * timezone is GMT.  (For a valid Julian date, integer overflow should be
	 * impossible with 64-bit pg_time_t, but let's check for safety.)
	 */
	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
		goto overflow;
	date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - UNIX_EPOCH_JDATE;

	day = ((pg_time_t) date) * SECS_PER_DAY;
	if (day / SECS_PER_DAY != date)
		goto overflow;
	sec = tm->tm_sec + (tm->tm_min + tm->tm_hour * MINS_PER_HOUR) * SECS_PER_MINUTE;
	mytime = day + sec;
	/* since sec >= 0, overflow could only be from +day to -mytime */
	if (mytime < 0 && day > 0)
		goto overflow;

	/*
	 * Find the DST time boundary just before or following the target time. We
	 * assume that all zones have GMT offsets less than 24 hours, and that DST
	 * boundaries can't be closer together than 48 hours, so backing up 24
	 * hours and finding the "next" boundary will work.
	 */
	prevtime = mytime - SECS_PER_DAY;
	if (mytime < 0 && prevtime > 0)
		goto overflow;

	res = pg_next_dst_boundary(&prevtime,
							   &before_gmtoff, &before_isdst,
							   &boundary,
							   &after_gmtoff, &after_isdst,
							   tzp);
	if (res < 0)
		goto overflow;			/* failure? */

	if (res == 0)
	{
		/* Non-DST zone, life is simple */
		tm->tm_isdst = before_isdst;
		*tp = mytime - before_gmtoff;
		return -(int) before_gmtoff;
	}

	/*
	 * Form the candidate pg_time_t values with local-time adjustment
	 * SPANNER_PG: these overflow checks never pass with 64-bit pg_time_t.
	 */
	beforetime = mytime - before_gmtoff;
	if ((before_gmtoff > 0 &&
		 mytime < 0 && beforetime > 0) ||
		(before_gmtoff <= 0 &&
		 mytime > 0 && beforetime < 0))
		goto overflow;
	aftertime = mytime - after_gmtoff;
	if ((after_gmtoff > 0 &&
		 mytime < 0 && aftertime > 0) ||
		(after_gmtoff <= 0 &&
		 mytime > 0 && aftertime < 0))
		goto overflow;

	/*
	 * If both before or both after the boundary time, we know what to do. The
	 * boundary time itself is considered to be after the transition, which
	 * means we can accept aftertime == boundary in the second case.
	 */
	if (beforetime < boundary && aftertime < boundary)
	{
		tm->tm_isdst = before_isdst;
		*tp = beforetime;
		return -(int) before_gmtoff;
	}
	if (beforetime > boundary && aftertime >= boundary)
	{
		tm->tm_isdst = after_isdst;
		*tp = aftertime;
		return -(int) after_gmtoff;
	}

	/*
	 * It's an invalid or ambiguous time due to timezone transition.  In a
	 * spring-forward transition, prefer the "before" interpretation; in a
	 * fall-back transition, prefer "after".  (We used to define and implement
	 * this test as "prefer the standard-time interpretation", but that rule
	 * does not help to resolve the behavior when both times are reported as
	 * standard time; which does happen, eg Europe/Moscow in Oct 2014.  Also,
	 * in some zones such as Europe/Dublin, there is widespread confusion
	 * about which time offset is "standard" time, so it's fortunate that our
	 * behavior doesn't depend on that.)
	 */
	if (beforetime > aftertime)
	{
		tm->tm_isdst = before_isdst;
		*tp = beforetime;
		return -(int) before_gmtoff;
	}
	tm->tm_isdst = after_isdst;
	*tp = aftertime;
	return -(int) after_gmtoff;

overflow:
	/* Given date is out of range, so assume UTC */
	tm->tm_isdst = 0;
	*tp = 0;
	return 0;
}

/* DecodeDate()
 * Decode date string which includes delimiters.
 * Return 0 if okay, a DTERR code if not.
 *
 *	str: field to be parsed
 *	fmask: bitmask for field types already seen
 *	*tmask: receives bitmask for fields found here
 *	*is2digits: set to true if we find 2-digit year
 *	*tm: field values are stored into appropriate members of this struct
 */
static int
DecodeDate(char *str, int fmask, int *tmask, bool *is2digits,
		   struct pg_tm *tm)
{
	fsec_t		fsec;
	int			nf = 0;
	int			i,
				len;
	int			dterr;
	bool		haveTextMonth = false;
	int			type,
				val,
				dmask = 0;
	char	   *field[MAXDATEFIELDS];

	*tmask = 0;

	/* parse this string... */
	while (*str != '\0' && nf < MAXDATEFIELDS)
	{
		/* skip field separators */
		while (*str != '\0' && !isalnum((unsigned char) *str))
			str++;

		if (*str == '\0')
			return DTERR_BAD_FORMAT;	/* end of string after separator */

		field[nf] = str;
		if (isdigit((unsigned char) *str))
		{
			while (isdigit((unsigned char) *str))
				str++;
		}
		else if (isalpha((unsigned char) *str))
		{
			while (isalpha((unsigned char) *str))
				str++;
		}

		/* Just get rid of any non-digit, non-alpha characters... */
		if (*str != '\0')
			*str++ = '\0';
		nf++;
	}

	/* look first for text fields, since that will be unambiguous month */
	for (i = 0; i < nf; i++)
	{
		if (isalpha((unsigned char) *field[i]))
		{
			type = DecodeSpecial(i, field[i], &val);
			if (type == IGNORE_DTF)
				continue;

			dmask = DTK_M(type);
			switch (type)
			{
				case MONTH:
					tm->tm_mon = val;
					haveTextMonth = true;
					break;

				default:
					return DTERR_BAD_FORMAT;
			}
			if (fmask & dmask)
				return DTERR_BAD_FORMAT;

			fmask |= dmask;
			*tmask |= dmask;

			/* mark this field as being completed */
			field[i] = NULL;
		}
	}

	/* now pick up remaining numeric fields */
	for (i = 0; i < nf; i++)
	{
		if (field[i] == NULL)
			continue;

		if ((len = strlen(field[i])) <= 0)
			return DTERR_BAD_FORMAT;

		dterr = DecodeNumber(len, field[i], haveTextMonth, fmask,
							 &dmask, tm,
							 &fsec, is2digits);
		if (dterr)
			return dterr;

		if (fmask & dmask)
			return DTERR_BAD_FORMAT;

		fmask |= dmask;
		*tmask |= dmask;
	}

	if ((fmask & ~(DTK_M(DOY) | DTK_M(TZ))) != DTK_DATE_M)
		return DTERR_BAD_FORMAT;

	/* validation of the field values must wait until ValidateDate() */

	return 0;
}

/* ValidateDate()
 * Check valid year/month/day values, handle BC and DOY cases
 * Return 0 if okay, a DTERR code if not.
 */
int
ValidateDate(int fmask, bool isjulian, bool is2digits, bool bc,
			 struct pg_tm *tm)
{
	if (fmask & DTK_M(YEAR))
	{
		if (isjulian)
		{
			/* tm_year is correct and should not be touched */
		}
		else if (bc)
		{
			/* there is no year zero in AD/BC notation */
			if (tm->tm_year <= 0)
				return DTERR_FIELD_OVERFLOW;
			/* internally, we represent 1 BC as year zero, 2 BC as -1, etc */
			tm->tm_year = -(tm->tm_year - 1);
		}
		else if (is2digits)
		{
			/* process 1 or 2-digit input as 1970-2069 AD, allow '0' and '00' */
			if (tm->tm_year < 0)	/* just paranoia */
				return DTERR_FIELD_OVERFLOW;
			if (tm->tm_year < 70)
				tm->tm_year += 2000;
			else if (tm->tm_year < 100)
				tm->tm_year += 1900;
		}
		else
		{
			/* there is no year zero in AD/BC notation */
			if (tm->tm_year <= 0)
				return DTERR_FIELD_OVERFLOW;
		}
	}

	/* now that we have correct year, decode DOY */
	if (fmask & DTK_M(DOY))
	{
		j2date(date2j(tm->tm_year, 1, 1) + tm->tm_yday - 1,
			   &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
	}

	/* check for valid month */
	if (fmask & DTK_M(MONTH))
	{
		if (tm->tm_mon < 1 || tm->tm_mon > MONTHS_PER_YEAR)
			return DTERR_MD_FIELD_OVERFLOW;
	}

	/* minimal check for valid day */
	if (fmask & DTK_M(DAY))
	{
		if (tm->tm_mday < 1 || tm->tm_mday > 31)
			return DTERR_MD_FIELD_OVERFLOW;
	}

	if ((fmask & DTK_DATE_M) == DTK_DATE_M)
	{
		/*
		 * Check for valid day of month, now that we know for sure the month
		 * and year.  Note we don't use MD_FIELD_OVERFLOW here, since it seems
		 * unlikely that "Feb 29" is a YMD-order error.
		 */
		if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
			return DTERR_FIELD_OVERFLOW;
	}

	return 0;
}


/* DecodeTime()
 * Decode time string which includes delimiters.
 * Return 0 if okay, a DTERR code if not.
 *
 * Only check the lower limit on hours, since this same code can be
 * used to represent time spans.
 */
static int
DecodeTime(char *str, int fmask, int range,
		   int *tmask, struct pg_tm *tm, fsec_t *fsec)
{
	char	   *cp;
	int			dterr;

	*tmask = DTK_TIME_M;

	errno = 0;
	tm->tm_hour = strtoint(str, &cp, 10);
	if (errno == ERANGE)
		return DTERR_FIELD_OVERFLOW;
	if (*cp != ':')
		return DTERR_BAD_FORMAT;
	errno = 0;
	tm->tm_min = strtoint(cp + 1, &cp, 10);
	if (errno == ERANGE)
		return DTERR_FIELD_OVERFLOW;
	if (*cp == '\0')
	{
		tm->tm_sec = 0;
		*fsec = 0;
		/* If it's a MINUTE TO SECOND interval, take 2 fields as being mm:ss */
		if (range == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)))
		{
			tm->tm_sec = tm->tm_min;
			tm->tm_min = tm->tm_hour;
			tm->tm_hour = 0;
		}
	}
	else if (*cp == '.')
	{
		/* always assume mm:ss.sss is MINUTE TO SECOND */
		dterr = ParseFractionalSecond(cp, fsec);
		if (dterr)
			return dterr;
		tm->tm_sec = tm->tm_min;
		tm->tm_min = tm->tm_hour;
		tm->tm_hour = 0;
	}
	else if (*cp == ':')
	{
		errno = 0;
		tm->tm_sec = strtoint(cp + 1, &cp, 10);
		if (errno == ERANGE)
			return DTERR_FIELD_OVERFLOW;
		if (*cp == '\0')
			*fsec = 0;
		else if (*cp == '.')
		{
			dterr = ParseFractionalSecond(cp, fsec);
			if (dterr)
				return dterr;
		}
		else
			return DTERR_BAD_FORMAT;
	}
	else
		return DTERR_BAD_FORMAT;

	/* do a validity check */
	if (tm->tm_hour < 0 || tm->tm_min < 0 || tm->tm_min > MINS_PER_HOUR - 1 ||
		tm->tm_sec < 0 || tm->tm_sec > SECS_PER_MINUTE ||
		*fsec < INT64CONST(0) ||
		*fsec > USECS_PER_SEC)
		return DTERR_FIELD_OVERFLOW;

	return 0;
}


/* DecodeNumber()
 * Interpret plain numeric field as a date value in context.
 * Return 0 if okay, a DTERR code if not.
 */
static int
DecodeNumber(int flen, char *str, bool haveTextMonth, int fmask,
			 int *tmask, struct pg_tm *tm, fsec_t *fsec, bool *is2digits)
{
	int			val;
	char	   *cp;
	int			dterr;

	*tmask = 0;

	errno = 0;
	val = strtoint(str, &cp, 10);
	if (errno == ERANGE)
		return DTERR_FIELD_OVERFLOW;
	if (cp == str)
		return DTERR_BAD_FORMAT;

	if (*cp == '.')
	{
		/*
		 * More than two digits before decimal point? Then could be a date or
		 * a run-together time: 2001.360 20011225 040506.789
		 */
		if (cp - str > 2)
		{
			dterr = DecodeNumberField(flen, str,
									  (fmask | DTK_DATE_M),
									  tmask, tm,
									  fsec, is2digits);
			if (dterr < 0)
				return dterr;
			return 0;
		}

		dterr = ParseFractionalSecond(cp, fsec);
		if (dterr)
			return dterr;
	}
	else if (*cp != '\0')
		return DTERR_BAD_FORMAT;

	/* Special case for day of year */
	if (flen == 3 && (fmask & DTK_DATE_M) == DTK_M(YEAR) && val >= 1 &&
		val <= 366)
	{
		*tmask = (DTK_M(DOY) | DTK_M(MONTH) | DTK_M(DAY));
		tm->tm_yday = val;
		/* tm_mon and tm_mday can't actually be set yet ... */
		return 0;
	}

	/* Switch based on what we have so far */
	switch (fmask & DTK_DATE_M)
	{
		case 0:

			/*
			 * Nothing so far; make a decision about what we think the input
			 * is.  There used to be lots of heuristics here, but the
			 * consensus now is to be paranoid.  It *must* be either
			 * YYYY-MM-DD (with a more-than-two-digit year field), or the
			 * field order defined by DateOrder.
			 */
			if (flen >= 3 || DateOrder == DATEORDER_YMD)
			{
				*tmask = DTK_M(YEAR);
				tm->tm_year = val;
			}
			else if (DateOrder == DATEORDER_DMY)
			{
				*tmask = DTK_M(DAY);
				tm->tm_mday = val;
			}
			else
			{
				*tmask = DTK_M(MONTH);
				tm->tm_mon = val;
			}
			break;

		case (DTK_M(YEAR)):
			/* Must be at second field of YY-MM-DD */
			*tmask = DTK_M(MONTH);
			tm->tm_mon = val;
			break;

		case (DTK_M(MONTH)):
			if (haveTextMonth)
			{
				/*
				 * We are at the first numeric field of a date that included a
				 * textual month name.  We want to support the variants
				 * MON-DD-YYYY, DD-MON-YYYY, and YYYY-MON-DD as unambiguous
				 * inputs.  We will also accept MON-DD-YY or DD-MON-YY in
				 * either DMY or MDY modes, as well as YY-MON-DD in YMD mode.
				 */
				if (flen >= 3 || DateOrder == DATEORDER_YMD)
				{
					*tmask = DTK_M(YEAR);
					tm->tm_year = val;
				}
				else
				{
					*tmask = DTK_M(DAY);
					tm->tm_mday = val;
				}
			}
			else
			{
				/* Must be at second field of MM-DD-YY */
				*tmask = DTK_M(DAY);
				tm->tm_mday = val;
			}
			break;

		case (DTK_M(YEAR) | DTK_M(MONTH)):
			if (haveTextMonth)
			{
				/* Need to accept DD-MON-YYYY even in YMD mode */
				if (flen >= 3 && *is2digits)
				{
					/* Guess that first numeric field is day was wrong */
					*tmask = DTK_M(DAY);	/* YEAR is already set */
					tm->tm_mday = tm->tm_year;
					tm->tm_year = val;
					*is2digits = false;
				}
				else
				{
					*tmask = DTK_M(DAY);
					tm->tm_mday = val;
				}
			}
			else
			{
				/* Must be at third field of YY-MM-DD */
				*tmask = DTK_M(DAY);
				tm->tm_mday = val;
			}
			break;

		case (DTK_M(DAY)):
			/* Must be at second field of DD-MM-YY */
			*tmask = DTK_M(MONTH);
			tm->tm_mon = val;
			break;

		case (DTK_M(MONTH) | DTK_M(DAY)):
			/* Must be at third field of DD-MM-YY or MM-DD-YY */
			*tmask = DTK_M(YEAR);
			tm->tm_year = val;
			break;

		case (DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY)):
			/* we have all the date, so it must be a time field */
			dterr = DecodeNumberField(flen, str, fmask,
									  tmask, tm,
									  fsec, is2digits);
			if (dterr < 0)
				return dterr;
			return 0;

		default:
			/* Anything else is bogus input */
			return DTERR_BAD_FORMAT;
	}

	/*
	 * When processing a year field, mark it for adjustment if it's only one
	 * or two digits.
	 */
	if (*tmask == DTK_M(YEAR))
		*is2digits = (flen <= 2);

	return 0;
}


/* DecodeNumberField()
 * Interpret numeric string as a concatenated date or time field.
 * Return a DTK token (>= 0) if successful, a DTERR code (< 0) if not.
 *
 * Use the context of previously decoded fields to help with
 * the interpretation.
 */
static int
DecodeNumberField(int len, char *str, int fmask,
				  int *tmask, struct pg_tm *tm, fsec_t *fsec, bool *is2digits)
{
	char	   *cp;

	/*
	 * Have a decimal point? Then this is a date or something with a seconds
	 * field...
	 */
	if ((cp = strchr(str, '.')) != NULL)
	{
		/*
		 * Can we use ParseFractionalSecond here?  Not clear whether trailing
		 * junk should be rejected ...
		 */
		double		frac;

		errno = 0;
		frac = strtod(cp, NULL);
		if (errno != 0)
			return DTERR_BAD_FORMAT;
		*fsec = rint(frac * 1000000);
		/* Now truncate off the fraction for further processing */
		*cp = '\0';
		len = strlen(str);
	}
	/* No decimal point and no complete date yet? */
	else if ((fmask & DTK_DATE_M) != DTK_DATE_M)
	{
		if (len >= 6)
		{
			*tmask = DTK_DATE_M;

			/*
			 * Start from end and consider first 2 as Day, next 2 as Month,
			 * and the rest as Year.
			 */
			tm->tm_mday = atoi(str + (len - 2));
			*(str + (len - 2)) = '\0';
			tm->tm_mon = atoi(str + (len - 4));
			*(str + (len - 4)) = '\0';
			tm->tm_year = atoi(str);
			if ((len - 4) == 2)
				*is2digits = true;

			return DTK_DATE;
		}
	}

	/* not all time fields are specified? */
	if ((fmask & DTK_TIME_M) != DTK_TIME_M)
	{
		/* hhmmss */
		if (len == 6)
		{
			*tmask = DTK_TIME_M;
			tm->tm_sec = atoi(str + 4);
			*(str + 4) = '\0';
			tm->tm_min = atoi(str + 2);
			*(str + 2) = '\0';
			tm->tm_hour = atoi(str);

			return DTK_TIME;
		}
		/* hhmm? */
		else if (len == 4)
		{
			*tmask = DTK_TIME_M;
			tm->tm_sec = 0;
			tm->tm_min = atoi(str + 2);
			*(str + 2) = '\0';
			tm->tm_hour = atoi(str);

			return DTK_TIME;
		}
	}

	return DTERR_BAD_FORMAT;
}


/* DecodeTimezone()
 * Interpret string as a numeric timezone.
 *
 * Return 0 if okay (and set *tzp), a DTERR code if not okay.
 */
int
DecodeTimezone(char *str, int *tzp)
{
	int			tz;
	int			hr,
				min,
				sec = 0;
	char	   *cp;

	/* leading character must be "+" or "-" */
	if (*str != '+' && *str != '-')
		return DTERR_BAD_FORMAT;

	errno = 0;
	hr = strtoint(str + 1, &cp, 10);
	if (errno == ERANGE)
		return DTERR_TZDISP_OVERFLOW;

	/* explicit delimiter? */
	if (*cp == ':')
	{
		errno = 0;
		min = strtoint(cp + 1, &cp, 10);
		if (errno == ERANGE)
			return DTERR_TZDISP_OVERFLOW;
		if (*cp == ':')
		{
			errno = 0;
			sec = strtoint(cp + 1, &cp, 10);
			if (errno == ERANGE)
				return DTERR_TZDISP_OVERFLOW;
		}
	}
	/* otherwise, might have run things together... */
	else if (*cp == '\0' && strlen(str) > 3)
	{
		min = hr % 100;
		hr = hr / 100;
		/* we could, but don't, support a run-together hhmmss format */
	}
	else
		min = 0;

	/* Range-check the values; see notes in datatype/timestamp.h */
	if (hr < 0 || hr > MAX_TZDISP_HOUR)
		return DTERR_TZDISP_OVERFLOW;
	if (min < 0 || min >= MINS_PER_HOUR)
		return DTERR_TZDISP_OVERFLOW;
	if (sec < 0 || sec >= SECS_PER_MINUTE)
		return DTERR_TZDISP_OVERFLOW;

	tz = (hr * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec;
	if (*str == '-')
		tz = -tz;

	*tzp = -tz;

	if (*cp != '\0')
		return DTERR_BAD_FORMAT;

	return 0;
}


/* DecodeTimezoneAbbrev()
 * Interpret string as a timezone abbreviation, if possible.
 *
 * Returns an abbreviation type (TZ, DTZ, or DYNTZ), or UNKNOWN_FIELD if
 * string is not any known abbreviation.  On success, set *offset and *tz to
 * represent the UTC offset (for TZ or DTZ) or underlying zone (for DYNTZ).
 * Note that full timezone names (such as America/New_York) are not handled
 * here, mostly for historical reasons.
 *
 * Given string must be lowercased already.
 *
 * Implement a cache lookup since it is likely that dates
 *	will be related in format.
 */
int
DecodeTimezoneAbbrev(int field, char *lowtoken,
					 int *offset, pg_tz **tz)
{
	int			type;
	const datetkn *tp;

	tp = abbrevcache[field];
	/* use strncmp so that we match truncated tokens */
	/*
	 * SPANNER_PG: use the hardcoded TimezoneAbbrevTable instead of a zoneabbrevtbl
	 * loaded from a file
	 */
	if (tp == NULL || strncmp(lowtoken, tp->token, TOKMAXLEN) != 0)
	{
		tp = datebsearch(lowtoken, TimezoneAbbrevTable, TimezoneAbbrevTableSize);
	}
	if (tp == NULL)
	{
		type = UNKNOWN_FIELD;
		*offset = 0;
		*tz = NULL;
	}
	else
	{
		abbrevcache[field] = tp;
		type = tp->type;
		/* SPANNER_PG: dynamic timezone abbreviations are not supported*/
		assert(type != DYNTZ);
		*offset = tp->value;
		*tz = NULL;
	}

	return type;
}

/* DecodeSpecial()
 * Decode text string using lookup table.
 *
 * Recognizes the keywords listed in datetktbl.
 * Note: at one time this would also recognize timezone abbreviations,
 * but no more; use DecodeTimezoneAbbrev for that.
 *
 * Given string must be lowercased already.
 *
 * Implement a cache lookup since it is likely that dates
 *	will be related in format.
 */
int
DecodeSpecial(int field, char *lowtoken, int *val)
{
	int			type;
	const datetkn *tp;

	tp = datecache[field];
	/* use strncmp so that we match truncated tokens */
	if (tp == NULL || strncmp(lowtoken, tp->token, TOKMAXLEN) != 0)
	{
		tp = datebsearch(lowtoken, datetktbl, szdatetktbl);
	}
	if (tp == NULL)
	{
		type = UNKNOWN_FIELD;
		*val = 0;
	}
	else
	{
		datecache[field] = tp;
		type = tp->type;
		*val = tp->value;
	}

	return type;
}


/*
 * Report an error detected by one of the datetime input processing routines.
 *
 * dterr is the error code, str is the original input string, datatype is
 * the name of the datatype we were trying to accept.
 *
 * Note: it might seem useless to distinguish DTERR_INTERVAL_OVERFLOW and
 * DTERR_TZDISP_OVERFLOW from DTERR_FIELD_OVERFLOW, but SQL99 mandates three
 * separate SQLSTATE codes, so ...
 * SPANNER_PG: return an absl::Status instead of using ereport
 */
absl::Status
DateTimeParseError(int dterr, absl::string_view str, const char *datatype)
{
	switch (dterr)
	{
		case DTERR_FIELD_OVERFLOW:
		case DTERR_MD_FIELD_OVERFLOW:
		  return absl::InvalidArgumentError(
				      absl::StrFormat("date/time field value out of range: \"%s\"",
							str));
			break;
		// SPANNER_PG: remove the hint for datestyle settings
		// TODO: support datestyle settings
		// case DTERR_MD_FIELD_OVERFLOW:
		// 	/* <nanny>same as above, but add hint about DateStyle</nanny> */
		//   return absl::InvalidArgumentError(
		//       absl::StrFormat("date/time field value out of range: \"%s\". Perhaps you need a different \"datestyle\" setting.",
		// 		      str));
		// 	break;
		case DTERR_INTERVAL_OVERFLOW:
		  return absl::InvalidArgumentError(
		      absl::StrFormat("interval field value out of range: \"%s\"",
							str));
			break;
		case DTERR_TZDISP_OVERFLOW:
		  return absl::InvalidArgumentError(
		      absl::StrFormat("time zone displacement out of range: \"%s\"",
							str));
			break;
		case DTERR_BAD_FORMAT:
		default:
		  return absl::InvalidArgumentError(
		      absl::StrFormat("invalid input syntax for type %s: \"%s\"",
							datatype, str));
			break;
	}
}

/* datebsearch()
 * Binary search -- from Knuth (6.2.1) Algorithm B.  Special case like this
 * is WAY faster than the generic bsearch().
 */
static const datetkn *
datebsearch(const char *key, const datetkn *base, int nel)
{
	if (nel > 0)
	{
		const datetkn *last = base + nel - 1,
				   *position;
		int			result;

		while (last >= base)
		{
			position = base + ((last - base) >> 1);
			/* precheck the first character for a bit of extra speed */
			result = (int) key[0] - (int) position->token[0];
			if (result == 0)
			{
				/* use strncmp so that we match truncated tokens */
				result = strncmp(key, position->token, TOKMAXLEN);
				if (result == 0)
					return position;
			}
			if (result < 0)
				last = position - 1;
			else
				base = position + 1;
		}
	}
	return NULL;
}

/* ClearPgTm
 *
 * Zero out a pg_tm and associated fsec_t
 */
static inline void
ClearPgTm(struct pg_tm *tm, fsec_t *fsec)
{
	tm->tm_year = 0;
	tm->tm_mon = 0;
	tm->tm_mday = 0;
	tm->tm_hour = 0;
	tm->tm_min = 0;
	tm->tm_sec = 0;
	*fsec = 0;
}

/*
 * Multiply frac by scale (to produce seconds) and add to *tm & *fsec.
 * We assume the input frac is less than 1 so overflow is not an issue.
 */
static void
AdjustFractSeconds(double frac, struct pg_tm *tm, fsec_t *fsec, int scale)
{
	int			sec;

	if (frac == 0)
		return;
	frac *= scale;
	sec = (int) frac;
	tm->tm_sec += sec;
	frac -= sec;
	*fsec += rint(frac * 1000000);
}

/* As above, but initial scale produces days */
static void
AdjustFractDays(double frac, struct pg_tm *tm, fsec_t *fsec, int scale)
{
	int			extra_days;

	if (frac == 0)
		return;
	frac *= scale;
	extra_days = (int) frac;
	tm->tm_mday += extra_days;
	frac -= extra_days;
	AdjustFractSeconds(frac, tm, fsec, SECS_PER_DAY);
}

/* DecodeUnits()
 * Decode text string using lookup table.
 *
 * This routine recognizes keywords associated with time interval units.
 *
 * Given string must be lowercased already.
 *
 * Implement a cache lookup since it is likely that dates
 *	will be related in format.
 */
int
DecodeUnits(int field, char *lowtoken, int *val)
{
	int			type;
	const datetkn *tp;

	tp = deltacache[field];
	/* use strncmp so that we match truncated tokens */
	if (tp == NULL || strncmp(lowtoken, tp->token, TOKMAXLEN) != 0)
	{
		tp = datebsearch(lowtoken, deltatktbl, szdeltatktbl);
	}
	if (tp == NULL)
	{
		type = UNKNOWN_FIELD;
		*val = 0;
	}
	else
	{
		deltacache[field] = tp;
		type = tp->type;
		*val = tp->value;
	}

	return type;
}								/* DecodeUnits() */

/* DecodeInterval()
 * Interpret previously parsed fields for general time interval.
 * Returns 0 if successful, DTERR code if bogus input detected.
 * dtype, tm, fsec are output parameters.
 *
 * Allow "date" field DTK_DATE since this could be just
 *	an unsigned floating point number. - thomas 1997-11-16
 *
 * Allow ISO-style time span, with implicit units on number of days
 *	preceding an hh:mm:ss field. - thomas 1998-04-30
 */
int
DecodeInterval(char **field, int *ftype, int nf, int range,
			   int *dtype, struct pg_tm *tm, fsec_t *fsec)
{
	bool		is_before = false;
	char	   *cp;
	int			fmask = 0,
				tmask,
				type;
	int			i;
	int			dterr;
	int			val;
	double		fval;

	*dtype = DTK_DELTA;
	type = IGNORE_DTF;
	ClearPgTm(tm, fsec);

	/* read through list backwards to pick up units before values */
	for (i = nf - 1; i >= 0; i--)
	{
		switch (ftype[i])
		{
			case DTK_TIME:
				dterr = DecodeTime(field[i], fmask, range,
								   &tmask, tm, fsec);
				if (dterr)
					return dterr;
				type = DTK_DAY;
				break;

			case DTK_TZ:

				/*
				 * Timezone means a token with a leading sign character and at
				 * least one digit; there could be ':', '.', '-' embedded in
				 * it as well.
				 */
				assert(*field[i] == '-' || *field[i] == '+');

				/*
				 * Check for signed hh:mm or hh:mm:ss.  If so, process exactly
				 * like DTK_TIME case above, plus handling the sign.
				 */
				if (strchr(field[i] + 1, ':') != NULL &&
					DecodeTime(field[i] + 1, fmask, range,
							   &tmask, tm, fsec) == 0)
				{
					if (*field[i] == '-')
					{
						/* flip the sign on all fields */
						tm->tm_hour = -tm->tm_hour;
						tm->tm_min = -tm->tm_min;
						tm->tm_sec = -tm->tm_sec;
						*fsec = -(*fsec);
					}

					/*
					 * Set the next type to be a day, if units are not
					 * specified. This handles the case of '1 +02:03' since we
					 * are reading right to left.
					 */
					type = DTK_DAY;
					break;
				}

				/*
				 * Otherwise, fall through to DTK_NUMBER case, which can
				 * handle signed float numbers and signed year-month values.
				 */

				/* FALLTHROUGH */
				ABSL_FALLTHROUGH_INTENDED;
			case DTK_DATE:
			case DTK_NUMBER:
				if (type == IGNORE_DTF)
				{
					/* use typmod to decide what rightmost field is */
					switch (range)
					{
						case INTERVAL_MASK(YEAR):
							type = DTK_YEAR;
							break;
						case INTERVAL_MASK(MONTH):
						case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
							type = DTK_MONTH;
							break;
						case INTERVAL_MASK(DAY):
							type = DTK_DAY;
							break;
						case INTERVAL_MASK(HOUR):
						case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
							type = DTK_HOUR;
							break;
						case INTERVAL_MASK(MINUTE):
						case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
						case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
							type = DTK_MINUTE;
							break;
						case INTERVAL_MASK(SECOND):
						case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
						case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
						case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
							type = DTK_SECOND;
							break;
						default:
							type = DTK_SECOND;
							break;
					}
				}

				errno = 0;
				val = strtoint(field[i], &cp, 10);
				if (errno == ERANGE)
					return DTERR_FIELD_OVERFLOW;

				if (*cp == '-')
				{
					/* SQL "years-months" syntax */
					int			val2;

					val2 = strtoint(cp + 1, &cp, 10);
					if (errno == ERANGE || val2 < 0 || val2 >= MONTHS_PER_YEAR)
						return DTERR_FIELD_OVERFLOW;
					if (*cp != '\0')
						return DTERR_BAD_FORMAT;
					type = DTK_MONTH;
					if (*field[i] == '-')
						val2 = -val2;
					if (((double) val * MONTHS_PER_YEAR + val2) > INT_MAX ||
						((double) val * MONTHS_PER_YEAR + val2) < INT_MIN)
						return DTERR_FIELD_OVERFLOW;
					val = val * MONTHS_PER_YEAR + val2;
					fval = 0;
				}
				else if (*cp == '.')
				{
					errno = 0;
					fval = strtod(cp, &cp);
					if (*cp != '\0' || errno != 0)
						return DTERR_BAD_FORMAT;

					if (*field[i] == '-')
						fval = -fval;
				}
				else if (*cp == '\0')
					fval = 0;
				else
					return DTERR_BAD_FORMAT;

				tmask = 0;		/* DTK_M(type); */

				switch (type)
				{
					case DTK_MICROSEC:
						*fsec += rint(val + fval);
						tmask = DTK_M(MICROSECOND);
						break;

					case DTK_MILLISEC:
						/* avoid overflowing the fsec field */
						tm->tm_sec += val / 1000;
						val -= (val / 1000) * 1000;
						*fsec += rint((val + fval) * 1000);
						tmask = DTK_M(MILLISECOND);
						break;

					case DTK_SECOND:
						tm->tm_sec += val;
						*fsec += rint(fval * 1000000);

						/*
						 * If any subseconds were specified, consider this
						 * microsecond and millisecond input as well.
						 */
						if (fval == 0)
							tmask = DTK_M(SECOND);
						else
							tmask = DTK_ALL_SECS_M;
						break;

					case DTK_MINUTE:
						tm->tm_min += val;
						AdjustFractSeconds(fval, tm, fsec, SECS_PER_MINUTE);
						tmask = DTK_M(MINUTE);
						break;

					case DTK_HOUR:
						tm->tm_hour += val;
						AdjustFractSeconds(fval, tm, fsec, SECS_PER_HOUR);
						tmask = DTK_M(HOUR);
						type = DTK_DAY; /* set for next field */
						break;

					case DTK_DAY:
						tm->tm_mday += val;
						AdjustFractSeconds(fval, tm, fsec, SECS_PER_DAY);
						tmask = DTK_M(DAY);
						break;

					case DTK_WEEK:
						tm->tm_mday += val * 7;
						AdjustFractDays(fval, tm, fsec, 7);
						tmask = DTK_M(WEEK);
						break;

					case DTK_MONTH:
						tm->tm_mon += val;
						AdjustFractDays(fval, tm, fsec, DAYS_PER_MONTH);
						tmask = DTK_M(MONTH);
						break;

					case DTK_YEAR:
						tm->tm_year += val;
						if (fval != 0)
							tm->tm_mon += fval * MONTHS_PER_YEAR;
						tmask = DTK_M(YEAR);
						break;

					case DTK_DECADE:
						tm->tm_year += val * 10;
						if (fval != 0)
							tm->tm_mon += fval * MONTHS_PER_YEAR * 10;
						tmask = DTK_M(DECADE);
						break;

					case DTK_CENTURY:
						tm->tm_year += val * 100;
						if (fval != 0)
							tm->tm_mon += fval * MONTHS_PER_YEAR * 100;
						tmask = DTK_M(CENTURY);
						break;

					case DTK_MILLENNIUM:
						tm->tm_year += val * 1000;
						if (fval != 0)
							tm->tm_mon += fval * MONTHS_PER_YEAR * 1000;
						tmask = DTK_M(MILLENNIUM);
						break;

					default:
						return DTERR_BAD_FORMAT;
				}
				break;

			case DTK_STRING:
			case DTK_SPECIAL:
				type = DecodeUnits(i, field[i], &val);
				if (type == IGNORE_DTF)
					continue;

				tmask = 0;		/* DTK_M(type); */
				switch (type)
				{
					case UNITS:
						type = val;
						break;

					case AGO:
						is_before = true;
						type = val;
						break;

					case RESERV:
						tmask = (DTK_DATE_M | DTK_TIME_M);
						*dtype = val;
						break;

					default:
						return DTERR_BAD_FORMAT;
				}
				break;

			default:
				return DTERR_BAD_FORMAT;
		}

		if (tmask & fmask)
			return DTERR_BAD_FORMAT;
		fmask |= tmask;
	}

	/* ensure that at least one time field has been found */
	if (fmask == 0)
		return DTERR_BAD_FORMAT;

	/* ensure fractional seconds are fractional */
	if (*fsec != 0)
	{
		int			sec;

		sec = *fsec / USECS_PER_SEC;
		*fsec -= sec * USECS_PER_SEC;
		tm->tm_sec += sec;
	}

	/*----------
	 * The SQL standard defines the interval literal
	 *	 '-1 1:00:00'
	 * to mean "negative 1 days and negative 1 hours", while Postgres
	 * traditionally treats this as meaning "negative 1 days and positive
	 * 1 hours".  In SQL_STANDARD intervalstyle, we apply the leading sign
	 * to all fields if there are no other explicit signs.
	 *
	 * We leave the signs alone if there are additional explicit signs.
	 * This protects us against misinterpreting postgres-style dump output,
	 * since the postgres-style output code has always put an explicit sign on
	 * all fields following a negative field.  But note that SQL-spec output
	 * is ambiguous and can be misinterpreted on load!	(So it's best practice
	 * to dump in postgres style, not SQL style.)
	 *----------
	 */
#if 0
	if (IntervalStyle == INTSTYLE_SQL_STANDARD && *field[0] == '-')
	{
		/* Check for additional explicit signs */
		bool		more_signs = false;

		for (i = 1; i < nf; i++)
		{
			if (*field[i] == '-' || *field[i] == '+')
			{
				more_signs = true;
				break;
			}
		}

		if (!more_signs)
		{
			/*
			 * Rather than re-determining which field was field[0], just force
			 * 'em all negative.
			 */
			if (*fsec > 0)
				*fsec = -(*fsec);
			if (tm->tm_sec > 0)
				tm->tm_sec = -tm->tm_sec;
			if (tm->tm_min > 0)
				tm->tm_min = -tm->tm_min;
			if (tm->tm_hour > 0)
				tm->tm_hour = -tm->tm_hour;
			if (tm->tm_mday > 0)
				tm->tm_mday = -tm->tm_mday;
			if (tm->tm_mon > 0)
				tm->tm_mon = -tm->tm_mon;
			if (tm->tm_year > 0)
				tm->tm_year = -tm->tm_year;
		}
	}
#endif  // SQL-standard intervals, which we don't support.

	/* finally, AGO negates everything */
	if (is_before)
	{
		*fsec = -(*fsec);
		tm->tm_sec = -tm->tm_sec;
		tm->tm_min = -tm->tm_min;
		tm->tm_hour = -tm->tm_hour;
		tm->tm_mday = -tm->tm_mday;
		tm->tm_mon = -tm->tm_mon;
		tm->tm_year = -tm->tm_year;
	}

	return 0;
}

/*
 * Helper functions to avoid duplicated code in DecodeISO8601Interval.
 *
 * Parse a decimal value and break it into integer and fractional parts.
 * Returns 0 or DTERR code.
 */
static int
ParseISO8601Number(char *str, char **endptr, int *ipart, double *fpart)
{
	double		val;

	if (!(isdigit((unsigned char) *str) || *str == '-' || *str == '.'))
		return DTERR_BAD_FORMAT;
	errno = 0;
	val = strtod(str, endptr);
	/* did we not see anything that looks like a double? */
	if (*endptr == str || errno != 0)
		return DTERR_BAD_FORMAT;
	/* watch out for overflow */
	if (val < INT_MIN || val > INT_MAX)
		return DTERR_FIELD_OVERFLOW;
	/* be very sure we truncate towards zero (cf dtrunc()) */
	if (val >= 0)
		*ipart = (int) floor(val);
	else
		*ipart = (int) -floor(-val);
	*fpart = val - *ipart;
	return 0;
}

/*
 * Determine number of integral digits in a valid ISO 8601 number field
 * (we should ignore sign and any fraction part)
 */
static int
ISO8601IntegerWidth(char *fieldstart)
{
	/* We might have had a leading '-' */
	if (*fieldstart == '-')
		fieldstart++;
	return strspn(fieldstart, "0123456789");
}


/* DecodeISO8601Interval()
 *	Decode an ISO 8601 time interval of the "format with designators"
 *	(section 4.4.3.2) or "alternative format" (section 4.4.3.3)
 *	Examples:  P1D	for 1 day
 *			   PT1H for 1 hour
 *			   P2Y6M7DT1H30M for 2 years, 6 months, 7 days 1 hour 30 min
 *			   P0002-06-07T01:30:00 the same value in alternative format
 *
 * Returns 0 if successful, DTERR code if bogus input detected.
 * Note: error code should be DTERR_BAD_FORMAT if input doesn't look like
 * ISO8601, otherwise this could cause unexpected error messages.
 * dtype, tm, fsec are output parameters.
 *
 *	A couple exceptions from the spec:
 *	 - a week field ('W') may coexist with other units
 *	 - allows decimals in fields other than the least significant unit.
 */
int
DecodeISO8601Interval(char *str,
					  int *dtype, struct pg_tm *tm, fsec_t *fsec)
{
	bool		datepart = true;
	bool		havefield = false;

	*dtype = DTK_DELTA;
	ClearPgTm(tm, fsec);

	if (strlen(str) < 2 || str[0] != 'P')
		return DTERR_BAD_FORMAT;

	str++;
	while (*str)
	{
		char	   *fieldstart;
		int			val;
		double		fval;
		char		unit;
		int			dterr;

		if (*str == 'T')		/* T indicates the beginning of the time part */
		{
			datepart = false;
			havefield = false;
			str++;
			continue;
		}

		fieldstart = str;
		dterr = ParseISO8601Number(str, &str, &val, &fval);
		if (dterr)
			return dterr;

		/*
		 * Note: we could step off the end of the string here.  Code below
		 * *must* exit the loop if unit == '\0'.
		 */
		unit = *str++;

		if (datepart)
		{
			switch (unit)		/* before T: Y M W D */
			{
				case 'Y':
					tm->tm_year += val;
					tm->tm_mon += (fval * MONTHS_PER_YEAR);
					break;
				case 'M':
					tm->tm_mon += val;
					AdjustFractDays(fval, tm, fsec, DAYS_PER_MONTH);
					break;
				case 'W':
					tm->tm_mday += val * 7;
					AdjustFractDays(fval, tm, fsec, 7);
					break;
				case 'D':
					tm->tm_mday += val;
					AdjustFractSeconds(fval, tm, fsec, SECS_PER_DAY);
					break;
				case 'T':		/* ISO 8601 4.4.3.3 Alternative Format / Basic */
				case '\0':
					if (ISO8601IntegerWidth(fieldstart) == 8 && !havefield)
					{
						tm->tm_year += val / 10000;
						tm->tm_mon += (val / 100) % 100;
						tm->tm_mday += val % 100;
						AdjustFractSeconds(fval, tm, fsec, SECS_PER_DAY);
						if (unit == '\0')
							return 0;
						datepart = false;
						havefield = false;
						continue;
					}
					/* Else fall through to extended alternative format */
					[[fallthrough]];
				case '-':		/* ISO 8601 4.4.3.3 Alternative Format,
								 * Extended */
					if (havefield)
						return DTERR_BAD_FORMAT;

					tm->tm_year += val;
					tm->tm_mon += (fval * MONTHS_PER_YEAR);
					if (unit == '\0')
						return 0;
					if (unit == 'T')
					{
						datepart = false;
						havefield = false;
						continue;
					}

					dterr = ParseISO8601Number(str, &str, &val, &fval);
					if (dterr)
						return dterr;
					tm->tm_mon += val;
					AdjustFractDays(fval, tm, fsec, DAYS_PER_MONTH);
					if (*str == '\0')
						return 0;
					if (*str == 'T')
					{
						datepart = false;
						havefield = false;
						continue;
					}
					if (*str != '-')
						return DTERR_BAD_FORMAT;
					str++;

					dterr = ParseISO8601Number(str, &str, &val, &fval);
					if (dterr)
						return dterr;
					tm->tm_mday += val;
					AdjustFractSeconds(fval, tm, fsec, SECS_PER_DAY);
					if (*str == '\0')
						return 0;
					if (*str == 'T')
					{
						datepart = false;
						havefield = false;
						continue;
					}
					return DTERR_BAD_FORMAT;
				default:
					/* not a valid date unit suffix */
					return DTERR_BAD_FORMAT;
			}
		}
		else
		{
			switch (unit)		/* after T: H M S */
			{
				case 'H':
					tm->tm_hour += val;
					AdjustFractSeconds(fval, tm, fsec, SECS_PER_HOUR);
					break;
				case 'M':
					tm->tm_min += val;
					AdjustFractSeconds(fval, tm, fsec, SECS_PER_MINUTE);
					break;
				case 'S':
					tm->tm_sec += val;
					AdjustFractSeconds(fval, tm, fsec, 1);
					break;
				case '\0':		/* ISO 8601 4.4.3.3 Alternative Format */
					if (ISO8601IntegerWidth(fieldstart) == 6 && !havefield)
					{
						tm->tm_hour += val / 10000;
						tm->tm_min += (val / 100) % 100;
						tm->tm_sec += val % 100;
						AdjustFractSeconds(fval, tm, fsec, 1);
						return 0;
					}
					/* Else fall through to extended alternative format */
					[[fallthrough]];
				case ':':		/* ISO 8601 4.4.3.3 Alternative Format,
								 * Extended */
					if (havefield)
						return DTERR_BAD_FORMAT;

					tm->tm_hour += val;
					AdjustFractSeconds(fval, tm, fsec, SECS_PER_HOUR);
					if (unit == '\0')
						return 0;

					dterr = ParseISO8601Number(str, &str, &val, &fval);
					if (dterr)
						return dterr;
					tm->tm_min += val;
					AdjustFractSeconds(fval, tm, fsec, SECS_PER_MINUTE);
					if (*str == '\0')
						return 0;
					if (*str != ':')
						return DTERR_BAD_FORMAT;
					str++;

					dterr = ParseISO8601Number(str, &str, &val, &fval);
					if (dterr)
						return dterr;
					tm->tm_sec += val;
					AdjustFractSeconds(fval, tm, fsec, 1);
					if (*str == '\0')
						return 0;
					return DTERR_BAD_FORMAT;

				default:
					/* not a valid time unit suffix */
					return DTERR_BAD_FORMAT;
			}
		}

		havefield = true;
	}

	return 0;
}
