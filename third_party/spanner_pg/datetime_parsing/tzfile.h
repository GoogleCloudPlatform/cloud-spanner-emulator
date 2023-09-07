/* Layout and location of TZif files.  */

#ifndef DATETIME_PARSING_TZFILE_H
#define DATETIME_PARSING_TZFILE_H

/*
 * This file is in the public domain, so clarified as of
 * 1996-06-05 by Arthur David Olson.
 *
 * IDENTIFICATION
 *	  datetime_parsing/tzfile.h
 */

/*
 * This header is for use ONLY with the time conversion code.
 * There is no guarantee that it will remain unchanged,
 * or that it will remain at all.
 * Do NOT copy it to any system include directory.
 * Thank you!
 */

/*
 * Information about time zone files.
 */

// SPANNER_PG: copied from src/timezone/tzfile.h

#define TZDEFAULT	"/etc/localtime"

struct tzhead
{
	char		tzh_magic[4];	/* TZ_MAGIC */
	char		tzh_version[1]; /* '\0' or '2' or '3' as of 2013 */
	char		tzh_reserved[15];	/* reserved; must be zero */
	char		tzh_ttisutcnt[4];	/* coded number of trans. time flags */
	char		tzh_ttisstdcnt[4];	/* coded number of trans. time flags */
	char		tzh_leapcnt[4]; /* coded number of leap seconds */
	char		tzh_timecnt[4]; /* coded number of transition times */
	char		tzh_typecnt[4]; /* coded number of local time types */
	char		tzh_charcnt[4]; /* coded number of abbr. chars */
};

#define TZ_MAX_TIMES	2000

/* This must be at least 17 for Europe/Samara and Europe/Vilnius.  */
#define TZ_MAX_TYPES	256		/* Limited by what (unsigned char)'s can hold */

#define TZ_MAX_CHARS	50		/* Maximum number of abbreviation characters */
 /* (limited by what unsigned chars can hold) */

#define TZ_MAX_LEAPS	50		/* Maximum number of leap second corrections */

#endif							/* !defined DATETIME_PARSING_TZFILE_H */
