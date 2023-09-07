/*-------------------------------------------------------------------------
 *
 * pg_statistic_ext_data.h
 *	  definition of the "extended statistics data" system catalog
 *	  (pg_statistic_ext_data)
 *
 * This catalog stores the statistical data for extended statistics objects.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_statistic_ext_data.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_STATISTIC_EXT_DATA_H
#define PG_STATISTIC_EXT_DATA_H

#include "catalog/genbki.h"
/*
 * BEGIN SPANGRES
 * Patched to remove the catalog/ prefix, because the way we're generating
 * headers doesn't put them in a subdirectory. That should be fixed at some
 * point
 * END SPANGRES
 */
#include "catalog/pg_statistic_ext_data_d.h"

/* ----------------
 *		pg_statistic_ext_data definition.  cpp turns this into
 *		typedef struct FormData_pg_statistic_ext_data
 * ----------------
 */
CATALOG(pg_statistic_ext_data,3429,StatisticExtDataRelationId)
{
	Oid			stxoid;			/* statistics object this data is for */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	pg_ndistinct stxdndistinct; /* ndistinct coefficients (serialized) */
	pg_dependencies stxddependencies;	/* dependencies (serialized) */
	pg_mcv_list stxdmcv;		/* MCV (serialized) */

#endif

} FormData_pg_statistic_ext_data;

/* ----------------
 *		Form_pg_statistic_ext_data corresponds to a pointer to a tuple with
 *		the format of pg_statistic_ext_data relation.
 * ----------------
 */
typedef FormData_pg_statistic_ext_data * Form_pg_statistic_ext_data;

#endif							/* PG_STATISTIC_EXT_DATA_H */
