// SPANGRES BEGIN
/*-------------------------------------------------------------------------
 *
 * reservoir.c
 *	  support routines for SPANNER.RESERVOIR tablesample method
 *
 * This file only implements `tsm_spanner_reservoir_handler()` to provide
 * details on argument types to the SPANNER.RESERVOIR sampling method. The
 * actual sampling is implemented in Spandex.
 *
 * IDENTIFICATION
 *	  src/backend/access/tablesample/spanner_reservoir.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tsmapi.h"

/*
 * Create a TsmRoutine descriptor for the SPANNER.RESERVOIR method.
 */
Datum
tsm_spanner_reservoir_handler(PG_FUNCTION_ARGS)
{
	TsmRoutine *tsm = makeNode(TsmRoutine);

	tsm->parameterTypes = list_make1_oid(INT8OID);
	tsm->repeatable_across_queries = false;
	tsm->repeatable_across_scans = false;
	tsm->SampleScanGetSampleSize = NULL;
	tsm->InitSampleScan = NULL;
	tsm->BeginSampleScan = NULL;
	tsm->NextSampleBlock = NULL;
	tsm->NextSampleTuple = NULL;
	tsm->EndSampleScan = NULL;

	PG_RETURN_POINTER(tsm);
}
// SPANGRES END