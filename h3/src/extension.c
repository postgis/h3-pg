/*
 * Copyright 2018-2020 Bytes & Brains
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <math.h> // cos, sin, etc.
#include <assert.h>

#include <postgres.h>			 // Datum, etc.
#include <fmgr.h>				 // PG_FUNCTION_ARGS, etc.
#include <funcapi.h>			 // Definitions for functions which return sets
#include <access/htup_details.h> // Needed to return HeapTuple
#include <utils/guc.h>			 // for GetConfigOption*
#include <utils/builtins.h>

#include <h3api.h> // Main H3 include
#include "extension.h"

/* should only be in ONE file */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(h3_get_extension_version);

/* Return version number for this extension (not main h3 lib) */
Datum
h3_get_extension_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(EXTVERSION));
}

bool		h3_guc_strict = false;
bool		h3_guc_extend_antimeridian = false;

void
_PG_init(void)
{
	DefineCustomBoolVariable("h3.strict",
						 "Enable strict indexing (fail on invalid lng/lat).",
							 NULL,
							 &h3_guc_strict,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("h3.extend_antimeridian",
						 "Extend boundaries by 180th meridian, when possible.",
							 NULL,
							 &h3_guc_extend_antimeridian,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}


/*
 * Set-Returning-Function assume user fctx contains indices
 * will skip missing (all zeros) indices
 */
Datum
srf_return_h3_indexes_from_user_fctx(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx = SRF_PERCALL_SETUP();
	int			call_cntr = funcctx->call_cntr;
	int			max_calls = funcctx->max_calls;

	H3Index    *indices = (H3Index *) funcctx->user_fctx;

	/* skip missing indices (all zeros) */
	while (call_cntr < max_calls && !indices[call_cntr])
	{
		funcctx->call_cntr = ++call_cntr;
	};

	if (call_cntr < max_calls)
	{
		Datum		result = H3IndexGetDatum(indices[call_cntr]);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * Returns hex/distance tuples from user_fctx
 * will skip missing (all zeros) indices
 */
Datum
srf_return_h3_index_distances_from_user_fctx(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx = SRF_PERCALL_SETUP();
	int			call_cntr = funcctx->call_cntr;
	int			max_calls = funcctx->max_calls;

	hexDistanceTuple *user_fctx = funcctx->user_fctx;
	H3Index    *indices = user_fctx->indices;
	int		   *distances = user_fctx->distances;

	/* skip missing indices (all zeros) */
	while (!indices[call_cntr])
	{
		funcctx->call_cntr = ++call_cntr;
	};

	if (call_cntr < max_calls)
	{
		TupleDesc	tuple_desc = funcctx->tuple_desc;
		Datum		values[2];
		bool		nulls[2] = {false};
		HeapTuple	tuple;
		Datum		result;

		values[0] = H3IndexGetDatum(indices[call_cntr]);
		values[1] = Int32GetDatum(distances[call_cntr]);

		tuple = heap_form_tuple(tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}
