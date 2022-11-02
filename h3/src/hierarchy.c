/*
 * Copyright 2018-2022 Bytes & Brains
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

#include <postgres.h>		// Datum, etc.
#include <fmgr.h>			// PG_FUNCTION_ARGS, etc.
#include <funcapi.h>		// Definitions for functions which return sets
#include <utils/array.h>	// Arrays
#include <utils/memutils.h> // MaxAllocSize

#include <h3api.h> // Main H3 include
#include "extension.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_parent);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_children);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_center_child);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_compact_cells);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_uncompact_cells);

/* Returns the parent (coarser) index containing given index */
Datum
h3_cell_to_parent(PG_FUNCTION_ARGS)
{
	H3Index		parent;
	H3Error		error;
	H3Index		origin = PG_GETARG_H3INDEX(0);
	int			resolution = PG_GETARG_OPTIONAL_RES(1, origin, -1);

	error = cellToParent(origin, resolution, &parent);
	H3_ERROR(error, "cellToParent");

	PG_RETURN_H3INDEX(parent);
}

/* Returns children indexes at given resolution (or next resolution if none given) */
Datum
h3_cell_to_children(PG_FUNCTION_ARGS)
{
	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		int64_t		max;
		int64_t		size;
		H3Index    *children;
		H3Error		error;

		/* create a function context for cross-call persistence */
		FuncCallContext *funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		MemoryContext oldcontext =
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* BEGIN One-time setup code */

		/* ensure valid resolution target */
		H3Index		origin = PG_GETARG_H3INDEX(0);
		int			resolution = PG_GETARG_OPTIONAL_RES(1, origin, 1);

		error = cellToChildrenSize(origin, resolution, &max);
		H3_ERROR(error, "cellToChildrenSize");

		size = max * sizeof(H3Index);
		ASSERT(
			   AllocSizeIsValid(size),
			   ERRCODE_OUT_OF_MEMORY,
			   "Cannot allocate necessary amount memory, try using h3_cell_to_children_slow()"
			);
		children = palloc(size);

		error = cellToChildren(origin, resolution, children);
		H3_ERROR(error, "cellToChildren");

		funcctx->user_fctx = children;
		funcctx->max_calls = max;

		/* END One-time setup code */

		MemoryContextSwitchTo(oldcontext);
	}

	SRF_RETURN_H3_INDEXES_FROM_USER_FCTX();
}

/* Returns the center child (finer) index contained by input index at given resolution */
Datum
h3_cell_to_center_child(PG_FUNCTION_ARGS)
{
	H3Index		child;
	H3Error		error;
	H3Index		origin = PG_GETARG_H3INDEX(0);
	int			resolution = PG_GETARG_OPTIONAL_RES(1, origin, 1);

	error = cellToCenterChild(origin, resolution, &child);
	H3_ERROR(error, "cellToCenterChild");

	PG_RETURN_H3INDEX(child);
}

Datum
h3_compact_cells(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		H3Error		error;
		Datum		value;
		bool		isnull;
		int			i = 0;

		FuncCallContext *funcctx = SRF_FIRSTCALL_INIT();
		MemoryContext oldcontext =
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
		int			max = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
		ArrayIterator iterator = array_create_iterator(array, 0, NULL);
		H3Index    *h3set = palloc(max * sizeof(H3Index));
		H3Index    *compactedSet = palloc0(max * sizeof(H3Index));

		/* Extract data from array into h3set, and wipe compactedSet memory */
		while (array_iterate(iterator, &value, &isnull))
		{
			h3set[i++] = DatumGetH3Index(value);
		}

		error = compactCells(h3set, compactedSet, max);
		H3_ERROR(error, "compactCells");

		funcctx->user_fctx = compactedSet;
		funcctx->max_calls = max;
		MemoryContextSwitchTo(oldcontext);
	}

	SRF_RETURN_H3_INDEXES_FROM_USER_FCTX();
}

Datum
h3_uncompact_cells(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		H3Error		error;
		int			resolution;
		Datum		value;
		bool		isnull;
		int			i = 0;
		int64_t		max;
		H3Index    *uncompactedSet;

		FuncCallContext *funcctx = SRF_FIRSTCALL_INIT();
		MemoryContext oldcontext =
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);

		int			numCompacted = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
		ArrayIterator iterator = array_create_iterator(array, 0, NULL);
		H3Index    *compactedSet = palloc(numCompacted * sizeof(H3Index));

		/*
		 * Extract data from array into compactedSet, and wipe compactedSet
		 * memory
		 */
		while (array_iterate(iterator, &value, &isnull))
		{
			compactedSet[i++] = DatumGetH3Index(value);
		}

		if (PG_NARGS() == 2)
		{
			resolution = PG_GETARG_INT32(1);
		}
		else
		{
			/* resolution parameter not set */
			int			highRes = 0;

			/* Find highest resolution in the given set */
			for (int i = 0; i < numCompacted; i++)
			{
				int			curRes = getResolution(compactedSet[i]);

				if (curRes > highRes)
					highRes = curRes;
			}

			/*
			 * If the highest resolution is the maximun allowed, uncompact to
			 * that
			 */
			/* Else uncompact one step further than the highest resolution */
			resolution = (highRes == 15 ? highRes : highRes + 1);
		}

		error = uncompactCellsSize(compactedSet, numCompacted, resolution, &max);
		H3_ERROR(error, "uncompactCellsSize");

		uncompactedSet = palloc0(max * sizeof(H3Index));

		error = uncompactCells(compactedSet, numCompacted, uncompactedSet, max, resolution);
		H3_ERROR(error, "uncompactCells");

		funcctx->user_fctx = uncompactedSet;
		funcctx->max_calls = max;
		MemoryContextSwitchTo(oldcontext);
	}

	SRF_RETURN_H3_INDEXES_FROM_USER_FCTX();
}
