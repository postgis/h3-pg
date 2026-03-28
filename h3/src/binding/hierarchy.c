/*
 * Copyright 2023-2024 Zacharias Knudsen
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

#include <postgres.h>
#include <h3api.h>

#include <fmgr.h>		 // PG_FUNCTION_INFO_V1
#include <funcapi.h>	 // SRF_IS_FIRSTCALL
#include <utils/array.h> // ArrayType

#include "error.h"
#include "type.h"
#include "srf.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_parent);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_children);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_center_child);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_child_pos);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_child_pos_to_cell);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_compact_cells);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_uncompact_cells);

typedef struct
{
	H3Index		parent;
	int			child_resolution;
	int64_t		next_child_pos;
	int64_t		child_count;
} H3ChildrenFctx;

/* Returns the parent (coarser) index containing given index */
Datum
h3_cell_to_parent(PG_FUNCTION_ARGS)
{
	H3Index		parent;
	H3Index		origin = PG_GETARG_H3INDEX(0);
	int			resolution = PG_GETARG_OPTIONAL_RES(1, origin, -1);

	h3_assert(cellToParent(origin, resolution, &parent));

	PG_RETURN_H3INDEX(parent);
}

/* Returns children indexes at given resolution (or next resolution if none given) */
Datum
h3_cell_to_children(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		FuncCallContext *funcctx = SRF_FIRSTCALL_INIT();
		MemoryContext oldcontext =
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		H3ChildrenFctx *user_fctx = palloc(sizeof(H3ChildrenFctx));

		/* Stream children in the same order defined by childPosToCell(). */
		user_fctx->parent = PG_GETARG_H3INDEX(0);
		user_fctx->child_resolution = PG_GETARG_OPTIONAL_RES(1, user_fctx->parent, 1);
		user_fctx->next_child_pos = 0;

		h3_assert(cellToChildrenSize(
			user_fctx->parent,
			user_fctx->child_resolution,
			&user_fctx->child_count
		));

		funcctx->user_fctx = user_fctx;
		funcctx->max_calls = user_fctx->child_count;
		MemoryContextSwitchTo(oldcontext);
	}

	{
		FuncCallContext *funcctx = SRF_PERCALL_SETUP();
		H3ChildrenFctx *user_fctx = funcctx->user_fctx;

		if (user_fctx->next_child_pos < user_fctx->child_count)
		{
			H3Index		child;

			h3_assert(childPosToCell(
				user_fctx->next_child_pos++,
				user_fctx->parent,
				user_fctx->child_resolution,
				&child
			));

			SRF_RETURN_NEXT(funcctx, H3IndexGetDatum(child));
		}

		SRF_RETURN_DONE(funcctx);
	}
}

/* Returns the center child (finer) index contained by input index at given resolution */
Datum
h3_cell_to_center_child(PG_FUNCTION_ARGS)
{
	H3Index		child;
	H3Index		origin = PG_GETARG_H3INDEX(0);
	int			resolution = PG_GETARG_OPTIONAL_RES(1, origin, 1);

	h3_assert(cellToCenterChild(origin, resolution, &child));

	PG_RETURN_H3INDEX(child);
}

Datum
h3_cell_to_child_pos(PG_FUNCTION_ARGS)
{
	H3Index		child = PG_GETARG_H3INDEX(0);
	int			parentRes = PG_GETARG_INT32(1);
	int64_t		childPos;

	h3_assert(cellToChildPos(child, parentRes, &childPos));

	PG_RETURN_INT64(childPos);
}

Datum
h3_child_pos_to_cell(PG_FUNCTION_ARGS)
{
	int64_t		childPos = PG_GETARG_INT64(0);
	H3Index		parent = PG_GETARG_H3INDEX(1);
	int			childRes = PG_GETARG_INT32(2);
	H3Index		child;

	h3_assert(childPosToCell(childPos, parent, childRes, &child));

	PG_RETURN_H3INDEX(child);
}

Datum
h3_compact_cells(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
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

		h3_assert(compactCells(h3set, compactedSet, max));

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

		h3_assert(uncompactCellsSize(compactedSet, numCompacted, resolution, &max));

		uncompactedSet = palloc0(max * sizeof(H3Index));

		h3_assert(uncompactCells(compactedSet, numCompacted, uncompactedSet, max, resolution));

		funcctx->user_fctx = uncompactedSet;
		funcctx->max_calls = max;
		MemoryContextSwitchTo(oldcontext);
	}

	SRF_RETURN_H3_INDEXES_FROM_USER_FCTX();
}
