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

#include <postgres.h>			 // Datum, etc.
#include <fmgr.h>				 // PG_FUNCTION_ARGS, etc.
#include <funcapi.h>			 // Needed to return HeapTuple
#include <access/htup_details.h> // Needed to return HeapTuple
#include <utils/geo_decls.h>	 // making native points

#include <h3api.h> // Main H3 include
#include "extension.h"

PG_FUNCTION_INFO_V1(h3_are_neighbor_cells);
PG_FUNCTION_INFO_V1(h3_cells_to_directed_edge);
PG_FUNCTION_INFO_V1(h3_is_valid_directed_edge);
PG_FUNCTION_INFO_V1(h3_get_directed_edge_origin);
PG_FUNCTION_INFO_V1(h3_get_directed_edge_destination);
PG_FUNCTION_INFO_V1(h3_directed_edge_to_cells);
PG_FUNCTION_INFO_V1(h3_origin_to_directed_edges);
PG_FUNCTION_INFO_V1(h3_directed_edge_to_boundary);

/* Returns whether or not the provided H3Indexes are neighbors */
Datum
h3_are_neighbor_cells(PG_FUNCTION_ARGS)
{
	H3Index		origin = PG_GETARG_H3INDEX(0);
	H3Index		destination = PG_GETARG_H3INDEX(1);

	int			areNeighbors;
	H3Error		error = areNeighborCells(origin, destination, &areNeighbors);

	ASSERT_EXTERNAL(error == 0, "Something went wrong");
	PG_RETURN_BOOL(areNeighbors);
}

/*
 * Returns a unidirectional edge H3 index based on the provided origin and
 * destination
 */
Datum
h3_cells_to_directed_edge(PG_FUNCTION_ARGS)
{
	H3Index		origin = PG_GETARG_H3INDEX(0);
	H3Index		destination = PG_GETARG_H3INDEX(1);
	H3Index		edge;
	H3Error		error = cellsToDirectedEdge(origin, destination, &edge);

	ASSERT_EXTERNAL(error == 0, "Something went wrong");
	PG_RETURN_H3INDEX(edge);
}

/* Determines if the provided H3Index is a valid unidirectional edge index */
Datum
h3_is_valid_directed_edge(PG_FUNCTION_ARGS)
{
	H3Index		edge = PG_GETARG_H3INDEX(0);
	bool		isValid = isValidDirectedEdge(edge);

	PG_RETURN_BOOL(isValid);
}

/* Returns the origin hexagon from the unidirectional edge H3Index */
Datum
h3_get_directed_edge_origin(PG_FUNCTION_ARGS)
{
	H3Index		edge = PG_GETARG_H3INDEX(0);
	H3Index		origin;

	H3Error		error  = getDirectedEdgeOrigin(edge, &origin);

	ASSERT_EXTERNAL(error == 0, "Something went wrong");
	PG_RETURN_H3INDEX(origin);
}

/* Returns the destination hexagon from the unidirectional edge H3Index */
Datum
h3_get_directed_edge_destination(PG_FUNCTION_ARGS)
{
	H3Index		edge = PG_GETARG_H3INDEX(0);
	H3Index		destination;
	
	H3Error		error = getDirectedEdgeDestination(edge, &destination);

	ASSERT_EXTERNAL(error == 0, "Something went wrong");
	PG_RETURN_H3INDEX(destination);
}

/* Returns the origin, destination pair of hexagon IDs for the given edge ID */
Datum
h3_directed_edge_to_cells(PG_FUNCTION_ARGS)
{
	TupleDesc	tuple_desc;
	Datum		values[2];
	bool		nulls[2] = {false};
	HeapTuple	tuple;
	Datum		result;

	H3Index		edge = PG_GETARG_H3INDEX(0);
	H3Index    *indexes = palloc(sizeof(H3Index) * 2);

	directedEdgeToCells(edge, indexes);

	ENSURE_TYPEFUNC_COMPOSITE(get_call_result_type(fcinfo, NULL, &tuple_desc));
	tuple_desc = BlessTupleDesc(tuple_desc);

	values[0] = H3IndexGetDatum(indexes[0]);
	values[1] = H3IndexGetDatum(indexes[1]);

	tuple = heap_form_tuple(tuple_desc, values, nulls);
	result = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(result);
}

/* Provides all of the unidirectional edges from the current H3Index */
Datum
h3_origin_to_directed_edges(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		FuncCallContext *funcctx = SRF_FIRSTCALL_INIT();
		MemoryContext oldcontext =
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		H3Index		origin = PG_GETARG_H3INDEX(0);
		int			maxSize = 6;
		H3Index    *edges = palloc(sizeof(H3Index) * maxSize);

		originToDirectedEdges(origin, edges);

		funcctx->user_fctx = edges;
		funcctx->max_calls = maxSize;
		MemoryContextSwitchTo(oldcontext);
	}

	SRF_RETURN_H3_INDEXES_FROM_USER_FCTX();
}

/* Provides the coordinates defining the unidirectional edge */
Datum
h3_directed_edge_to_boundary(PG_FUNCTION_ARGS)
{
	H3Index		edge = PG_GETARG_H3INDEX(0);

	CellBoundary geoBoundary;
	POLYGON    *polygon;
	int			size;

	directedEdgeToBoundary(edge, &geoBoundary);

	size = offsetof(POLYGON, p[0]) +sizeof(polygon->p[0]) * geoBoundary.numVerts;
	polygon = (POLYGON *) palloc(size);
	SET_VARSIZE(polygon, size);
	polygon->npts = geoBoundary.numVerts;

	for (int v = 0; v < geoBoundary.numVerts; v++)
	{
		polygon->p[v].x = radsToDegs(geoBoundary.verts[v].lat);
		polygon->p[v].y = radsToDegs(geoBoundary.verts[v].lng);
	}
	PG_RETURN_POLYGON_P(polygon);
}
