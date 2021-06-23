/*
 * Copyright 2018-2021 Bytes & Brains
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

#include <postgres.h> // Datum, etc.
#include <fmgr.h>	  // PG_FUNCTION_ARGS, etc.
#include <funcapi.h>  // Definitions for functions which return sets
#include <utils/geo_decls.h> // making native points
#include <utils/builtins.h> // making native points

#include <h3api.h> // Main H3 include
#include "extension.h"

PG_FUNCTION_INFO_V1(h3_distance);
PG_FUNCTION_INFO_V1(h3_get_hexagon_area_avg);
PG_FUNCTION_INFO_V1(h3_cell_area);
PG_FUNCTION_INFO_V1(h3_get_hexagon_edge_length_avg);
PG_FUNCTION_INFO_V1(h3_exact_edge_length);
PG_FUNCTION_INFO_V1(h3_get_num_cells);
PG_FUNCTION_INFO_V1(h3_get_res_0_cells);
PG_FUNCTION_INFO_V1(h3_get_pentagons);

/* The great circle distance in radians between two spherical coordinates */
Datum
h3_distance(PG_FUNCTION_ARGS)
{
	Point	   *aPoint = PG_GETARG_POINT_P(0);
	Point	   *bPoint = PG_GETARG_POINT_P(1);
	char	   *unit = text_to_cstring(PG_GETARG_TEXT_PP(2));

	LatLng		a;
	LatLng		b;
	double		distance;

	a.lng = degsToRads(aPoint->x);
	a.lat = degsToRads(aPoint->y);
	b.lng = degsToRads(bPoint->x);
	b.lat = degsToRads(bPoint->y);

	if (strcmp(unit, "rads") == 0)
		distance = distanceRads(&a, &b);
	else if (strcmp(unit, "km") == 0)
		distance = distanceKm(&a, &b);
	else if (strcmp(unit, "m") == 0)
		distance = distanceM(&a, &b);
	else
		ASSERT_EXTERNAL(0, "Unit must be m, km or rads.");

	PG_RETURN_FLOAT8(distance);
}

/* Average hexagon area in square (kilo)meters at the given resolution */
Datum
h3_get_hexagon_area_avg(PG_FUNCTION_ARGS)
{
	int			resolution = PG_GETARG_INT32(0);
	char	   *unit = text_to_cstring(PG_GETARG_TEXT_PP(1));
	double		area;

	if (strcmp(unit, "km") == 0)
		area = getHexagonAreaAvgKm2(resolution);
	else if (strcmp(unit, "m") == 0)
		area = getHexagonAreaAvgM2(resolution);
	else
		ASSERT_EXTERNAL(0, "Unit must be m or km.");

	PG_RETURN_FLOAT8(area);
}

/* Exact area for a specific cell (hexagon or pentagon) */
Datum
h3_cell_area(PG_FUNCTION_ARGS)
{
	H3Index		cell = PG_GETARG_H3INDEX(0);
	char	   *unit = text_to_cstring(PG_GETARG_TEXT_PP(1));
	double		area;

	if (strcmp(unit, "rads^2") == 0)
		area = cellAreaRads2(cell);
	else if (strcmp(unit, "km^2") == 0)
		area = cellAreaKm2(cell);
	else if (strcmp(unit, "m^2") == 0)
		area = cellAreaM2(cell);
	else
		ASSERT_EXTERNAL(0, "Unit must be m^2, km^2 or rads^2.");

	PG_RETURN_FLOAT8(area);
}

/* Average hexagon edge length in (kilo)meters at the given resolution */
Datum
h3_get_hexagon_edge_length_avg(PG_FUNCTION_ARGS)
{
	int			resolution = PG_GETARG_INT32(0);
	char	   *unit = text_to_cstring(PG_GETARG_TEXT_PP(1));
	double		length;

	if (strcmp(unit, "km") == 0)
		length = getHexagonEdgeLengthAvgKm(resolution);
	else if (strcmp(unit, "m") == 0)
		length = getHexagonEdgeLengthAvgM(resolution);
	else
		ASSERT_EXTERNAL(0, "Unit must be m or km.");

	PG_RETURN_FLOAT8(length);
}

/* Exact length for a specific unidirectional edge */
Datum
h3_exact_edge_length(PG_FUNCTION_ARGS)
{
	H3Index		edge = PG_GETARG_H3INDEX(0);
	char	   *unit = text_to_cstring(PG_GETARG_TEXT_PP(1));
	double		length;

	if (strcmp(unit, "rads") == 0)
		length = exactEdgeLengthRads(edge);
	else if (strcmp(unit, "km") == 0)
		length = exactEdgeLengthKm(edge);
	else if (strcmp(unit, "m") == 0)
		length = exactEdgeLengthM(edge);
	else
		ASSERT_EXTERNAL(0, "Unit must be m, km or rads.");

	PG_RETURN_FLOAT8(length);
}

/* Number of unique H3 indexes at the given resolution */
Datum
h3_get_num_cells(PG_FUNCTION_ARGS)
{
	int			resolution = PG_GETARG_INT32(0);
	unsigned long long retVal = getNumCells(resolution);

	PG_RETURN_INT64(retVal);
}

/* Provides all resolution 0 indexes */
Datum
h3_get_res_0_cells(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		FuncCallContext *funcctx = SRF_FIRSTCALL_INIT();
		MemoryContext oldcontext =
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		int			count = res0CellCount();
		H3Index    *indexes = palloc(sizeof(H3Index) * count);

		getRes0Cells(indexes);

		funcctx->user_fctx = indexes;
		funcctx->max_calls = count;
		MemoryContextSwitchTo(oldcontext);
	}

	SRF_RETURN_H3_INDEXES_FROM_USER_FCTX();
}

/* All the pentagon H3 indexes at the specified resolution */
Datum
h3_get_pentagons(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		FuncCallContext *funcctx = SRF_FIRSTCALL_INIT();
		MemoryContext oldcontext =
		MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		int			resolution = PG_GETARG_INT32(0);
		int			count = pentagonCount();
		H3Index    *indexes = palloc(sizeof(H3Index) * count);

		getPentagons(resolution, indexes);

		funcctx->user_fctx = indexes;
		funcctx->max_calls = count;
		MemoryContextSwitchTo(oldcontext);
	}

	SRF_RETURN_H3_INDEXES_FROM_USER_FCTX();
}
