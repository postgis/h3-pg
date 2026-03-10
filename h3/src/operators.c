/*
 * Copyright 2022-2024 Zacharias Knudsen
 * Copyright 2026 Darafei Praliaskouski
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

#include <fmgr.h> // PG_FUNCTION_ARGS

#include "algos.h"
#include "operators.h"
#include "type.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_distance);

/* b-tree */
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_eq);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_ne);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_lt);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_le);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gt);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_ge);

/* r-tree */
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_overlaps);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_contains);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_contained_by);

/*
 * Compute grid distance after refining the coarser input to the finer
 * resolution's center child, matching the SQL-visible <-> operator semantics.
 */
H3Error
h3index_grid_distance(H3Index a, H3Index b, int64_t *distance)
{
	int			resA = getResolution(a);
	int			resB = getResolution(b);

	if (resA < resB)
	{
		H3Error error = cellToCenterChild(a, resB, &a);

		if (error)
			return error;
	}
	else if (resB < resA)
	{
		H3Error error = cellToCenterChild(b, resA, &b);

		if (error)
			return error;
	}

	return gridDistance(a, b, distance);
}

/*
 * Distance operator allowing for different resolutions.
 *
 * Keep the SQL-visible type as bigint so extension upgrades do not need to
 * drop and recreate the operator. gridDistance failures sort last by using
 * the maximum bigint sentinel instead of the old negative error marker.
 */
Datum
h3index_distance(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);
	int64_t		distance;

	if (h3index_grid_distance(a, b, &distance))
		PG_RETURN_INT64(PG_INT64_MAX);

	PG_RETURN_INT64(distance);
}

/* b-tree operators */
Datum
h3index_eq(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(a == b);
}

Datum
h3index_ne(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(a != b);
}

Datum
h3index_lt(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(a < b);
}

Datum
h3index_le(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(a <= b);
}

Datum
h3index_gt(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(a > b);
}

Datum
h3index_ge(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(a >= b);
}

/* r-tree operators */
Datum
h3index_overlaps(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(containment(a, b) != 0);
}

Datum
h3index_contains(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(containment(a, b) > 0);
}

Datum
h3index_contained_by(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);

	PG_RETURN_BOOL(containment(b, a) > 0);
}
