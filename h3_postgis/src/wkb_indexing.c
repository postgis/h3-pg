/*
 * Copyright 2022-2024 Zacharias Knudsen
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

#include <fmgr.h>  // PG_FUNCTION_ARGS
#include <math.h>

#include "constants.h"
#include "error.h"
#include "type.h"
#include "wkb_split.h"
#include "wkb_vect3.h"
#include "wkb.h"

#define SIGN(x) ((x < 0) ? -1 : (x > 0) ? 1 \
										: 0)
/*
 * Stay just inside the pole when we synthesize an antimeridian bridge for a
 * single-crossing polar cell. The planar WKB path needs an explicit segment
 * there, but an exact +/-90 vertex would collapse the bridge.
 */
#define ABS_LAT_MAX (degsToRads(89.9999))

#define SPLIT_ASSERT(condition, message)			\
	ASSERT(										\
		condition,									\
		ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,		\
		message)

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cell_to_boundary_wkb);

/* Converts CellBoundary coordinates to degrees in place. */
void
			boundary_to_degs(CellBoundary * boundary);

/* Counts antimeridian crossings in one cell boundary. */
int
			boundary_crosses_180_num(const CellBoundary * boundary);

/* Splits one antimeridian-crossing boundary into west/east planar parts. */
void
			boundary_split_180(const CellBoundary * boundary, CellBoundary * part1, CellBoundary * part2);

/* Expands a single-crossing polar cell into one explicit planar boundary. */
void
			boundary_split_180_polar(const CellBoundary * boundary, CellBoundary * res);

/*
 * Serialize one H3 cell boundary to WKB.
 *
 * Most cells can be emitted directly. Cells crossing the antimeridian need to
 * be split first so the planar PostGIS geometry stays valid.
 */
Datum
h3_cell_to_boundary_wkb(PG_FUNCTION_ARGS)
{
	H3Index		cell = PG_GETARG_H3INDEX(0);

	bytea	   *wkb;
	CellBoundary boundary;
	int			crossNum;

	h3_assert(cellToBoundary(cell, &boundary));

	crossNum = boundary_crosses_180_num(&boundary);
	if (crossNum == 0)
	{
		/* Cell is not crossed by antimeridian */
		boundary_to_degs(&boundary);
		wkb = boundary_to_wkb(&boundary);
	}
	else if (crossNum == 1)
	{
		/*
		 * A single crossing means the cell touches a pole and returns through the
		 * prime meridian. That needs the dedicated polar splitter.
		 */
		CellBoundary split;

		boundary_split_180_polar(&boundary, &split);
		boundary_to_degs(&split);
		wkb = boundary_to_wkb(&split);
	}
	else
	{
		/* A normal antimeridian crossing splits into west/east polygons. */
		CellBoundary parts[2];

		boundary_split_180(&boundary, &parts[0], &parts[1]);

		boundary_to_degs(&parts[0]);
		boundary_to_degs(&parts[1]);
		wkb = boundary_array_to_wkb(parts, 2);
	}

	PG_RETURN_BYTEA_P(wkb);
}

void
boundary_to_degs(CellBoundary * boundary)
{
	LatLng	   *verts = boundary->verts;
	const int	numVerts = boundary->numVerts;

	for (int v = 0; v < numVerts; v++)
	{
		verts[v].lng = radsToDegs(verts[v].lng);
		verts[v].lat = radsToDegs(verts[v].lat);
	}
}

int
boundary_crosses_180_num(const CellBoundary * boundary)
{
	const int	numVerts = boundary->numVerts;
	const LatLng *verts = boundary->verts;

	int			num = 0;

	for (int v = 0; v < numVerts; v++)
	{
		double		lon = verts[v].lng;
		double		nextLon = verts[(v + 1) % numVerts].lng;

		if (SIGN(lon) != SIGN(nextLon)
			&& fabs(lon - nextLon) > M_PI)
		{
			++num;
		}
	}
	return num;
}

void
boundary_split_180(const CellBoundary * boundary, CellBoundary * part1, CellBoundary * part2)
{
	const int	numVerts = boundary->numVerts;
	const LatLng *verts = boundary->verts;

	part1->numVerts = 0;
	part2->numVerts = 0;
	for (int v = 0; v < numVerts; v++)
	{
		int			next = (v + 1) % numVerts;
		double		lon;
		double		nextLon;
		CellBoundary *part;

		lon = verts[v].lng;
		nextLon = verts[next].lng;
		part = (lon < 0) ? part1 : part2;

		/* Add current vertex */
		part->verts[part->numVerts++] = verts[v];

		if (SIGN(lon) != SIGN(nextLon))
		{
			LatLng		vert;

			SPLIT_ASSERT(
						 fabs(lon - nextLon) > M_PI,
						 "Cell boundaries crossed by the Prime meridian "
						 "must be handled in `boundary_split_180_polar`");

			vert.lat = split_180_lat(&verts[v], &verts[next]);
			vert.lng = (lon < 0) ? -M_PI : M_PI;

			/* Add split point */
			/* current part  */
			part->verts[part->numVerts++] = vert;
			/* next part */
			vert.lng = -vert.lng;
			part = (part == part1) ? part2 : part1;
			part->verts[part->numVerts++] = vert;
		}
	}
}

void
boundary_split_180_polar(const CellBoundary * boundary, CellBoundary * res)
{
	const int	numVerts = boundary->numVerts;
	const LatLng *verts = boundary->verts;

	res->numVerts = 0;
	for (int v = 0; v < numVerts; v++)
	{
		int			next = (v + 1) % numVerts;
		double		lon;
		double		nextLon;

		/* Add current vertex */
		res->verts[res->numVerts++] = verts[v];

		lon = verts[v].lng;
		nextLon = verts[next].lng;
		if (SIGN(lon) != SIGN(nextLon)
			&& fabs(lon - nextLon) > M_PI)
		{
			LatLng		vert;
			double		splitLat;

			SPLIT_ASSERT(
						 v + 1 == res->numVerts,
					"Cell boundaries crossed by antimeridian more than once "
						 "must be handled in `boundary_split_180`");

			splitLat = split_180_lat(&verts[v], &verts[next]);

			/* Add intersection point */
			vert.lat = splitLat;
			vert.lng = (lon < 0) ? -M_PI : M_PI;
			res->verts[res->numVerts++] = vert;

			/* Add points on antimeridian near the pole */
			vert.lat = SIGN(vert.lat) * ABS_LAT_MAX;
			res->verts[res->numVerts++] = vert;
			vert.lng = -vert.lng;
			res->verts[res->numVerts++] = vert;

			/* Add intersection point */
			vert.lat = splitLat;
			res->verts[res->numVerts++] = vert;
		}
	}
}
