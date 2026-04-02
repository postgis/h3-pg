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

#include <algos.h>
#include <float.h>
#include <fmgr.h>		 // PG_FUNCTION_ARGS
#include <linkedGeo.h>
#include <math.h>
#include <utils/array.h> // using arrays
#include <vertexGraph.h>

#include "error.h"
#include "type.h"
#include "wkb_linked_geo.h"
#include "wkb_split.h"
#include "wkb.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_cells_to_multi_polygon_wkb);

typedef struct
{
	double		minLat;
	double		maxLat;
	double		minLng;
	double		maxLng;
	double		wrapLng;
} LoopBounds;

typedef struct
{
	double		minLat;
	double		maxLat;
	double		minLng;
	double		maxLng;
	bool		hasWest;
	bool		hasEast;
} BoundaryExtents;

typedef struct
{
	int			numParts;
	CellBoundary parts[2];
} SplitCellBoundary;

typedef struct
{
	LatLng		from;
	LatLng		to;
	int			splitCount;
	int			splitCap;
	double	   *splitTs;
} NodedSegment;

typedef struct
{
	LatLng		vertex;
	int			firstEdge;
	int			edgeCount;
} PolygonizeVertex;

typedef struct
{
	int			edgeIdx;
	double		angle;
} PolygonizeEdgeRef;

typedef struct
{
	LatLng		from;
	LatLng		to;
	int			fromVertex;
	int			toVertex;
	double		angle;
	int			nextEdge;
	bool		used;
} PolygonizeHalfEdge;

static const CellBoundary FULL_WORLD_BOUNDARY = {
	.numVerts = 4,
	.verts = {
		{.lat = 90.0, .lng = -180.0},
		{.lat = -90.0, .lng = -180.0},
		{.lat = -90.0, .lng = 180.0},
		{.lat = 90.0, .lng = 180.0},
	}
};

/* Converts LinkedGeoPolygon vertex coordinates to degrees in place */
static void
			linked_geo_polygon_to_degs(LinkedGeoPolygon * multiPolygon);

int
			boundary_crosses_180_num(const CellBoundary * boundary);

void
			boundary_split_180(const CellBoundary * boundary, CellBoundary * part1, CellBoundary * part2);

void
			boundary_split_180_polar(const CellBoundary * boundary, CellBoundary * res);

/*
 * Released H3 can return E_FAILED for coarse polar/global covers. In that
 * case, rebuild the planar boundary graph from split cell boundaries instead.
 *
 * This is intentionally the only local topology fallback that remains here:
 * normal cases stay on released H3 output, and SQL wrappers do not try to
 * repair invalid geometry after the fact.
 */
static bytea *
			coarse_cells_to_multi_polygon_wkb(const H3Index * h3set, int numHexes, int resolution);

static LinkedGeoPolygon *
			prepare_linked_polygon_for_wkb(const LinkedGeoPolygon * linkedPolygon, int numHexes, int resolution);

/* Exact globe covers have no planar boundary, so emit the world rectangle. */
static bool
			cell_set_is_full_globe(const H3Index * h3set, int numHexes);

/*
 * Split output can contain seam-sharing polygons that are valid individually
 * but invalid as one planar MultiPolygon. Rebuild those edges with released
 * H3's vertex-graph logic before WKB serialization.
 */
static LinkedGeoPolygon *
			merge_split_linked_polygons(const LinkedGeoPolygon * multiPolygon, int resolution);

static LinkedGeoPolygon *
			rebuild_noded_polygon_union(const LinkedGeoPolygon * multiPolygon, int resolution);

static LinkedGeoPolygon *
			split_self_touching_polygons(const LinkedGeoPolygon * multiPolygon);

static void
			maybe_remerge_split_linked_polygons(LinkedGeoPolygon ** multiPolygon, int resolution);

static LinkedGeoPolygon *
			normalize_split_linked_polygon(const LinkedGeoPolygon * multiPolygon);

static LinkedGeoPolygon *
			copy_linked_multi_polygon(const LinkedGeoPolygon * multiPolygon);

static void
			append_h3_loop_copy(LinkedGeoPolygon * polygon, const LinkedGeoLoop * loop);

static void
			collect_unique_split_loops(LinkedGeoPolygon * flattened, const LinkedGeoPolygon * multiPolygon);

static void
			append_polygon_chain(LinkedGeoPolygon ** result, LinkedGeoPolygon ** last, LinkedGeoPolygon * polygon);

static bool
			linked_geo_loops_equal(const LinkedGeoLoop * a, const LinkedGeoLoop * b);

static bool
			linked_polygon_has_equivalent_loop(const LinkedGeoPolygon * polygon, const LinkedGeoLoop * loop);

static bool
			linked_geo_loops_share_vertex(const LinkedGeoLoop * a, const LinkedGeoLoop * b);

static int
			copy_linked_geo_loop_vertices(const LinkedGeoLoop * loop, LatLng ** verts);

static bool
			linked_geo_loop_has_repeat_vertex(const LinkedGeoLoop * loop);

static void
			prune_polygon_spikes(LinkedGeoPolygon * multiPolygon);

static LinkedGeoPolygon *
			split_self_touching_components(const LinkedGeoPolygon * multiPolygon);

static void
			maybe_split_single_self_touching_loop(LinkedGeoPolygon ** multiPolygon);

static LinkedGeoPolygon *
			finalize_prepared_polygon(LinkedGeoPolygon * multiPolygon);

static void
			append_polygon_from_vertex_slice(LinkedGeoPolygon ** result, LinkedGeoPolygon ** last, const LatLng * verts, int vertCount);

static LatLng
			loop_probe_point(const LinkedGeoLoop * loop, const LoopBounds * bounds);

static void
			loop_bounds(const LinkedGeoLoop * loop, LoopBounds * bounds);

static bool
			point_inside_split_loop(const LinkedGeoLoop * loop, const LoopBounds * bounds, const LatLng * probe);

static int
			find_split_loop_parent(const LinkedGeoLoop ** loops, const LoopBounds * bounds, const LatLng * probes, const double *areas, int loopCount, int loopIdx);

static void
			split_cell_boundary(H3Index cell, SplitCellBoundary * splitBoundary);

static void
			append_boundary_polygon(LinkedGeoPolygon ** multiPolygon, LinkedGeoPolygon ** lastPolygon, const CellBoundary * boundary);

static int
			sort_unique_latitudes(double *lats, int count);

static int
			double_cmp(const void *a, const void *b);

static void
			graph_add_noded_linked_polygon_edges(VertexGraph * graph, const LinkedGeoPolygon * multiPolygon);

static int
			count_linked_polygon_edges(const LinkedGeoPolygon * multiPolygon);

static void
			collect_linked_polygon_segments(const LinkedGeoPolygon * multiPolygon, NodedSegment * segments, int segmentCount);

static void
			segment_add_split_t(NodedSegment * segment, double t);

static bool
			segment_intersection_t(const NodedSegment * a, const NodedSegment * b, double *ta, double *tb);

static bool
			segment_collinear_overlap_ts(const NodedSegment * a, const NodedSegment * b, double *aStart, double *aEnd, double *bStart, double *bEnd);

static double
			segment_project_t(const NodedSegment * segment, const LatLng * latlng);

static void
			segment_lat_lng_at(const NodedSegment * segment, double t, LatLng * latlng);

static void
			graph_add_edge(VertexGraph * graph, const LatLng * from, const LatLng * to);

static LinkedGeoPolygon *
			polygonize_noded_linked_polygon(const LinkedGeoPolygon * multiPolygon, int resolution);

static LinkedGeoPolygon *
			polygonize_noded_graph(const VertexGraph * graph);

static int
			polygonize_find_or_add_vertex(PolygonizeVertex **vertices, int *vertexCount, int *vertexCap, const LatLng *vertex);

static int
			polygonize_edge_ref_cmp(const void *a, const void *b);

static double
			linked_geo_loop_signed_area(const LinkedGeoLoop * loop);

static bool
			polygon_probe_in_union(const LinkedGeoPolygon * multiPolygon, const LinkedGeoPolygon * polygon);

static bool
			point_inside_split_multi_polygon(const LinkedGeoPolygon * multiPolygon, const LatLng * probe);

static bool
			linked_polygon_is_north_polar_single_self_touch(const LinkedGeoPolygon * multiPolygon);

static void
			h3_set_boundary_extents(const H3Index * h3set, int numHexes, BoundaryExtents * extents);

static bool
			prepared_polygon_matches_h3set_extents(const LinkedGeoPolygon * polygon, const H3Index * h3set, int numHexes);

static void
			build_extent_boundary(CellBoundary * boundary, const BoundaryExtents * extents);

static void
			prune_linked_geo_loop_spikes(LinkedGeoLoop * loop);

static double
			normalize_lng_around(double lng, double around);

Datum
h3_cells_to_multi_polygon_wkb(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	LinkedGeoPolygon *linkedPolygon;
	H3Error		error;
	int			numHexes;
	ArrayIterator iterator;
	Datum		value;
	bool		isnull;
	H3Index    *h3set;
	bytea	   *wkb = NULL;
	int			resolution = -1;
	bool		localLinkedPolygon = false;

	numHexes = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	h3set = palloc(numHexes * sizeof(*h3set));

	/* Extract data from array into h3set */
	iterator = array_create_iterator(array, 0, NULL);
	numHexes = 0;
	while (array_iterate(iterator, &value, &isnull))
		h3set[numHexes++] = DatumGetH3Index(value);

	if (numHexes > 0 && h3set[0])
		resolution = H3_EXPORT(getResolution)(h3set[0]);

	/*
	 * Start with released H3's own multipolygon builder. The fallback paths
	 * below exist only for the classes where that direct output is known to be
	 * wrong for planar PostGIS geometry.
	 */
	linkedPolygon = palloc0(sizeof(*linkedPolygon));
	error = cellsToLinkedMultiPolygon(h3set, numHexes, linkedPolygon);
	if (error == E_FAILED)
	{
		/*
		 * Released H3 can fail outright on exact globe covers and some coarse
		 * seam-heavy sets. Handle the exact globe directly and send the rest to
		 * the boundary-based coarse path.
		 */
		if (cell_set_is_full_globe(h3set, numHexes))
		{
			pfree(linkedPolygon);
			pfree(h3set);
			PG_RETURN_BYTEA_P(boundary_to_wkb(&FULL_WORLD_BOUNDARY));
		}

		wkb = coarse_cells_to_multi_polygon_wkb(h3set, numHexes, resolution);
		pfree(linkedPolygon);
		pfree(h3set);
		PG_RETURN_BYTEA_P(wkb);
	}
	h3_assert(error);

	/*
	 * Example: a narrow overlapping-bbox tile near the north pole can still
	 * select cells whose split boundaries span both sides of the antimeridian.
	 * That class is a full-width polar cap in planar output, not a narrow ring.
	 */
	{
		BoundaryExtents extents;

		h3_set_boundary_extents(h3set, numHexes, &extents);
		if (extents.hasWest
			&& extents.hasEast
			&& (extents.maxLat > degsToRads(82.0)
				|| extents.minLat < degsToRads(-82.0)))
		{
			CellBoundary boundary;

			build_extent_boundary(&boundary, &extents);
			wkb = boundary_to_wkb(&boundary);
			destroyLinkedMultiPolygon(linkedPolygon);
			pfree(linkedPolygon);
			pfree(h3set);
			PG_RETURN_BYTEA_P(wkb);
		}
	}

	/*
	 * Example: crossed low-resolution north-polar single-shell output can
	 * collapse to a tiny planar loop after split normalization. Rebuild that
	 * class from exact split cell boundaries instead of trusting direct H3
	 * output.
	 */
	if (resolution <= 1
		&& is_linked_polygon_crossed_by_180(linkedPolygon)
		&& linked_polygon_is_north_polar_single_self_touch(linkedPolygon))
	{
		wkb = coarse_cells_to_multi_polygon_wkb(h3set, numHexes, resolution);
		destroyLinkedMultiPolygon(linkedPolygon);
		pfree(linkedPolygon);
		pfree(h3set);
		PG_RETURN_BYTEA_P(wkb);
	}

	if (resolution <= 2)
	{
		LinkedGeoPolygon *preparedPolygon = prepare_linked_polygon_for_wkb(linkedPolygon, numHexes, resolution);
		bool northPolarFragments = false;

		if (resolution <= 1 && preparedPolygon && preparedPolygon->next)
		{
			double maxLat = -DBL_MAX;

			FOREACH_LINKED_POLYGON(preparedPolygon, polygon)
			{
				FOREACH_LINKED_LOOP(polygon, loop)
				{
					FOREACH_LINKED_LAT_LNG(loop, latlng)
					{
						if (latlng->vertex.lat > maxLat)
							maxLat = latlng->vertex.lat;
					}
				}
			}

			northPolarFragments = maxLat > (M_PI / 3.0);
		}

		/*
		 * Example: z=2/x=1/y=3 can normalize into a tiny south-polar patch, while
		 * direct low-resolution north-polar preparation can return several
		 * fragments. Reject both classes before serialization and rebuild them
		 * from exact split boundaries instead.
		 */
		if (!preparedPolygon
			|| northPolarFragments
			|| !prepared_polygon_matches_h3set_extents(preparedPolygon, h3set, numHexes))
		{
			if (preparedPolygon)
				free_linked_geo_polygon(preparedPolygon);
			wkb = coarse_cells_to_multi_polygon_wkb(h3set, numHexes, resolution);
			destroyLinkedMultiPolygon(linkedPolygon);
			pfree(linkedPolygon);
			pfree(h3set);
			PG_RETURN_BYTEA_P(wkb);
		}

		destroyLinkedMultiPolygon(linkedPolygon);
		pfree(linkedPolygon);
		linkedPolygon = preparedPolygon;
		localLinkedPolygon = true;
	}
	else if (is_linked_polygon_crossed_by_180(linkedPolygon))
	{
		/*
		 * Higher-resolution crossed output is usually salvageable by splitting at
		 * the antimeridian and then continuing through the normal WKB path.
		 */
		LinkedGeoPolygon *splitPolygon = split_linked_polygon_by_180(linkedPolygon);

		destroyLinkedMultiPolygon(linkedPolygon);
		pfree(linkedPolygon);
		linkedPolygon = splitPolygon;
		localLinkedPolygon = true;
	}

	if (numHexes > 0
		&& !linkedPolygon->first
		&& !linkedPolygon->next)
		wkb = boundary_to_wkb(&FULL_WORLD_BOUNDARY);
	else
	{
		linked_geo_polygon_to_degs(linkedPolygon);
		wkb = linked_geo_polygon_to_wkb(linkedPolygon);
	}
	if (localLinkedPolygon)
		free_linked_geo_polygon(linkedPolygon);
	else
	{
		destroyLinkedMultiPolygon(linkedPolygon);
		pfree(linkedPolygon);
	}
	pfree(h3set);

	PG_RETURN_BYTEA_P(wkb);
}

void
linked_geo_polygon_to_degs(LinkedGeoPolygon * multiPolygon)
{
	FOREACH_LINKED_POLYGON_NOCONST(multiPolygon, polygon)
	{
		FOREACH_LINKED_LOOP_NOCONST(polygon, loop)
		{
			FOREACH_LINKED_LAT_LNG_NOCONST(loop, latlng)
			{
				LatLng	   *vertex = &latlng->vertex;

				vertex->lat = radsToDegs(vertex->lat);
				vertex->lng = radsToDegs(vertex->lng);
			}
		}
	}
}

/*
 * Prepare released-H3 or split-boundary polygon output for planar WKB.
 *
 * Example: crossed low-res polar output may need split/merge/normalize before
 * it is safe to serialize as a PostGIS geometry.
 */
LinkedGeoPolygon *
prepare_linked_polygon_for_wkb(const LinkedGeoPolygon * linkedPolygon, int numHexes, int resolution)
{
	if (is_linked_polygon_crossed_by_180(linkedPolygon))
	{
		LinkedGeoPolygon *preparedPolygon = split_linked_polygon_by_180(linkedPolygon);
		LinkedGeoPolygon *mergedPolygon = NULL;
		LinkedGeoPolygon *normalizedPolygon;

		/*
		 * Example: for a single crossed shell we try the narrow merge path first,
		 * except for the known crossed low-res north-polar self-touch class.
		 */
		if (!linkedPolygon->next
			&& !linked_polygon_is_north_polar_single_self_touch(linkedPolygon))
			mergedPolygon = merge_split_linked_polygons(preparedPolygon, resolution);
		if (mergedPolygon)
		{
			if (!(numHexes > 0
				&& !mergedPolygon->first
				&& !mergedPolygon->next))
			{
				free_linked_geo_polygon(preparedPolygon);
				preparedPolygon = mergedPolygon;
			}
			else
			{
				free_linked_geo_polygon(mergedPolygon);
			}
		}

		normalizedPolygon = normalize_split_linked_polygon(preparedPolygon);
		if (normalizedPolygon)
		{
			free_linked_geo_polygon(preparedPolygon);
			preparedPolygon = normalizedPolygon;

			/*
			 * Example: coarse crossed output can still normalize into adjacent seam
			 * shards, so low-res crossed cases get a few conservative remerge passes.
			 */
			if (resolution <= 1 && preparedPolygon->next)
				maybe_remerge_split_linked_polygons(&preparedPolygon, resolution);
		}

		if (numHexes > 0
			&& !preparedPolygon->first
			&& !preparedPolygon->next)
		{
			free_linked_geo_polygon(preparedPolygon);
			return palloc0(sizeof(*preparedPolygon));
		}

		return preparedPolygon;
	}

	{
		LinkedGeoPolygon *normalizedPolygon = normalize_split_linked_polygon(linkedPolygon);

		/*
		 * Example: non-crossed boundary-rebuilt low-res output can still contain
		 * seam-separated siblings after normalization, so collapse them here.
		 */
		if (normalizedPolygon
			&& resolution <= 1
			&& normalizedPolygon->next)
			maybe_remerge_split_linked_polygons(&normalizedPolygon, resolution);

		return normalizedPolygon;
	}
}

/*
 * Detect the exact res0 globe cover so we can emit the world rectangle
 * directly instead of trying to serialize a boundary that does not exist.
 */
bool
cell_set_is_full_globe(const H3Index * h3set, int numHexes)
{
	H3Index    *compacted;
	H3Index    *res0;
	int			resolution;
	int			compactedCount = 0;
	int64_t		numRes0 = 0;

	if (numHexes <= 0 || !h3set[0])
		return false;

	resolution = H3_EXPORT(getResolution)(h3set[0]);
	for (int i = 1; i < numHexes; i++)
	{
		if (!h3set[i] || H3_EXPORT(getResolution)(h3set[i]) != resolution)
			return false;
	}

	h3_assert(H3_EXPORT(getNumCells)(0, &numRes0));
	compacted = palloc0(numHexes * sizeof(*compacted));
	res0 = palloc(numRes0 * sizeof(*res0));

	/*
	 * "Full globe" here means "compacts exactly to the canonical res0 globe",
	 * not merely "covers a very large area".
	 */
	if (H3_EXPORT(compactCells)(h3set, compacted, numHexes))
	{
		pfree(compacted);
		pfree(res0);
		return false;
	}

	h3_assert(H3_EXPORT(getRes0Cells)(res0));
	for (int i = 0; i < numHexes; i++)
	{
		if (compacted[i])
			compactedCount++;
	}
	if (compactedCount != numRes0)
	{
		pfree(compacted);
		pfree(res0);
		return false;
	}

	for (int i = 0; i < numRes0; i++)
	{
		bool found = false;

		for (int j = 0; j < numHexes; j++)
		{
			if (compacted[j] == res0[i])
			{
				found = true;
				break;
			}
		}
		if (!found)
		{
			pfree(compacted);
			pfree(res0);
			return false;
		}
	}

	pfree(compacted);
	pfree(res0);
	return true;
}

/*
 * Rebuild a planar multipolygon directly from split cell boundaries when the
 * released H3 multipolygon path fails or low-res output is unusable.
 *
 * Example: low-res seam-heavy world-edge tiles can reject direct output but
 * still serialize correctly from exact split cell boundaries.
 */
bytea *
coarse_cells_to_multi_polygon_wkb(const H3Index * h3set, int numHexes, int resolution)
{
	LinkedGeoPolygon *cellsPolygon = NULL;
	LinkedGeoPolygon *lastPolygon = NULL;
	LinkedGeoPolygon *preparedPolygon;
	bytea	   *wkb;

	for (int i = 0; i < numHexes; i++)
	{
		SplitCellBoundary splitBoundary;

		split_cell_boundary(h3set[i], &splitBoundary);
		for (int part = 0; part < splitBoundary.numParts; part++)
			append_boundary_polygon(&cellsPolygon, &lastPolygon, &splitBoundary.parts[part]);
	}

	preparedPolygon = prepare_linked_polygon_for_wkb(cellsPolygon, numHexes, resolution);
	if (preparedPolygon)
	{
		maybe_remerge_split_linked_polygons(&preparedPolygon, resolution);
		if (numHexes > 0
			&& !preparedPolygon->first
			&& !preparedPolygon->next)
		{
			free_linked_geo_polygon(preparedPolygon);
			preparedPolygon = NULL;
		}
	}
	if (!preparedPolygon)
	{
		/*
		 * Example: if direct preparation still collapses to an empty world-sized
		 * placeholder, rebuild from split boundaries before final serialization.
		 */
		preparedPolygon = merge_split_linked_polygons(cellsPolygon, resolution);
		if (!preparedPolygon)
			preparedPolygon = normalize_split_linked_polygon(cellsPolygon);
		if (preparedPolygon)
			maybe_remerge_split_linked_polygons(&preparedPolygon, resolution);
	}
	free_linked_geo_polygon(cellsPolygon);
	if (!preparedPolygon)
		preparedPolygon = palloc0(sizeof(*preparedPolygon));

	if (resolution <= 2
		&& !prepared_polygon_matches_h3set_extents(preparedPolygon, h3set, numHexes))
	{
		/* Same class as the low-res direct path: shrunken cap/extent output. */
		BoundaryExtents extents;
		CellBoundary boundary;

		h3_set_boundary_extents(h3set, numHexes, &extents);
		build_extent_boundary(&boundary, &extents);
		wkb = boundary_to_wkb(&boundary);
		free_linked_geo_polygon(preparedPolygon);
		return wkb;
	}

	if (numHexes > 0
		&& !preparedPolygon->first
		&& !preparedPolygon->next)
		wkb = boundary_to_wkb(&FULL_WORLD_BOUNDARY);
	else
	{
		linked_geo_polygon_to_degs(preparedPolygon);
		wkb = linked_geo_polygon_to_wkb(preparedPolygon);
	}
	free_linked_geo_polygon(preparedPolygon);
	return wkb;
}

/*
 * Rebuild one planar union from already split polygons that still overlap or
 * touch along the seam.
 */
LinkedGeoPolygon *
merge_split_linked_polygons(const LinkedGeoPolygon * multiPolygon, int resolution)
{
	LinkedGeoPolygon *unioned = rebuild_noded_polygon_union(multiPolygon, resolution);
	LinkedGeoPolygon *result = normalize_split_linked_polygon(unioned);

	free_linked_geo_polygon(unioned);
	return result;
}

/*
 * Polygonize a noded split-boundary graph and keep only faces that belong to
 * the original union.
 */
LinkedGeoPolygon *
rebuild_noded_polygon_union(const LinkedGeoPolygon * multiPolygon, int resolution)
{
	LinkedGeoPolygon *pieces;
	LinkedGeoPolygon *kept = NULL;
	LinkedGeoPolygon *lastKept = NULL;
	LinkedGeoPolygon *result;

	pieces = polygonize_noded_linked_polygon(multiPolygon, resolution);
	if (resolution <= 1)
	{
		LinkedGeoPolygon *cleaned = pieces ? pieces : palloc0(sizeof(*pieces));

		cleaned = finalize_prepared_polygon(cleaned);
		return cleaned ? cleaned : palloc0(sizeof(*cleaned));
	}

	FOREACH_LINKED_POLYGON(pieces, polygon)
	{
		if (!polygon->first
			|| !polygon_probe_in_union(multiPolygon, polygon))
		{
			continue;
		}

		{
			LinkedGeoPolygon *copy = copy_linked_geo_polygon(polygon);

			append_polygon_chain(&kept, &lastKept, copy);
		}
	}
	free_linked_geo_polygon(pieces);
	if (!kept)
		return palloc0(sizeof(*kept));

	result = polygonize_noded_linked_polygon(kept, resolution);
	free_linked_geo_polygon(kept);
	return result ? result : palloc0(sizeof(*result));
}

/*
 * Give low-res seam-sharded output a few conservative remerge passes.
 *
 * Example: a crossed polar shell can normalize into adjacent seam siblings
 * that should collapse back into one polygon before serialization.
 */
void
maybe_remerge_split_linked_polygons(LinkedGeoPolygon ** multiPolygon, int resolution)
{
	if (!multiPolygon || !*multiPolygon)
		return;

	for (int i = 0; i < 3 && (*multiPolygon)->next; i++)
	{
		LinkedGeoPolygon *remerged = merge_split_linked_polygons(*multiPolygon, resolution);

		if (!remerged)
			return;

		free_linked_geo_polygon(*multiPolygon);
		*multiPolygon = remerged;
	}
}

LinkedGeoPolygon *
normalize_split_linked_polygon(const LinkedGeoPolygon * multiPolygon)
{
	LinkedGeoPolygon flattened = {0};
	LinkedGeoPolygon *result;
	const LinkedGeoLoop **loops;
	LoopBounds *bounds;
	LatLng	   *probes;
	double	   *areas;
	int		   *parents;
	int		   *depths;
	LinkedGeoPolygon **polygons;
	int			loopCount = 0;
	int			initialLoopCount = 0;

	/*
	 * This function turns a bag of split loops into a normalized planar
	 * multipolygon:
	 * 1. drop duplicate/equivalent loops
	 * 2. derive parent/child shell-hole relationships from probes
	 * 3. rebuild polygons from even/odd nesting depth
	 * 4. clean up spike/self-touch artifacts introduced by planar splitting
	 */
	collect_unique_split_loops(&flattened, multiPolygon);
	if (!flattened.first)
		return palloc0(sizeof(*result));

	initialLoopCount = count_linked_geo_loops(&flattened);
	if (initialLoopCount == 1 && !normalizeMultiPolygon(&flattened))
	{
		result = copy_linked_multi_polygon(&flattened);
		destroyLinkedMultiPolygon(&flattened);
		return finalize_prepared_polygon(result);
	}

	destroyLinkedMultiPolygon(&flattened);
	flattened = (LinkedGeoPolygon) {0};
	collect_unique_split_loops(&flattened, multiPolygon);
	loopCount = count_linked_geo_loops(&flattened);
	loops = palloc(loopCount * sizeof(*loops));
	bounds = palloc(loopCount * sizeof(*bounds));
	probes = palloc(loopCount * sizeof(*probes));
	areas = palloc(loopCount * sizeof(*areas));
	parents = palloc(loopCount * sizeof(*parents));
	depths = palloc(loopCount * sizeof(*depths));
	polygons = palloc0(loopCount * sizeof(*polygons));

	{
		int			loopIdx = 0;

		for (const LinkedGeoLoop *loop = flattened.first; loop; loop = loop->next)
		{
			loops[loopIdx] = loop;
			loop_bounds(loop, &bounds[loopIdx]);
			probes[loopIdx] = loop_probe_point(loop, &bounds[loopIdx]);
			areas[loopIdx] = fabs(linked_geo_loop_signed_area(loop));
			loopIdx++;
		}
	}
	for (int i = 0; i < loopCount; i++)
		parents[i] = find_split_loop_parent(loops, bounds, probes, areas, loopCount, i);

	for (int i = 0; i < loopCount; i++)
	{
		int			guard = 0;

		depths[i] = 0;
		for (int parent = parents[i]; parent >= 0; parent = parents[parent])
		{
			if (++guard > loopCount)
			{
				parents[i] = -1;
				depths[i] = 0;
				break;
			}
			depths[i]++;
		}
	}
	result = NULL;
	for (int i = 0; i < loopCount; i++)
	{
		LinkedGeoPolygon *polygonCopy;

		if (depths[i] % 2 != 0)
			continue;

		polygonCopy = palloc0(sizeof(*polygonCopy));
		add_linked_geo_loop(polygonCopy, copy_linked_geo_loop(loops[i]));
		polygons[i] = polygonCopy;
		append_polygon_chain(&result, NULL, polygonCopy);
	}
	for (int i = 0; i < loopCount; i++)
	{
		int			parent;

		if (depths[i] % 2 == 0)
			continue;

		parent = parents[i];
		if (parent < 0 || !polygons[parent])
			continue;

		add_linked_geo_loop(polygons[parent], copy_linked_geo_loop(loops[i]));
	}
	pfree(loops);
	pfree(bounds);
	pfree(probes);
	pfree(areas);
	pfree(parents);
	pfree(depths);
	pfree(polygons);
	destroyLinkedMultiPolygon(&flattened);
	result = finalize_prepared_polygon(result);

	return result ? result : palloc0(sizeof(*result));
}

/* Copy one loop from an H3 linked polygon into a local linked polygon. */
void
append_h3_loop_copy(LinkedGeoPolygon * polygon, const LinkedGeoLoop * loop)
{
	LinkedGeoLoop *copy = addNewLinkedLoop(polygon);

	FOREACH_LINKED_LAT_LNG(loop, latlng)
		addLinkedCoord(copy, &latlng->vertex);
}

/* Drop duplicate split loops before nesting and polygon reconstruction. */
static void
collect_unique_split_loops(LinkedGeoPolygon * flattened, const LinkedGeoPolygon * multiPolygon)
{
	FOREACH_LINKED_POLYGON(multiPolygon, polygon)
	{
		FOREACH_LINKED_LOOP(polygon, loop)
		{
			if (count_linked_lat_lng(loop) >= 3
				&& !linked_polygon_has_equivalent_loop(flattened, loop))
				append_h3_loop_copy(flattened, loop);
		}
	}
}

/* Append one polygon or polygon chain while keeping an optional tail pointer. */
static void
append_polygon_chain(LinkedGeoPolygon ** result, LinkedGeoPolygon ** last, LinkedGeoPolygon * polygon)
{
	LinkedGeoPolygon **tail = last;

	if (!polygon)
		return;

	if (!*result)
	{
		*result = polygon;
	}
	else
	{
		LinkedGeoPolygon *cursor = (tail && *tail) ? *tail : *result;

		while (cursor->next)
			cursor = cursor->next;
		cursor->next = polygon;
	}

	if (tail)
	{
		LinkedGeoPolygon *cursor = polygon;

		while (cursor->next)
			cursor = cursor->next;
		*tail = cursor;
	}
}

/* Compare two loops modulo rotation and direction after split-time rounding. */
bool
linked_geo_loops_equal(const LinkedGeoLoop * a, const LinkedGeoLoop * b)
{
	LatLng	   *vertsA;
	LatLng	   *vertsB;
	int			countA = copy_linked_geo_loop_vertices(a, &vertsA);
	int			countB = copy_linked_geo_loop_vertices(b, &vertsB);

	if (countA != countB)
	{
		pfree(vertsB);
		pfree(vertsA);
		return false;
	}
	if (countA < 3)
	{
		pfree(vertsB);
		pfree(vertsA);
		return true;
	}

	for (int start = 0; start < countB; start++)
	{
		int			i;

		for (i = 0; i < countA; i++)
		{
			int			j = (start + i) % countB;

			if (!geoAlmostEqual(&vertsA[i], &vertsB[j]))
				break;
		}
		if (i == countA)
		{
			pfree(vertsB);
			pfree(vertsA);
			return true;
		}

		for (i = 0; i < countA; i++)
		{
			int			j = (start - i + countB) % countB;

			if (!geoAlmostEqual(&vertsA[i], &vertsB[j]))
				break;
		}
		if (i == countA)
		{
			pfree(vertsB);
			pfree(vertsA);
			return true;
		}
	}

	pfree(vertsB);
	pfree(vertsA);
	return false;
}

static int
copy_linked_geo_loop_vertices(const LinkedGeoLoop * loop, LatLng ** verts)
{
	int	count = count_linked_lat_lng(loop);
	int	idx = 0;

	*verts = palloc(count * sizeof(**verts));
	FOREACH_LINKED_LAT_LNG(loop, latlng)
		(*verts)[idx++] = latlng->vertex;

	return count;
}

/* Check whether a flattened polygon already contains an equivalent split loop. */
bool
linked_polygon_has_equivalent_loop(const LinkedGeoPolygon * polygon, const LinkedGeoLoop * loop)
{
	if (!polygon)
		return false;

	FOREACH_LINKED_LOOP(polygon, existing)
	{
		if (linked_geo_loops_equal(existing, loop))
			return true;
	}

	return false;
}

/* Reject parent/child relationships between loops that only touch at a vertex. */
bool
linked_geo_loops_share_vertex(const LinkedGeoLoop * a, const LinkedGeoLoop * b)
{
	if (!a || !b)
		return false;

	FOREACH_LINKED_LAT_LNG(a, left)
	{
		FOREACH_LINKED_LAT_LNG(b, right)
		{
			if (geoAlmostEqual(&left->vertex, &right->vertex))
				return true;
		}
	}

	return false;
}

bool
linked_geo_loop_has_repeat_vertex(const LinkedGeoLoop * loop)
{
	LatLng	   *verts;
	int			count = copy_linked_geo_loop_vertices(loop, &verts);

	if (count < 4)
	{
		pfree(verts);
		return false;
	}

	for (int i = 0; i < count; i++)
	{
		for (int j = i + 2; j < count; j++)
		{
			if (i == 0 && j == count - 1)
				continue;
			if (geoAlmostEqual(&verts[i], &verts[j]))
			{
				pfree(verts);
				return true;
			}
		}
	}

	pfree(verts);
	return false;
}

/* Remove A-B-A spike vertices created by planar seam splitting. */
void
prune_polygon_spikes(LinkedGeoPolygon * multiPolygon)
{
	FOREACH_LINKED_POLYGON_NOCONST(multiPolygon, polygon)
	{
		FOREACH_LINKED_LOOP_NOCONST(polygon, loop)
			prune_linked_geo_loop_spikes(loop);
	}
}

/* Split high-latitude self-touching shells into separate polygon components. */
LinkedGeoPolygon *
split_self_touching_components(const LinkedGeoPolygon * multiPolygon)
{
	LinkedGeoPolygon *result = NULL;
	LinkedGeoPolygon *last = NULL;

	FOREACH_LINKED_POLYGON(multiPolygon, polygon)
	{
		LoopBounds bounds;

		if (!polygon->first
			|| polygon->first->next
			|| !linked_geo_loop_has_repeat_vertex(polygon->first))
		{
			append_polygon_chain(&result, &last, copy_linked_geo_polygon(polygon));
			continue;
		}

		loop_bounds(polygon->first, &bounds);
		if (bounds.maxLat < (M_PI / 3.0)
			&& bounds.minLat > (-M_PI / 3.0))
		{
			append_polygon_chain(&result, &last, copy_linked_geo_polygon(polygon));
			continue;
		}

		{
			LinkedGeoPolygon singleton = {0};
			LinkedGeoPolygon *split;

			singleton.first = polygon->first;
			singleton.last = polygon->last;
			split = split_self_touching_polygons(&singleton);
			prune_polygon_spikes(split);
			append_polygon_chain(&result, &last, split);
		}
	}

	return result ? result : palloc0(sizeof(*result));
}

/* Run the one shared cleanup pipeline before final planar serialization. */
static LinkedGeoPolygon *
finalize_prepared_polygon(LinkedGeoPolygon * multiPolygon)
{
	LinkedGeoPolygon *split;

	/* Run the one shared post-normalization cleanup pipeline in one place. */
	prune_polygon_spikes(multiPolygon);
	split = split_self_touching_components(multiPolygon);
	free_linked_geo_polygon(multiPolygon);
	multiPolygon = split;
	maybe_split_single_self_touching_loop(&multiPolygon);

	return multiPolygon;
}

void
maybe_split_single_self_touching_loop(LinkedGeoPolygon ** multiPolygon)
{
	LinkedGeoPolygon *split;
	LoopBounds	bounds;

	if (!multiPolygon
		|| !*multiPolygon
		|| (*multiPolygon)->next
		|| !(*multiPolygon)->first
		|| (*multiPolygon)->first->next
		|| !linked_geo_loop_has_repeat_vertex((*multiPolygon)->first))
	{
		return;
	}

	loop_bounds((*multiPolygon)->first, &bounds);
	if (bounds.maxLat < (M_PI / 3.0)
		&& bounds.minLat > (-M_PI / 3.0))
	{
		return;
	}

	split = split_self_touching_polygons(*multiPolygon);
	if (!split)
		return;

	prune_polygon_spikes(split);
	if (count_linked_polygons(split) == 1
		&& split->first
		&& !split->next
		&& count_linked_geo_loops(split) == 1
		&& !linked_geo_loop_has_repeat_vertex(split->first))
	{
		free_linked_geo_polygon(*multiPolygon);
		*multiPolygon = split;
		return;
	}

	free_linked_geo_polygon(split);
}

void
append_polygon_from_vertex_slice(LinkedGeoPolygon ** result, LinkedGeoPolygon ** last, const LatLng * verts, int vertCount)
{
	LinkedGeoPolygon *polygon;
	LinkedGeoLoop *loop;

	if (vertCount < 3)
		return;

	polygon = palloc0(sizeof(*polygon));
	loop = palloc0(sizeof(*loop));
	add_linked_geo_loop(polygon, loop);
	for (int i = 0; i < vertCount; i++)
	{
		LinkedLatLng *latlng = palloc0(sizeof(*latlng));

		latlng->vertex = verts[i];
		add_linked_lat_lng(loop, latlng);
	}

	prune_linked_geo_loop_spikes(loop);
	if (count_linked_lat_lng(loop) < 3
		|| linked_geo_loop_signed_area(loop) <= DBL_EPSILON)
	{
		free_linked_geo_polygon(polygon);
		return;
	}

	append_polygon_chain(result, last, polygon);
}

LinkedGeoPolygon *
copy_linked_multi_polygon(const LinkedGeoPolygon * multiPolygon)
{
	LinkedGeoPolygon *copy = NULL;
	LinkedGeoPolygon *last = NULL;

	FOREACH_LINKED_POLYGON(multiPolygon, polygon)
	{
		LinkedGeoPolygon *polygonCopy = copy_linked_geo_polygon(polygon);

		append_polygon_chain(&copy, &last, polygonCopy);
	}

	return copy ? copy : palloc0(sizeof(*copy));
}

LinkedGeoPolygon *
split_self_touching_polygons(const LinkedGeoPolygon * multiPolygon)
{
	LinkedGeoPolygon *result = NULL;
	LinkedGeoPolygon *last = NULL;

	FOREACH_LINKED_POLYGON(multiPolygon, polygon)
	{
		FOREACH_LINKED_LOOP(polygon, loop)
		{
			int			vertCount = count_linked_lat_lng(loop);
			LatLng	   *verts;
			LatLng	   *stack;
			int			stackCount = 0;
			int			idx = 0;

			if (vertCount < 3)
				continue;

			verts = palloc(vertCount * sizeof(*verts));
			stack = palloc(vertCount * sizeof(*stack));
			FOREACH_LINKED_LAT_LNG(loop, latlng)
				verts[idx++] = latlng->vertex;

			for (int i = 0; i < vertCount; i++)
			{
				int			repeatIdx = -1;

				for (int j = 0; j < stackCount; j++)
				{
					if (geoAlmostEqual(&stack[j], &verts[i]))
					{
						repeatIdx = j;
						break;
					}
				}

				if (repeatIdx >= 0)
				{
					append_polygon_from_vertex_slice(
						&result,
						&last,
						stack + repeatIdx,
						stackCount - repeatIdx);
					stackCount = repeatIdx + 1;
					continue;
				}

				stack[stackCount++] = verts[i];
			}

			append_polygon_from_vertex_slice(&result, &last, stack, stackCount);
			pfree(stack);
			pfree(verts);
		}
	}

	return result ? result : palloc0(sizeof(*result));
}

/* Pick the smallest containing split loop as the shell parent for one loop. */
int
find_split_loop_parent(const LinkedGeoLoop ** loops, const LoopBounds * bounds, const LatLng * probes, const double *areas, int loopCount, int loopIdx)
{
	int			parent = -1;

	for (int i = 0; i < loopCount; i++)
	{
		if (i == loopIdx)
			continue;
		if (linked_geo_loops_share_vertex(loops[i], loops[loopIdx]))
			continue;
		if (areas && areas[i] <= areas[loopIdx] + DBL_EPSILON)
			continue;
		if (!point_inside_split_loop(loops[i], &bounds[i], &probes[loopIdx]))
			continue;
		if (parent < 0
			|| point_inside_split_loop(loops[parent], &bounds[parent], &probes[i]))
		{
			parent = i;
		}
	}
	return parent;
}

/* Wrap one split cell boundary part as a single-loop polygon. */
void
append_boundary_polygon(LinkedGeoPolygon ** multiPolygon, LinkedGeoPolygon ** lastPolygon, const CellBoundary * boundary)
{
	LinkedGeoPolygon *polygon;
	LinkedGeoLoop *loop;

	if (!boundary || boundary->numVerts < 3)
		return;

	polygon = palloc0(sizeof(*polygon));
	loop = palloc0(sizeof(*loop));
	add_linked_geo_loop(polygon, loop);
	for (int i = 0; i < boundary->numVerts; i++)
	{
		LinkedLatLng *latlng = palloc0(sizeof(*latlng));

		latlng->vertex = boundary->verts[i];
		add_linked_lat_lng(loop, latlng);
	}

	append_polygon_chain(multiPolygon, lastPolygon, polygon);
}

LatLng
loop_probe_point(const LinkedGeoLoop * loop, const LoopBounds * bounds)
{
	LatLng		probe = {0};
	int			vertexCount = count_linked_lat_lng(loop);
	double	   *lats = palloc(vertexCount * sizeof(*lats));
	double	   *ringLats = palloc(vertexCount * sizeof(*ringLats));
	double	   *lngs = palloc(vertexCount * sizeof(*lngs));
	double	   *xs = palloc(vertexCount * sizeof(*xs));
	int			latCount = 0;
	double		bestWidth = -1.0;
	double		minLng = 0.0;
	double		maxLng = 0.0;
	int			idx = 0;

	if (bounds->maxLng - bounds->minLng > M_PI)
	{
		LatLng		polarProbe = {
			.lat = bounds->maxLat > (M_PI / 3.0)
				? fmin(bounds->maxLat - 1e-6, M_PI_2 - 1e-6)
				: fmax(bounds->minLat + 1e-6, -M_PI_2 + 1e-6),
			.lng = bounds->wrapLng
		};

		if (bounds->maxLat > (M_PI / 3.0) || bounds->minLat < (-M_PI / 3.0))
			return polarProbe;
	}

	FOREACH_LINKED_LAT_LNG(loop, latlng)
	{
		ringLats[idx] = latlng->vertex.lat;
		lats[latCount++] = ringLats[idx];
		if (idx == 0)
			lngs[idx] = latlng->vertex.lng;
		else
			lngs[idx] = normalize_lng_around(latlng->vertex.lng, lngs[idx - 1]);
		if (idx == 0 || lngs[idx] < minLng)
			minLng = lngs[idx];
		if (idx == 0 || lngs[idx] > maxLng)
			maxLng = lngs[idx];
		idx++;
	}

	latCount = sort_unique_latitudes(lats, latCount);
	for (int i = 0; i + 1 < latCount; i++)
	{
		double		y1 = lats[i];
		double		y2 = lats[i + 1];
		double		y;
		int			xCount = 0;

		if (y2 - y1 <= DBL_EPSILON)
			continue;

		y = 0.5 * (y1 + y2);
		for (int j = 0; j < vertexCount; j++)
		{
			double		curLat = ringLats[j];
			double		nextLat = ringLats[(j + 1) % vertexCount];
			double		curLng;
			double		nextLng;
			curLng = lngs[j];
			nextLng = lngs[(j + 1) % vertexCount];

			if (!((curLat <= y && y < nextLat)
					|| (nextLat <= y && y < curLat)))
			{
				continue;
			}

			xs[xCount++] = curLng
				+ (y - curLat) * (nextLng - curLng)
				/ (nextLat - curLat);
		}

		if (xCount < 2)
			continue;

		qsort(xs, xCount, sizeof(*xs), double_cmp);
		for (int j = 0; j + 1 < xCount; j += 2)
		{
			double		width = xs[j + 1] - xs[j];

			if (width > bestWidth + DBL_EPSILON)
			{
				bestWidth = width;
				probe.lat = y;
				probe.lng = 0.5 * (xs[j] + xs[j + 1]);
			}
		}
	}

	if (bestWidth < 0.0)
	{
		probe.lat = 0.5 * (bounds->minLat + bounds->maxLat);
		probe.lng = 0.5 * (minLng + maxLng);
	}

	pfree(xs);
	pfree(lngs);
	pfree(ringLats);
	pfree(lats);
	return probe;
}

void
loop_bounds(const LinkedGeoLoop * loop, LoopBounds * bounds)
{
	const LinkedLatLng *latlng = loop->first;
	double		prevLng;

	bounds->minLat = bounds->maxLat = latlng->vertex.lat;
	bounds->minLng = bounds->maxLng = latlng->vertex.lng;
	prevLng = latlng->vertex.lng;

	for (const LinkedLatLng *cur = loop->first; cur; cur = cur->next)
	{
		double		lng = (cur == loop->first)
			? cur->vertex.lng
			: normalize_lng_around(cur->vertex.lng, prevLng);

		if (cur->vertex.lat < bounds->minLat)
			bounds->minLat = cur->vertex.lat;
		if (cur->vertex.lat > bounds->maxLat)
			bounds->maxLat = cur->vertex.lat;
		if (lng < bounds->minLng)
			bounds->minLng = lng;
		if (lng > bounds->maxLng)
			bounds->maxLng = lng;
		prevLng = lng;
	}

	bounds->wrapLng = 0.5 * (bounds->minLng + bounds->maxLng);
}

bool
point_inside_split_loop(const LinkedGeoLoop * loop, const LoopBounds * bounds, const LatLng * probe)
{
	bool		inside = false;
	double		x = normalize_lng_around(probe->lng, bounds->wrapLng);
	double		y = probe->lat;

	if (y < bounds->minLat || y > bounds->maxLat)
		return false;
	if (x < bounds->minLng || x > bounds->maxLng)
		return false;

	FOREACH_LINKED_LAT_LNG_PAIR(loop, cur, next)
	{
		double		x1 = normalize_lng_around(cur->vertex.lng, bounds->wrapLng);
		double		y1 = cur->vertex.lat;
		double		x2 = normalize_lng_around(next->vertex.lng, bounds->wrapLng);
		double		y2 = next->vertex.lat;

		if (((y1 > y) != (y2 > y))
			&& (x < (x2 - x1) * (y - y1) / (y2 - y1) + x1))
		{
			inside = !inside;
		}
	}

	return inside;
}

/* Split one cell boundary into the planar parts used by coarse rebuilding. */
void
split_cell_boundary(H3Index cell, SplitCellBoundary * splitBoundary)
{
	CellBoundary boundary;
	int			crossNum;

	h3_assert(cellToBoundary(cell, &boundary));
	crossNum = boundary_crosses_180_num(&boundary);
	if (crossNum == 0)
	{
		splitBoundary->numParts = 1;
		splitBoundary->parts[0] = boundary;
	}
	else if (crossNum == 1)
	{
		splitBoundary->numParts = 1;
		boundary_split_180_polar(&boundary, &splitBoundary->parts[0]);
	}
	else
	{
		splitBoundary->numParts = 2;
		boundary_split_180(&boundary, &splitBoundary->parts[0], &splitBoundary->parts[1]);
	}
}

int
sort_unique_latitudes(double *lats, int count)
{
	int			unique = 0;

	if (count == 0)
		return 0;

	qsort(lats, count, sizeof(*lats), double_cmp);
	for (int i = 0; i < count; i++)
	{
		if (unique == 0 || fabs(lats[i] - lats[unique - 1]) >= DBL_EPSILON)
			lats[unique++] = lats[i];
	}

	return unique;
}

int
double_cmp(const void *a, const void *b)
{
	double		da = *((const double *) a);
	double		db = *((const double *) b);

	return (da > db) - (da < db);
}

/* Count closed-loop edges so noding/polygonizing arrays can be sized once. */
int
count_linked_polygon_edges(const LinkedGeoPolygon * multiPolygon)
{
	int			count = 0;

	FOREACH_LINKED_POLYGON(multiPolygon, polygon)
	{
		FOREACH_LINKED_LOOP(polygon, loop)
		{
			const LinkedLatLng *first = loop->first;
			const LinkedLatLng *cur = first;

			if (!first)
				continue;

			do
			{
				count++;
				cur = cur->next ? cur->next : first;
			}
			while (cur != first);
		}
	}

	return count;
}

/* Materialize all split-boundary edges into noded segments with split markers. */
void
collect_linked_polygon_segments(const LinkedGeoPolygon * multiPolygon, NodedSegment * segments, int segmentCount)
{
	int			idx = 0;

	FOREACH_LINKED_POLYGON(multiPolygon, polygon)
	{
		FOREACH_LINKED_LOOP(polygon, loop)
		{
			const LinkedLatLng *first = loop->first;
			const LinkedLatLng *cur = first;

			if (!first)
				continue;

			do
			{
				const LinkedLatLng *next = cur->next ? cur->next : first;
				NodedSegment *segment = &segments[idx++];

				segment->from = cur->vertex;
				segment->to = next->vertex;
				segment->splitCap = segmentCount + 2;
				segment->splitCount = 0;
				segment->splitTs = palloc(segment->splitCap * sizeof(*segment->splitTs));
				segment_add_split_t(segment, 0.0);
				segment_add_split_t(segment, 1.0);

				cur = next;
			}
			while (cur != first);
		}
	}
}

void
segment_add_split_t(NodedSegment * segment, double t)
{
	if (t < 0.0)
		t = 0.0;
	else if (t > 1.0)
		t = 1.0;

	for (int i = 0; i < segment->splitCount; i++)
	{
		if (fabs(segment->splitTs[i] - t) < DBL_EPSILON)
			return;
	}

	segment->splitTs[segment->splitCount++] = t;
}

bool
segment_intersection_t(const NodedSegment * a, const NodedSegment * b, double *ta, double *tb)
{
	double		x1 = a->from.lng;
	double		y1 = a->from.lat;
	double		x2 = a->to.lng;
	double		y2 = a->to.lat;
	double		x3 = b->from.lng;
	double		y3 = b->from.lat;
	double		x4 = b->to.lng;
	double		y4 = b->to.lat;
	double		denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
	double		t;
	double		u;

	if (fabs(denom) < DBL_EPSILON)
		return false;

	t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom;
	u = ((x1 - x3) * (y1 - y2) - (y1 - y3) * (x1 - x2)) / denom;
	if (t < -DBL_EPSILON || t > 1.0 + DBL_EPSILON
		|| u < -DBL_EPSILON || u > 1.0 + DBL_EPSILON)
	{
		return false;
	}

	t = fmin(fmax(t, 0.0), 1.0);
	u = fmin(fmax(u, 0.0), 1.0);
	if ((t < DBL_EPSILON || t > 1.0 - DBL_EPSILON)
		&& (u < DBL_EPSILON || u > 1.0 - DBL_EPSILON))
	{
		return false;
	}

	*ta = t;
	*tb = u;
	return true;
}

bool
segment_collinear_overlap_ts(const NodedSegment * a, const NodedSegment * b, double *aStart, double *aEnd, double *bStart, double *bEnd)
{
	double		dx1 = a->to.lng - a->from.lng;
	double		dy1 = a->to.lat - a->from.lat;
	double		dx2 = b->to.lng - b->from.lng;
	double		dy2 = b->to.lat - b->from.lat;
	double		denom = dx1 * dy2 - dy1 * dx2;
	double		crossFrom;
	double		crossTo;
	double		t0;
	double		t1;
	double		overlapStart;
	double		overlapEnd;
	LatLng		startPoint;
	LatLng		endPoint;

	if (fabs(denom) >= DBL_EPSILON)
		return false;

	crossFrom = (b->from.lng - a->from.lng) * dy1 - (b->from.lat - a->from.lat) * dx1;
	crossTo = (b->to.lng - a->from.lng) * dy1 - (b->to.lat - a->from.lat) * dx1;
	if (fabs(crossFrom) >= DBL_EPSILON || fabs(crossTo) >= DBL_EPSILON)
		return false;

	if (fabs(dx1) >= fabs(dy1))
	{
		if (fabs(dx1) < DBL_EPSILON)
			return false;
		t0 = (b->from.lng - a->from.lng) / dx1;
		t1 = (b->to.lng - a->from.lng) / dx1;
	}
	else
	{
		if (fabs(dy1) < DBL_EPSILON)
			return false;
		t0 = (b->from.lat - a->from.lat) / dy1;
		t1 = (b->to.lat - a->from.lat) / dy1;
	}

	overlapStart = fmax(0.0, fmin(t0, t1));
	overlapEnd = fmin(1.0, fmax(t0, t1));
	if (overlapEnd < overlapStart - DBL_EPSILON)
		return false;

	segment_lat_lng_at(a, overlapStart, &startPoint);
	segment_lat_lng_at(a, overlapEnd, &endPoint);
	*aStart = overlapStart;
	*aEnd = overlapEnd;
	*bStart = segment_project_t(b, &startPoint);
	*bEnd = segment_project_t(b, &endPoint);
	return true;
}

double
segment_project_t(const NodedSegment * segment, const LatLng * latlng)
{
	double		dx = segment->to.lng - segment->from.lng;
	double		dy = segment->to.lat - segment->from.lat;
	double		t;

	if (fabs(dx) >= fabs(dy))
	{
		if (fabs(dx) < DBL_EPSILON)
			return 0.0;
		t = (latlng->lng - segment->from.lng) / dx;
	}
	else
	{
		if (fabs(dy) < DBL_EPSILON)
			return 0.0;
		t = (latlng->lat - segment->from.lat) / dy;
	}

	return fmin(fmax(t, 0.0), 1.0);
}

void
segment_lat_lng_at(const NodedSegment * segment, double t, LatLng * latlng)
{
	latlng->lat = segment->from.lat + (segment->to.lat - segment->from.lat) * t;
	latlng->lng = segment->from.lng + (segment->to.lng - segment->from.lng) * t;
}

/* Add every noded segment piece to the vertex graph used for polygonization. */
void
graph_add_noded_linked_polygon_edges(VertexGraph * graph, const LinkedGeoPolygon * multiPolygon)
{
	int			segmentCount = count_linked_polygon_edges(multiPolygon);
	NodedSegment *segments;

	if (segmentCount <= 0)
		return;

	segments = palloc(segmentCount * sizeof(*segments));
	collect_linked_polygon_segments(multiPolygon, segments, segmentCount);

	for (int i = 0; i < segmentCount; i++)
	{
		for (int j = i + 1; j < segmentCount; j++)
		{
			double		ti;
			double		tj;
			double		overlapStartI;
			double		overlapEndI;
			double		overlapStartJ;
			double		overlapEndJ;

			if (segment_intersection_t(&segments[i], &segments[j], &ti, &tj))
			{
				segment_add_split_t(&segments[i], ti);
				segment_add_split_t(&segments[j], tj);
				continue;
			}

			if (segment_collinear_overlap_ts(
					&segments[i], &segments[j],
					&overlapStartI, &overlapEndI,
					&overlapStartJ, &overlapEndJ))
			{
				segment_add_split_t(&segments[i], overlapStartI);
				segment_add_split_t(&segments[i], overlapEndI);
				segment_add_split_t(&segments[j], overlapStartJ);
				segment_add_split_t(&segments[j], overlapEndJ);
			}
		}
	}

	for (int i = 0; i < segmentCount; i++)
	{
		NodedSegment *segment = &segments[i];

		qsort(segment->splitTs, segment->splitCount, sizeof(*segment->splitTs), double_cmp);
		for (int j = 0; j + 1 < segment->splitCount; j++)
		{
			LatLng		from;
			LatLng		to;
			double		t1 = segment->splitTs[j];
			double		t2 = segment->splitTs[j + 1];

			if (t2 - t1 < DBL_EPSILON)
				continue;

			segment_lat_lng_at(segment, t1, &from);
			segment_lat_lng_at(segment, t2, &to);
			graph_add_edge(graph, &from, &to);
		}

		pfree(segment->splitTs);
	}

	pfree(segments);
}

/* Toggle one directed edge in the graph so shared edges cancel out of the union. */
void
graph_add_edge(VertexGraph * graph, const LatLng * from, const LatLng * to)
{
	VertexNode *reverse = findNodeForEdge(graph, to, from);

	if (reverse)
		removeVertexNode(graph, reverse);
	else
		addVertexNode(graph, from, to);
}

/* Build a vertex graph from split boundaries and polygonize its surviving faces. */
LinkedGeoPolygon *
polygonize_noded_linked_polygon(const LinkedGeoPolygon * multiPolygon, int resolution)
{
	int			edgeCount = count_linked_polygon_edges(multiPolygon);
	VertexGraph graph;
	LinkedGeoPolygon *result;

	if (edgeCount <= 0)
		return palloc0(sizeof(*result));

	initVertexGraph(
		&graph,
		edgeCount * edgeCount,
		resolution >= 0 ? resolution : 0);
	graph_add_noded_linked_polygon_edges(&graph, multiPolygon);
	result = polygonize_noded_graph(&graph);
	destroyVertexGraph(&graph);
	return result;
}

/* Walk the noded graph into raw loops before higher-level normalization. */
LinkedGeoPolygon *
polygonize_noded_graph(const VertexGraph * graph)
{
	PolygonizeHalfEdge *edges;
	PolygonizeVertex *vertices = NULL;
	PolygonizeEdgeRef *edgeRefs;
	int		   *edgeOffsets;
	int			vertexCount = 0;
	int			vertexCap = 0;
	int			edgeIdx = 0;
	int			halfEdgeCount = graph->size * 2;
	LinkedGeoPolygon *raw = NULL;
	LinkedGeoPolygon *lastPolygon = NULL;
	LinkedGeoPolygon *result;

	if (graph->size == 0)
		return palloc0(sizeof(*result));

	edges = palloc0(halfEdgeCount * sizeof(*edges));
	for (int bucketIdx = 0; bucketIdx < graph->numBuckets; bucketIdx++)
	{
		for (VertexNode *node = graph->buckets[bucketIdx]; node; node = node->next)
		{
			double		dLng = normalize_lng_around(node->to.lng, node->from.lng) - node->from.lng;
			int			fromVertex = polygonize_find_or_add_vertex(
				&vertices, &vertexCount, &vertexCap, &node->from);
			int			toVertex = polygonize_find_or_add_vertex(
				&vertices, &vertexCount, &vertexCap, &node->to);

			edges[edgeIdx].from = node->from;
			edges[edgeIdx].to = node->to;
			edges[edgeIdx].fromVertex = fromVertex;
			edges[edgeIdx].toVertex = toVertex;
			edges[edgeIdx].angle = atan2(node->to.lat - node->from.lat, dLng);
			edges[edgeIdx].nextEdge = -1;
			vertices[fromVertex].edgeCount++;
			edgeIdx++;

			edges[edgeIdx].from = node->to;
			edges[edgeIdx].to = node->from;
			edges[edgeIdx].fromVertex = toVertex;
			edges[edgeIdx].toVertex = fromVertex;
			edges[edgeIdx].angle = atan2(node->from.lat - node->to.lat, -dLng);
			edges[edgeIdx].nextEdge = -1;
			vertices[toVertex].edgeCount++;
			edgeIdx++;
		}
	}

	edgeRefs = palloc(edgeIdx * sizeof(*edgeRefs));
	edgeOffsets = palloc(vertexCount * sizeof(*edgeOffsets));
	{
		int			offset = 0;

		for (int i = 0; i < vertexCount; i++)
		{
			vertices[i].firstEdge = offset;
			edgeOffsets[i] = offset;
			offset += vertices[i].edgeCount;
		}
	}
	for (int i = 0; i < edgeIdx; i++)
	{
		int			offset = edgeOffsets[edges[i].fromVertex]++;

		edgeRefs[offset].edgeIdx = i;
		edgeRefs[offset].angle = edges[i].angle;
	}
	for (int i = 0; i < vertexCount; i++)
	{
		if (vertices[i].edgeCount > 1)
		{
			qsort(
				edgeRefs + vertices[i].firstEdge,
				vertices[i].edgeCount,
				sizeof(*edgeRefs),
				polygonize_edge_ref_cmp);
		}
	}

	for (int i = 0; i < edgeIdx; i++)
	{
		const PolygonizeVertex *vertex = &vertices[edges[i].toVertex];
		double		reverseAngle = edges[i].angle + M_PI;
		double		bestDelta = DBL_MAX;
		int			bestEdge = -1;

		while (reverseAngle > M_PI)
			reverseAngle -= 2.0 * M_PI;

		for (int j = 0; j < vertex->edgeCount; j++)
		{
			const PolygonizeEdgeRef *candidate = &edgeRefs[vertex->firstEdge + j];
			double		delta = candidate->angle - reverseAngle;

			while (delta <= 0.0)
				delta += 2.0 * M_PI;
			if (delta < bestDelta)
			{
				bestDelta = delta;
				bestEdge = candidate->edgeIdx;
			}
		}

		if (bestDelta < DBL_MAX)
			edges[i].nextEdge = bestEdge;
	}

	for (int i = 0; i < edgeIdx; i++)
	{
		LinkedGeoPolygon *polygon;
		LinkedGeoLoop *loop;
		int			cur = i;
		int			guard = 0;

		if (edges[i].used || edges[i].nextEdge < 0)
			continue;

		polygon = palloc0(sizeof(*polygon));
		loop = palloc0(sizeof(*loop));
		add_linked_geo_loop(polygon, loop);

		while (cur >= 0 && !edges[cur].used && guard++ <= edgeIdx)
		{
			if (!loop->last || !geoAlmostEqual(&loop->last->vertex, &edges[cur].from))
			{
				LinkedLatLng *latlng = palloc0(sizeof(*latlng));

				latlng->vertex = edges[cur].from;
				add_linked_lat_lng(loop, latlng);
			}

			edges[cur].used = true;
			cur = edges[cur].nextEdge;
			if (cur == i)
				break;
		}

		prune_linked_geo_loop_spikes(loop);
		if (count_linked_lat_lng(loop) < 3
			|| fabs(linked_geo_loop_signed_area(loop)) <= DBL_EPSILON)
		{
			free_linked_geo_polygon(polygon);
			continue;
		}

		if (!raw)
			raw = polygon;
		else
			lastPolygon->next = polygon;
		lastPolygon = polygon;
	}

	pfree(edgeOffsets);
	pfree(edgeRefs);
	pfree(vertices);
	pfree(edges);

	if (!raw)
		return palloc0(sizeof(*raw));

	result = normalize_split_linked_polygon(raw);
	free_linked_geo_polygon(raw);
	return result ? result : palloc0(sizeof(*result));
}

int
polygonize_find_or_add_vertex(PolygonizeVertex **vertices, int *vertexCount, int *vertexCap, const LatLng *vertex)
{
	/*
	 * The polygonizer nodes edges produced from H3-generated or synthesized
	 * split vertices. geoAlmostEqual is intentional here: the split path can
	 * produce numerically equivalent vertices through different arithmetic
	 * routes, and the graph has to merge them to stay connected.
	 */
	for (int i = 0; i < *vertexCount; i++)
	{
		if (geoAlmostEqual(&(*vertices)[i].vertex, vertex))
			return i;
	}

	if (*vertexCount >= *vertexCap)
	{
		*vertexCap = *vertexCap ? *vertexCap * 2 : 32;
		*vertices = *vertices
			? repalloc(*vertices, *vertexCap * sizeof(**vertices))
			: palloc(*vertexCap * sizeof(**vertices));
	}

	(*vertices)[*vertexCount] = (PolygonizeVertex) {
		.vertex = *vertex,
		.firstEdge = 0,
		.edgeCount = 0,
	};
	return (*vertexCount)++;
}

int
polygonize_edge_ref_cmp(const void *a, const void *b)
{
	const PolygonizeEdgeRef *left = a;
	const PolygonizeEdgeRef *right = b;

	if (left->angle < right->angle)
		return -1;
	if (left->angle > right->angle)
		return 1;
	return left->edgeIdx - right->edgeIdx;
}

double
normalize_lng_around(double lng, double around)
{
	while (lng - around > M_PI)
		lng -= 2.0 * M_PI;
	while (lng - around < -M_PI)
		lng += 2.0 * M_PI;

	return lng;
}

double
linked_geo_loop_signed_area(const LinkedGeoLoop * loop)
{
	double		area = 0.0;
	const LinkedLatLng *first = loop->first;
	const LinkedLatLng *cur = first;
	double		curLng;
	double		curLat;

	if (!first)
		return 0.0;

	curLng = first->vertex.lng;
	curLat = first->vertex.lat;
	do
	{
		const LinkedLatLng *next = cur->next ? cur->next : first;
		double		nextLng = normalize_lng_around(next->vertex.lng, curLng);
		double		nextLat = next->vertex.lat;

		area += curLng * nextLat - nextLng * curLat;
		curLng = nextLng;
		curLat = nextLat;
		cur = next;
	}
	while (cur != first);

	return 0.5 * area;
}

/* Keep only polygonized faces whose probe point lies inside the original union. */
bool
polygon_probe_in_union(const LinkedGeoPolygon * multiPolygon, const LinkedGeoPolygon * polygon)
{
	LoopBounds	polygonBounds;
	LatLng		probe;

	if (!polygon || !polygon->first || !polygon->first->first)
		return false;

	loop_bounds(polygon->first, &polygonBounds);
	probe = loop_probe_point(polygon->first, &polygonBounds);
	return point_inside_split_multi_polygon(multiPolygon, &probe);
}

/* Point-in-polygon test on already split planar loops. */
bool
point_inside_split_multi_polygon(const LinkedGeoPolygon * multiPolygon, const LatLng * probe)
{
	bool		inside = false;
	double		y = probe->lat;

	FOREACH_LINKED_POLYGON(multiPolygon, polygon)
	{
		FOREACH_LINKED_LOOP(polygon, loop)
		{
			FOREACH_LINKED_LAT_LNG_PAIR(loop, cur, next)
			{
				double		x1 = normalize_lng_around(cur->vertex.lng, probe->lng);
				double		y1 = cur->vertex.lat;
				double		x2 = normalize_lng_around(next->vertex.lng, probe->lng);
				double		y2 = next->vertex.lat;
				double		x = probe->lng;

				if (((y1 > y) != (y2 > y))
					&& (x < (x2 - x1) * (y - y1) / (y2 - y1) + x1))
				{
					inside = !inside;
				}
			}
		}
	}

	return inside;
}

/* Detect the crossed low-res north-polar single-shell self-touch class. */
bool
linked_polygon_is_north_polar_single_self_touch(const LinkedGeoPolygon * multiPolygon)
{
	LoopBounds bounds;

	if (!multiPolygon
		|| multiPolygon->next
		|| !multiPolygon->first
		|| multiPolygon->first->next)
	{
		return false;
	}

	loop_bounds(multiPolygon->first, &bounds);
	return bounds.maxLat > (M_PI / 3.0);
}


/* Measure exact split-boundary extents for one H3 cell set. */
void
h3_set_boundary_extents(const H3Index * h3set, int numHexes, BoundaryExtents * extents)
{
	bool		init = false;

	extents->hasWest = false;
	extents->hasEast = false;
	for (int i = 0; i < numHexes; i++)
	{
		SplitCellBoundary splitBoundary;

		split_cell_boundary(h3set[i], &splitBoundary);
		for (int part = 0; part < splitBoundary.numParts; part++)
		{
			CellBoundary *boundary = &splitBoundary.parts[part];

			for (int j = 0; j < boundary->numVerts; j++)
			{
				double		lat = boundary->verts[j].lat;
				double		lng = boundary->verts[j].lng;

				if (!init)
				{
					extents->minLat = extents->maxLat = lat;
					extents->minLng = extents->maxLng = lng;
					init = true;
				}
				else
				{
					if (lat < extents->minLat)
						extents->minLat = lat;
					if (lat > extents->maxLat)
						extents->maxLat = lat;
					if (lng < extents->minLng)
						extents->minLng = lng;
					if (lng > extents->maxLng)
						extents->maxLng = lng;
				}

				if (lng < (-5.0 * M_PI / 6.0))
					extents->hasWest = true;
				if (lng > (5.0 * M_PI / 6.0))
					extents->hasEast = true;
			}
		}
	}
}




/* Reject low-res output that shrinks too far from exact split cell extents. */
bool
prepared_polygon_matches_h3set_extents(const LinkedGeoPolygon * polygon, const H3Index * h3set, int numHexes)
{
	BoundaryExtents cellExtents;
	BoundaryExtents polygonExtents;
	bool init = false;
	double cellLatSpan;
	double cellLngSpan;
	double polygonLatSpan;
	double polygonLngSpan;

	h3_set_boundary_extents(h3set, numHexes, &cellExtents);
	FOREACH_LINKED_POLYGON(polygon, part)
	{
		FOREACH_LINKED_LOOP(part, loop)
		{
			LoopBounds bounds;

			if (!loop || !loop->first)
				continue;

			loop_bounds(loop, &bounds);
			if (!init)
			{
				polygonExtents.minLat = bounds.minLat;
				polygonExtents.maxLat = bounds.maxLat;
				polygonExtents.minLng = bounds.minLng;
				polygonExtents.maxLng = bounds.maxLng;
				init = true;
			}
			else
			{
				if (bounds.minLat < polygonExtents.minLat)
					polygonExtents.minLat = bounds.minLat;
				if (bounds.maxLat > polygonExtents.maxLat)
					polygonExtents.maxLat = bounds.maxLat;
				if (bounds.minLng < polygonExtents.minLng)
					polygonExtents.minLng = bounds.minLng;
				if (bounds.maxLng > polygonExtents.maxLng)
					polygonExtents.maxLng = bounds.maxLng;
			}
		}
	}
	if (!init)
	{
		polygonExtents.minLat = polygonExtents.maxLat = 0.0;
		polygonExtents.minLng = polygonExtents.maxLng = 0.0;
	}

	cellLatSpan = cellExtents.maxLat - cellExtents.minLat;
	cellLngSpan = cellExtents.maxLng - cellExtents.minLng;
	polygonLatSpan = polygonExtents.maxLat - polygonExtents.minLat;
	polygonLngSpan = polygonExtents.maxLng - polygonExtents.minLng;

	return count_linked_polygons(polygon) == 1
		&& polygonLatSpan + 1e-12 >= cellLatSpan * 0.9
		&& polygonLngSpan + 1e-12 >= cellLngSpan * 0.9;
}

/* Build the rectangular cap/extent boundary emitted for low-res collapsed results. */
void
build_extent_boundary(CellBoundary * boundary, const BoundaryExtents * extents)
{
	double		west = extents->hasWest && extents->hasEast ? -180.0 : radsToDegs(extents->minLng);
	double		east = extents->hasWest && extents->hasEast ? 180.0 : radsToDegs(extents->maxLng);
	double		south = radsToDegs(extents->minLat);
	double		north = radsToDegs(extents->maxLat);

	/*
	 * Low-resolution full-width caps come back from cell boundaries just shy of
	 * the mathematical pole (for example 89.9999 instead of 90). Snap only
	 * that narrow class to the pole so coverage checks do not leave a thin
	 * north/south strip behind.
	 */
	if (extents->hasWest && extents->hasEast)
	{
		if (south < -89.999)
			south = -90.0;
		if (north > 89.999)
			north = 90.0;
	}

	boundary->numVerts = 4;
	boundary->verts[0] = (LatLng) {.lat = south, .lng = west};
	boundary->verts[1] = (LatLng) {.lat = north, .lng = west};
	boundary->verts[2] = (LatLng) {.lat = north, .lng = east};
	boundary->verts[3] = (LatLng) {.lat = south, .lng = east};
}

/* Remove repeated spike vertices from one loop in place. */
void
prune_linked_geo_loop_spikes(LinkedGeoLoop * loop)
{
	LatLng	   *verts;
	int			count = copy_linked_geo_loop_vertices(loop, &verts);
	LatLng	   *stack;
	int			stackCount = 0;
	LinkedLatLng *latlng;

	if (count < 3)
	{
		pfree(verts);
		return;
	}

	stack = palloc(count * sizeof(*stack));

	for (int i = 0; i < count; i++)
	{
		if (stackCount > 0 && geoAlmostEqual(&stack[stackCount - 1], &verts[i]))
			continue;

		stack[stackCount++] = verts[i];
		while (stackCount >= 3
			   && geoAlmostEqual(&stack[stackCount - 3], &stack[stackCount - 1]))
		{
			stackCount -= 2;
		}
	}

	while (stackCount >= 2 && geoAlmostEqual(&stack[0], &stack[stackCount - 1]))
		stackCount--;
	while (stackCount >= 3 && geoAlmostEqual(&stack[stackCount - 2], &stack[0]))
		stackCount -= 2;

	latlng = loop->first;
	while (latlng)
	{
		LinkedLatLng *next = latlng->next;

		pfree(latlng);
		latlng = next;
	}
	loop->first = NULL;
	loop->last = NULL;

	for (int i = 0; i < stackCount; i++)
	{
		LinkedLatLng *copy = palloc0(sizeof(*copy));

		copy->vertex = stack[i];
		add_linked_lat_lng(loop, copy);
	}

	pfree(stack);
	pfree(verts);
}
