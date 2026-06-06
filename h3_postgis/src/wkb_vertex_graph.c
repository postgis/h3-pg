/*
 * Copyright 2017-2018, 2020-2021 Uber Technologies, Inc.
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

#include <stdint.h>
#include <math.h>
#include <utils/memutils.h>

#include "wkb_vertex_graph.h"

/*
 * Vertex graph used by the h3_postgis WKB polygonizer.
 *
 * Historical context:
 * H3 versions before 4.5 exposed an internal vertexGraph helper. h3_postgis
 * used the same directed-edge cancellation model while converting H3 cell
 * boundaries into planar WKB rings. H3 4.5 removed that private helper, and the
 * replacement cellsToMultiPolygon path remains internal to H3 and does not
 * replace h3_postgis' antimeridian-aware, noded WKB pipeline. This file is
 * therefore the smallest graph primitive needed to preserve the existing
 * h3_postgis polygonization semantics.
 *
 * Mathematical model:
 * The graph behaves like a multiset of directed boundary segments with one
 * cancellation rule. Adding segment A -> B stores that directed segment unless
 * the opposite segment B -> A is already present; in that case the stored
 * opposite segment is removed. Shared cell boundaries are emitted once by each
 * adjacent cell in opposite directions, so they cancel. Segments that remain in
 * the graph are boundary half-edges of the exterior polygonal arrangement and
 * are consumed by polygonize_noded_graph().
 *
 * Scope of responsibility:
 * This module does not classify rings, holes, winding order, containment, or
 * antimeridian topology. wkb_regions.c is responsible for producing planar
 * split/noded segments and for polygonizing the surviving half-edges. This
 * module only stores, finds, and removes directed segments under H3's
 * geoAlmostEqual() coordinate equality.
 */
static uint32_t hash_vertex(const VertexGraph *graph, const LatLng *vertex);
static uint64_t mix_uint64(uint64_t value);
static void *palloc0_array_checked(Size count, Size elementSize);

void
initVertexGraph(VertexGraph *graph, int numBuckets, int res)
{
	if (numBuckets <= 0)
		numBuckets = 1;

	/*
	 * numBuckets is a capacity hint derived by the caller from the expected
	 * number of boundary segments. The table must nevertheless contain at least
	 * one bucket because add/find/remove operations index the bucket array
	 * directly.
	 *
	 * Buckets are only an acceleration structure. Candidate nodes selected by
	 * the hash are always rechecked with geoAlmostEqual() on the relevant
	 * endpoint(s), so hash collisions can increase scan length but cannot create
	 * a false geometric match.
	 */
	graph->buckets = palloc0_array_checked(numBuckets, sizeof(VertexNode *));
	graph->numBuckets = numBuckets;
	graph->size = 0;
	graph->res = res;
	/*
	 * The hash uses quantized coordinates only to choose a likely bucket for the
	 * start vertex. resMultiplier mirrors the historical H3 helper: lower H3
	 * resolutions generate coarser boundary coordinates and therefore tolerate a
	 * coarser hash quantization. Equality itself remains geoAlmostEqual().
	 */
	graph->resMultiplier = pow(10.0, 15 - res);
}

void
destroyVertexGraph(VertexGraph *graph)
{
	/*
	 * The graph owns the bucket array and every VertexNode in its chains. LatLng
	 * endpoints are copied into each node, so destruction does not touch any
	 * caller-owned coordinate storage.
	 */
	for (int i = 0; i < graph->numBuckets; i++)
	{
		VertexNode *node = graph->buckets[i];

		while (node != NULL)
		{
			VertexNode *next = node->next;

			pfree(node);
			node = next;
		}
	}

	if (graph->buckets)
		pfree(graph->buckets);
	graph->buckets = NULL;
	graph->size = 0;
	graph->numBuckets = 0;
}

VertexNode *
addVertexNode(VertexGraph *graph, const LatLng *fromVtx, const LatLng *toVtx)
{
	VertexNode *node;
	VertexNode *currentNode;
	uint32_t	index = hash_vertex(graph, fromVtx);

	node = palloc(sizeof(*node));
	node->from = *fromVtx;
	node->to = *toVtx;
	node->next = NULL;

	/*
	 * Nodes are indexed by start vertex only. Both graph operations that need a
	 * lookup begin with a known start point: reverse-edge cancellation searches
	 * for B -> A before inserting A -> B, and ring walking asks for an outgoing
	 * half-edge from the current vertex. The end vertex is therefore tested
	 * after bucket selection rather than being part of the bucket key.
	 */
	currentNode = graph->buckets[index];
	if (currentNode == NULL)
	{
		graph->buckets[index] = node;
	}
	else
	{
		for (;;)
		{
			/*
			 * The directed segment set is idempotent: inserting the same A -> B
			 * segment again leaves the resident node in place and returns it.
			 */
			if (geoAlmostEqual(&currentNode->from, fromVtx) &&
				geoAlmostEqual(&currentNode->to, toVtx))
			{
				/*
				 * Duplicate directed pieces can appear after geometric splitting
				 * and noding. Preserving the first node keeps pointer identity
				 * stable for callers that already obtained a graph node and
				 * matches the contract of the historical helper.
				 */
				pfree(node);
				return currentNode;
			}
			if (currentNode->next == NULL)
				break;
			currentNode = currentNode->next;
		}
		currentNode->next = node;
	}

	graph->size++;
	return node;
}

int
removeVertexNode(VertexGraph *graph, VertexNode *node)
{
	uint32_t	index = hash_vertex(graph, &node->from);
	VertexNode *currentNode = graph->buckets[index];
	bool		found = false;

	if (currentNode != NULL)
	{
		if (currentNode == node)
		{
			graph->buckets[index] = node->next;
			found = true;
		}
		while (!found && currentNode->next != NULL)
		{
			if (currentNode->next == node)
			{
				currentNode->next = node->next;
				found = true;
				break;
			}
			currentNode = currentNode->next;
		}
	}

	if (!found)
		return 1;

	/*
	 * Removal is by node identity, not by a fresh coordinate search. That makes
	 * the operation unambiguous when a bucket contains several outgoing segments
	 * whose start coordinates are almost equal under geoAlmostEqual().
	 */
	pfree(node);
	graph->size--;
	return 0;
}

VertexNode *
findNodeForEdge(const VertexGraph *graph, const LatLng *fromVtx,
				const LatLng *toVtx)
{
	uint32_t	index = hash_vertex(graph, fromVtx);
	VertexNode *node = graph->buckets[index];

	while (node != NULL)
	{
		/*
		 * Two lookup modes are required by the polygonization algorithm:
		 *
		 * - toVtx != NULL: exact directed-edge lookup under geoAlmostEqual(),
		 *   used to decide whether the reverse segment is already present and
		 *   should cancel the candidate insertion.
		 * - toVtx == NULL: outgoing half-edge lookup, used by the ring walker
		 *   after the next start vertex has already been determined.
		 */
		if (geoAlmostEqual(&node->from, fromVtx) &&
			(toVtx == NULL || geoAlmostEqual(&node->to, toVtx)))
			return node;
		node = node->next;
	}

	return NULL;
}

static uint32_t
hash_vertex(const VertexGraph *graph, const LatLng *vertex)
{
	uint64_t	latHash;
	uint64_t	lngHash;
	int64_t		lat;
	int64_t		lng;

	if (graph->numBuckets <= 0)
		return 0;

	/*
	 * The hash is intentionally derived only from the start vertex because every
	 * graph lookup is anchored at a start coordinate. Latitude and longitude are
	 * quantized independently and mixed before combination; reducing a vertex to
	 * lat+lng would systematically collide points on the same diagonal and make
	 * dense polygon boundaries degrade toward long linear scans.
	 */
	lat = (int64_t) llround(vertex->lat * graph->resMultiplier);
	lng = (int64_t) llround(vertex->lng * graph->resMultiplier);
	latHash = mix_uint64((uint64_t) lat);
	lngHash = mix_uint64((uint64_t) lng);
	return (uint32_t) ((latHash ^ (lngHash + UINT64_C(0x9e3779b97f4a7c15)
								   + (latHash << 6) + (latHash >> 2)))
					   % (uint64_t) graph->numBuckets);
}

static uint64_t
mix_uint64(uint64_t value)
{
	/*
	 * SplitMix64 finalizer used as a small non-cryptographic avalanche step.
	 * The graph is not a security boundary; the goal is deterministic diffusion
	 * of quantized coordinate integers across the caller-provided bucket count.
	 */
	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	value ^= value >> 31;
	return value;
}

static void *
palloc0_array_checked(Size count, Size elementSize)
{
	/*
	 * Guard the arithmetic before calling palloc0(). PostgreSQL checks whether
	 * the final allocation size is valid, but it cannot detect an overflow that
	 * already occurred while computing count * elementSize.
	 */
	if (elementSize != 0 && count > MaxAllocSize / elementSize)
		elog(ERROR, "too many vertex graph buckets");
	return palloc0(count * elementSize);
}
