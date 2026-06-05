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

#include <math.h>

#include "wkb_vertex_graph.h"

/*
 * This is a narrow replacement for H3's removed internal vertexGraph helper.
 * h3_postgis uses it only while rebuilding WKB polygons from H3 cell
 * boundaries that were split/noded for antimeridian and overlap handling.
 *
 * The graph stores directed boundary pieces. When the caller sees the same
 * segment in the opposite direction, it removes the existing edge instead of
 * adding another one; shared internal boundaries then disappear and only the
 * exterior polygon rings remain for polygonize_noded_graph().
 */
static uint32_t hash_vertex(const VertexGraph *graph, const LatLng *vertex);

void
initVertexGraph(VertexGraph *graph, int numBuckets, int res)
{
	if (numBuckets <= 0)
		numBuckets = 1;

	/*
	 * Keep buckets always allocated. The graph helpers index directly into this
	 * array, so a zero-bucket graph would turn later add/find calls into NULL
	 * dereferences.
	 */
	graph->buckets = palloc0(numBuckets * sizeof(VertexNode *));
	graph->numBuckets = numBuckets;
	graph->size = 0;
	graph->res = res;
	/*
	 * Coordinates are compared with geoAlmostEqual(). The hash only needs to
	 * group nearby starts well enough to avoid scanning every edge; exact graph
	 * identity is still checked with geoAlmostEqual() on both endpoints.
	 */
	graph->resMultiplier = pow(10.0, 15 - res);
}

void
destroyVertexGraph(VertexGraph *graph)
{
	/* Nodes are owned by their bucket chains, so free each chain directly. */
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

	currentNode = graph->buckets[index];
	if (currentNode == NULL)
	{
		graph->buckets[index] = node;
	}
	else
	{
		for (;;)
		{
			/* Avoid inserting the same directed boundary piece twice. */
			if (geoAlmostEqual(&currentNode->from, fromVtx) &&
				geoAlmostEqual(&currentNode->to, toVtx))
			{
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
		 * A NULL toVtx means "any outgoing edge from this start". The polygon
		 * walker uses that to choose the next half-edge after it has already
		 * resolved ordering elsewhere.
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
	if (graph->numBuckets <= 0)
		return 0;

	/*
	 * Hashing only the start vertex matches the lookup pattern: reverse-edge
	 * cancellation and polygon walking both begin with a known start point.
	 */
	return (uint32_t) fmod(fabs((vertex->lat + vertex->lng) * graph->resMultiplier),
						   graph->numBuckets);
}
