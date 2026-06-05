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

static uint32_t hash_vertex(const VertexGraph *graph, const LatLng *vertex);

void
initVertexGraph(VertexGraph *graph, int numBuckets, int res)
{
	if (numBuckets <= 0)
		numBuckets = 1;

	graph->buckets = palloc0(numBuckets * sizeof(VertexNode *));
	graph->numBuckets = numBuckets;
	graph->size = 0;
	graph->res = res;
	graph->resMultiplier = pow(10.0, 15 - res);
}

void
destroyVertexGraph(VertexGraph *graph)
{
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

	return (uint32_t) fmod(fabs((vertex->lat + vertex->lng) * graph->resMultiplier),
						   graph->numBuckets);
}
