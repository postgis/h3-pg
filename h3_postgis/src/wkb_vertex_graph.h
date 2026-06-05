/*
 * Copyright 2017, 2020-2021 Uber Technologies, Inc.
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

#ifndef WKB_VERTEX_GRAPH_H
#define WKB_VERTEX_GRAPH_H

#include <latLng.h>

typedef struct VertexNode VertexNode;

struct VertexNode
{
	LatLng		from;
	LatLng		to;
	VertexNode *next;
};

typedef struct
{
	VertexNode **buckets;
	int			numBuckets;
	int			size;
	int			res;
	double		resMultiplier;
} VertexGraph;

void initVertexGraph(VertexGraph *graph, int numBuckets, int res);
void destroyVertexGraph(VertexGraph *graph);
VertexNode *addVertexNode(VertexGraph *graph, const LatLng *fromVtx,
						  const LatLng *toVtx);
int removeVertexNode(VertexGraph *graph, VertexNode *node);
VertexNode *findNodeForEdge(const VertexGraph *graph, const LatLng *fromVtx,
							const LatLng *toVtx);

#endif
