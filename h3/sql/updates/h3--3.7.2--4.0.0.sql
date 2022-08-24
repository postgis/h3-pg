/*
 * Copyright 2022 Bytes & Brains
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION h3 UPDATE TO '4.0.0'" to load this file. \quit

-- Move postgis integration to its own extension
DROP FUNCTION IF EXISTS h3_geo_to_h3(geometry, resolution integer);
DROP FUNCTION IF EXISTS h3_geo_to_h3(geography, resolution integer);
DROP FUNCTION IF EXISTS h3_to_geo_boundary_geometry(h3index);
DROP FUNCTION IF EXISTS h3_to_geo_boundary_geography(h3index);
DROP FUNCTION IF EXISTS h3_to_geo_boundary_geometry(h3index, extend boolean);
DROP FUNCTION IF EXISTS h3_to_geo_boundary_geography(h3index, extend boolean);
DROP FUNCTION IF EXISTS h3_polyfill(multi geometry, resolution integer);
DROP FUNCTION IF EXISTS h3_polyfill(multi geography, resolution integer);
DROP CAST (h3index AS geometry);
DROP CAST (h3index AS geography);
DROP FUNCTION IF EXISTS h3_to_geometry(h3index);
DROP FUNCTION IF EXISTS h3_to_geography(h3index);

-- H3 Core v4 renames

-- indexing
ALTER FUNCTION h3_geo_to_h3(point, resolution integer) RENAME TO h3_lat_lng_to_cell;
ALTER FUNCTION h3_to_geo(h3index) RENAME TO h3_cell_to_lat_lng;
ALTER FUNCTION h3_to_geo_boundary(h3index, extend_at_meridian BOOLEAN) RENAME TO h3_cell_to_boundary;

-- inspection

ALTER FUNCTION h3_get_base_cell(h3index) RENAME TO h3_get_base_cell_number;
ALTER FUNCTION h3_is_valid(h3index) RENAME TO h3_is_valid_cell;
ALTER FUNCTION h3_get_faces(h3index) RENAME TO h3_get_icosahedron_faces;

-- traversal
ALTER FUNCTION h3_k_ring(h3index, k integer) RENAME TO h3_grid_disk;
ALTER FUNCTION h3_k_ring_distances(h3index, k integer, OUT index h3index, OUT distance int) RENAME TO h3_grid_disk_distances;
ALTER FUNCTION h3_hex_ring(h3index, k integer) RENAME TO h3_grid_ring_unsafe;
ALTER FUNCTION h3_line(h3index, h3index) RENAME TO h3_grid_path_cells;
ALTER FUNCTION h3_distance(h3index, h3index) RENAME TO h3_grid_distance;
ALTER FUNCTION h3_experimental_h3_to_local_ij(origin h3index, index h3index) RENAME TO h3_cell_to_local_ij;
ALTER FUNCTION h3_experimental_local_ij_to_h3(origin h3index, coord POINT) RENAME TO h3_local_ij_to_cell;

-- hierarchy
ALTER FUNCTION h3_to_parent(h3index, resolution integer) RENAME TO h3_cell_to_parent;
ALTER FUNCTION h3_to_children(h3index, resolution integer) RENAME TO h3_cell_to_children;
ALTER FUNCTION h3_to_center_child(h3index, resolution integer) RENAME TO h3_cell_to_center_child;
ALTER FUNCTION h3_compact(h3index[]) RENAME TO h3_compact_cells;
ALTER FUNCTION h3_uncompact(h3index[], resolution integer) RENAME TO h3_uncompact_cells;
ALTER FUNCTION h3_to_children_slow(index h3index, resolution integer) RENAME TO h3_cell_to_children_slow;

-- regions
ALTER FUNCTION h3_polyfill(exterior polygon, holes polygon[], resolution integer) RENAME TO h3_polygon_to_cells;
ALTER FUNCTION h3_set_to_multi_polygon(h3index[], OUT exterior polygon, OUT holes polygon[]) RENAME TO h3_cells_to_multi_polygon;

-- edge
ALTER FUNCTION h3_indexes_are_neighbors(h3index, h3index) RENAME TO h3_are_neighbor_cells;
ALTER FUNCTION h3_get_h3_unidirectional_edge(origin h3index, destination h3index) RENAME TO h3_cells_to_directed_edge;
ALTER FUNCTION h3_unidirectional_edge_is_valid(edge h3index) RENAME TO h3_is_valid_directed_edge;
ALTER FUNCTION h3_get_origin_h3_index_from_unidirectional_edge(edge h3index) RENAME TO h3_get_directed_edge_origin;
ALTER FUNCTION h3_get_destination_h3_index_from_unidirectional_edge(edge h3index) RENAME TO h3_get_directed_edge_destination;
ALTER FUNCTION h3_get_h3_indexes_from_unidirectional_edge(edge h3index, OUT origin h3index, OUT destination h3index) RENAME TO h3_directed_edge_to_cells;
ALTER FUNCTION h3_get_h3_unidirectional_edges_from_hexagon(h3index) RENAME TO h3_origin_to_directed_edges;
ALTER FUNCTION h3_get_h3_unidirectional_edge_boundary(edge h3index) RENAME TO h3_directed_edge_to_boundary;

-- miscellaneous
ALTER FUNCTION h3_point_dist(a point, b point, unit text) RENAME TO h3_great_circle_distance;
ALTER FUNCTION h3_hex_area(resolution integer, unit text) RENAME TO h3_get_hexagon_area_avg;
ALTER FUNCTION h3_edge_length(resolution integer, unit text) RENAME TO h3_get_hexagon_edge_length_avg;
ALTER FUNCTION h3_exact_edge_length(edge h3index, unit text) RENAME TO h3_edge_length;
ALTER FUNCTION h3_num_hexagons(resolution integer) RENAME TO h3_get_num_cells;
ALTER FUNCTION h3_get_res_0_indexes() RENAME TO h3_get_res_0_cells;
ALTER FUNCTION h3_get_pentagon_indexes(resolution integer) RENAME TO h3_get_pentagons;

-- deprecated
DROP FUNCTION IF EXISTS h3_hex_area(integer, boolean);
DROP FUNCTION IF EXISTS h3_edge_length(integer, boolean);


-- copied from 07-vertex.sql
CREATE OR REPLACE FUNCTION
    h3_cell_to_vertex(cell h3index, vertexNum integer) RETURNS h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_cell_to_vertex(cell h3index, vertexNum integer)
IS 'Returns a single vertex for a given cell, as an H3 index';

CREATE OR REPLACE FUNCTION
    h3_cell_to_vertexes(cell h3index) RETURNS SETOF h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_cell_to_vertexes(cell h3index)
IS 'Returns all vertexes for a given cell, as H3 indexes';

CREATE OR REPLACE FUNCTION
    h3_vertex_to_lat_lng(vertex h3index) RETURNS point
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_vertex_to_lat_lng(vertex h3index)
IS 'Get the geocoordinates of an H3 vertex';

CREATE OR REPLACE FUNCTION
    h3_is_valid_vertex(vertex h3index) RETURNS boolean
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_is_valid_vertex(vertex h3index)
IS 'Whether the input is a valid H3 vertex';
