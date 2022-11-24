# API Reference

# Base type
An unsigned 64-bit integer representing any H3 object (hexagon, pentagon, directed edge ...)
represented as a (or 16-character) hexadecimal string, like '8928308280fffff'.







# Indexing functions
These function are used for finding the H3 index containing coordinates,
and for finding the center and boundary of H3 indexes.

### h3_lat_lng_to_cell(latlng `point`, resolution `integer`) ⇒ `h3index`
*Since v4.0.0*

See also: <a href="#h3_lat_lng_to_cell.geometry.resolution.integer.h3index">h3_lat_lng_to_cell(`geometry`, `integer`)</a>, <a href="#h3_lat_lng_to_cell.geography.resolution.integer.h3index">h3_lat_lng_to_cell(`geography`, `integer`)</a>


Indexes the location at the specified resolution.


### h3_cell_to_lat_lng(cell `h3index`) ⇒ `point`
*Since v4.0.0*

See also: <a href="#h3_cell_to_geometry.h3index.geometry">h3_cell_to_geometry(`h3index`)</a>, <a href="#h3_cell_to_geography.h3index.geography">h3_cell_to_geography(`h3index`)</a>


Finds the centroid of the index.


### h3_cell_to_boundary(cell `h3index`) ⇒ `polygon`
*Since v4.0.0*

See also: <a href="#h3_cell_to_boundary_geometry.h3index.geometry">h3_cell_to_boundary_geometry(`h3index`)</a>, <a href="#h3_cell_to_boundary_geography.h3index.geography">h3_cell_to_boundary_geography(`h3index`)</a>


Finds the boundary of the index.

Use `SET h3.extend_antimeridian TO true` to extend coordinates when crossing 180th meridian.


# Index inspection functions
These functions provide metadata about an H3 index, such as its resolution
or base cell, and provide utilities for converting into and out of the
64-bit representation of an H3 index.

### h3_get_resolution(`h3index`) ⇒ `integer`
*Since v1.0.0*


Returns the resolution of the index.


### h3_get_base_cell_number(`h3index`) ⇒ `integer`
*Since v4.0.0*


Returns the base cell number of the index.


### h3_is_valid_cell(`h3index`) ⇒ `boolean`
*Since v1.0.0*


Returns true if the given H3Index is valid.


### h3_is_res_class_iii(`h3index`) ⇒ `boolean`
*Since v1.0.0*


Returns true if this index has a resolution with Class III orientation.


### h3_is_pentagon(`h3index`) ⇒ `boolean`
*Since v1.0.0*


Returns true if this index represents a pentagonal cell.


### h3_get_icosahedron_faces(`h3index`) ⇒ `integer[]`
*Since v4.0.0*


Find all icosahedron faces intersected by a given H3 index.


# Grid traversal functions
Grid traversal allows finding cells in the vicinity of an origin cell, and
determining how to traverse the grid from one cell to another.

### h3_grid_disk(origin `h3index`, [k `integer` = 1]) ⇒ SETOF `h3index`
*Since v4.0.0*


Produces indices within "k" distance of the origin index.


### h3_grid_disk_distances(origin `h3index`, [k `integer` = 1], OUT index `h3index`, OUT distance `int`) ⇒ SETOF `record`
*Since v4.0.0*


Produces indices within "k" distance of the origin index paired with their distance to the origin.


### h3_grid_ring_unsafe(origin `h3index`, [k `integer` = 1]) ⇒ SETOF `h3index`
*Since v4.0.0*


Returns the hollow hexagonal ring centered at origin with distance "k".


### h3_grid_path_cells(origin `h3index`, destination `h3index`) ⇒ SETOF `h3index`
*Since v4.0.0*

See also: <a href="#h3_grid_path_cells_recursive.origin.h3index.destination.h3index.SETOF.h3index">h3_grid_path_cells_recursive(`h3index`, `h3index`)</a>


Given two H3 indexes, return the line of indexes between them (inclusive).

This function may fail to find the line between two indexes, for
example if they are very far apart. It may also fail when finding
distances for indexes on opposite sides of a pentagon.


### h3_grid_distance(origin `h3index`, destination `h3index`) ⇒ `bigint`
*Since v4.0.0*


Returns the distance in grid cells between the two indices.


### h3_cell_to_local_ij(origin `h3index`, index `h3index`) ⇒ `point`
*Since v0.2.0*


Produces local IJ coordinates for an H3 index anchored by an origin.


### h3_local_ij_to_cell(origin `h3index`, coord `point`) ⇒ `h3index`
*Since v0.2.0*


Produces an H3 index from local IJ coordinates anchored by an origin.


# Hierarchical grid functions
These functions permit moving between resolutions in the H3 grid system.
The functions produce parent (coarser) or children (finer) cells.

### h3_cell_to_parent(cell `h3index`, resolution `integer`) ⇒ `h3index`
*Since v4.0.0*


Returns the parent of the given index.


### h3_cell_to_children(cell `h3index`, resolution `integer`) ⇒ SETOF `h3index`
*Since v4.0.0*


Returns the set of children of the given index.


### h3_cell_to_center_child(cell `h3index`, resolution `integer`) ⇒ `h3index`
*Since v4.0.0*


Returns the center child (finer) index contained by input index at given resolution.


### h3_compact_cells(cells `h3index[]`) ⇒ SETOF `h3index`
*Since v4.0.0*


Compacts the given array as best as possible.


### h3_uncompact_cells(cells `h3index[]`, resolution `integer`) ⇒ SETOF `h3index`
*Since v4.0.0*


Uncompacts the given array at the given resolution.


### h3_cell_to_parent(cell `h3index`) ⇒ `h3index`
*Since v4.0.0*


Returns the parent of the given index.


### h3_cell_to_children(cell `h3index`) ⇒ SETOF `h3index`
*Since v4.0.0*


Returns the set of children of the given index.


### h3_cell_to_center_child(cell `h3index`) ⇒ `h3index`
*Since v4.0.0*


Returns the center child (finer) index contained by input index at next resolution.


### h3_uncompact_cells(cells `h3index[]`) ⇒ SETOF `h3index`
*Since v4.0.0*


Uncompacts the given array at the resolution one higher than the highest resolution in the set.


### h3_cell_to_children_slow(index `h3index`, resolution `integer`) ⇒ SETOF `h3index`
*Since v4.0.0*


Slower version of H3ToChildren but allocates less memory.


### h3_cell_to_children_slow(index `h3index`) ⇒ SETOF `h3index`


Slower version of H3ToChildren but allocates less memory.


# Region functions
These functions convert H3 indexes to and from polygonal areas.

### h3_polygon_to_cells(exterior `polygon`, holes `polygon[]`, [resolution `integer` = 1]) ⇒ SETOF `h3index`
*Since v4.0.0*

See also: <a href="#h3_polygon_to_cells.multi.geometry.resolution.integer.SETOF.h3index">h3_polygon_to_cells(`geometry`, `integer`)</a>, <a href="#h3_polygon_to_cells.multi.geography.resolution.integer.SETOF.h3index">h3_polygon_to_cells(`geography`, `integer`)</a>


Takes an exterior polygon [and a set of hole polygon] and returns the set of hexagons that best fit the structure.


### h3_cells_to_multi_polygon(`h3index[]`, OUT exterior `polygon`, OUT holes `polygon[]`) ⇒ SETOF `record`
*Since v4.0.0*

See also: <a href="#h3_cells_to_multi_polygon_geometry.h3index.geometry">h3_cells_to_multi_polygon_geometry(`h3index[]`)</a>, <a href="#h3_cells_to_multi_polygon_geography.h3index.geography">h3_cells_to_multi_polygon_geography(`h3index[]`)</a>, <a href="#h3_cells_to_multi_polygon_geometry.setof.h3index.">h3_cells_to_multi_polygon_geometry(setof `h3index`)</a>, <a href="#h3_cells_to_multi_polygon_geography.setof.h3index.">h3_cells_to_multi_polygon_geography(setof `h3index`)</a>


Create a LinkedGeoPolygon describing the outline(s) of a set of hexagons. Polygon outlines will follow GeoJSON MultiPolygon order: Each polygon will have one outer loop, which is first in the list, followed by any holes.


# Unidirectional edge functions
Unidirectional edges allow encoding the directed edge from one cell to a
neighboring cell.

### h3_are_neighbor_cells(origin `h3index`, destination `h3index`) ⇒ `boolean`
*Since v4.0.0*


Returns true if the given indices are neighbors.


### h3_cells_to_directed_edge(origin `h3index`, destination `h3index`) ⇒ `h3index`
*Since v4.0.0*


Returns a unidirectional edge H3 index based on the provided origin and destination.


### h3_is_valid_directed_edge(edge `h3index`) ⇒ `boolean`
*Since v4.0.0*


Returns true if the given edge is valid.


### h3_get_directed_edge_origin(edge `h3index`) ⇒ `h3index`
*Since v4.0.0*


Returns the origin index from the given edge.


### h3_get_directed_edge_destination(edge `h3index`) ⇒ `h3index`
*Since v4.0.0*


Returns the destination index from the given edge.


### h3_directed_edge_to_cells(edge `h3index`, OUT origin `h3index`, OUT destination `h3index`) ⇒ `record`
*Since v4.0.0*


Returns the pair of indices from the given edge.


### h3_origin_to_directed_edges(`h3index`) ⇒ SETOF `h3index`
*Since v4.0.0*


Returns all unidirectional edges with the given index as origin.


### h3_directed_edge_to_boundary(edge `h3index`) ⇒ `polygon`
*Since v4.0.0*


Provides the coordinates defining the unidirectional edge.


# H3 Vertex functions
Functions for working with cell vertexes.

### h3_cell_to_vertex(cell `h3index`, vertexNum `integer`) ⇒ `h3index`
*Since v4.0.0*


Returns a single vertex for a given cell, as an H3 index.


### h3_cell_to_vertexes(cell `h3index`) ⇒ SETOF `h3index`
*Since v4.0.0*


Returns all vertexes for a given cell, as H3 indexes.


### h3_vertex_to_lat_lng(vertex `h3index`) ⇒ `point`
*Since v4.0.0*


Get the geocoordinates of an H3 vertex.


### h3_is_valid_vertex(vertex `h3index`) ⇒ `boolean`
*Since v4.0.0*


Whether the input is a valid H3 vertex.


# Miscellaneous H3 functions
These functions include descriptions of the H3 grid system.

### h3_great_circle_distance(a `point`, b `point`, [unit `text` = km]) ⇒ `double precision`
*Since v4.0.0*


The great circle distance in radians between two spherical coordinates.


### h3_get_hexagon_area_avg(resolution `integer`, [unit `text` = km]) ⇒ `double precision`
*Since v4.0.0*


Average hexagon area in square (kilo)meters at the given resolution.


### h3_cell_area(cell `h3index`, [unit `text` = km^2]) ⇒ `double precision`
*Since v4.0.0*


Exact area for a specific cell (hexagon or pentagon).


### h3_get_hexagon_edge_length_avg(resolution `integer`, [unit `text` = km]) ⇒ `double precision`
*Since v4.0.0*


Average hexagon edge length in (kilo)meters at the given resolution.


### h3_edge_length(edge `h3index`, [unit `text` = km]) ⇒ `double precision`
*Since v4.0.0*


Exact length for a specific unidirectional edge.


### h3_get_num_cells(resolution `integer`) ⇒ `bigint`
*Since v4.0.0*


Number of unique H3 indexes at the given resolution.


### h3_get_res_0_cells() ⇒ SETOF `h3index`
*Since v4.0.0*


Returns all 122 resolution 0 indexes.


### h3_get_pentagons(resolution `integer`) ⇒ SETOF `h3index`
*Since v4.0.0*


All the pentagon H3 indexes at the specified resolution.


# Operators

## B-tree operators

### Operator: `h3index` = `h3index`
*Since v0.1.0*


Returns true if two indexes are the same.


### Operator: `h3index` <> `h3index`
*Since v0.1.0*


## R-tree Operators

### Operator: `h3index` && `h3index`
*Since v3.6.1*


Returns true if the two H3 indexes intersect.


### Operator: `h3index` @> `h3index`
*Since v3.6.1*


Returns true if A containts B.


### Operator: `h3index` <@ `h3index`
*Since v3.6.1*


Returns true if A is contained by B.


### Operator: `h3index` <-> `h3index`
*Since v3.7.0*


Returns the distance in grid cells between the two indices.


# Type casts

### `h3index` :: `bigint`


Convert H3 index to bigint, which is useful when you need a decimal representation.


### `bigint` :: `h3index`


Convert bigint to H3 index.


### `h3index` :: `point`


Convert H3 index to point.


# Extension specific functions

### h3_get_extension_version() ⇒ `text`
*Since v1.0.0*


Get the currently installed version of the extension.


# Deprecated functions

### h3_cell_to_boundary(cell `h3index`, extend_antimeridian `boolean`) ⇒ `polygon`


DEPRECATED: Use `SET h3.extend_antimeridian TO true` instead.

# PostGIS Integration

# PostGIS Indexing Functions

### h3_lat_lng_to_cell(`geometry`, resolution `integer`) ⇒ `h3index`
*Since v4.0.0*


Indexes the location at the specified resolution.


### h3_lat_lng_to_cell(`geography`, resolution `integer`) ⇒ `h3index`
*Since v4.0.0*


Indexes the location at the specified resolution.


### h3_cell_to_geometry(`h3index`) ⇒ `geometry`
*Since v4.0.0*


Finds the centroid of the index.


### h3_cell_to_geography(`h3index`) ⇒ `geography`
*Since v4.0.0*


Finds the centroid of the index.


### h3_cell_to_boundary_geometry(`h3index`) ⇒ `geometry`
*Since v4.0.0*


Finds the boundary of the index.

Splits polygons when crossing 180th meridian.


### h3_cell_to_boundary_geography(`h3index`) ⇒ `geography`
*Since v4.0.0*


Finds the boundary of the index.

Splits polygons when crossing 180th meridian.


# PostGIS Grid Traversal Functions

### h3_grid_path_cells_recursive(origin `h3index`, destination `h3index`) ⇒ SETOF `h3index`
*Since vunreleased*


# PostGIS Region Functions

### h3_polygon_to_cells(multi `geometry`, resolution `integer`) ⇒ SETOF `h3index`
*Since v4.0.0*


### h3_polygon_to_cells(multi `geography`, resolution `integer`) ⇒ SETOF `h3index`
*Since v4.0.0*


### h3_cells_to_multi_polygon_geometry(`h3index[]`) ⇒ `geometry`
*Since vunreleased*


### h3_cells_to_multi_polygon_geography(`h3index[]`) ⇒ `geography`
*Since vunreleased*


### h3_cells_to_multi_polygon_geometry(setof `h3index`)
*Since vunreleased*


### h3_cells_to_multi_polygon_geography(setof `h3index`)
*Since vunreleased*


# PostGIS casts

### `h3index` :: `geometry`
*Since v0.3.0*


### `h3index` :: `geography`
*Since v0.3.0*


# WKB indexing functions

### h3_cell_to_boundary_wkb(cell `h3index`) ⇒ `bytea`
*Since vunreleased*


Finds the boundary of the index, converts to EWKB.

Splits polygons when crossing 180th meridian.

This function has to return WKB since Postgres does not provide multipolygon type.


# WKB regions functions

### h3_cells_to_multi_polygon_wkb(`h3index[]`) ⇒ `bytea`
*Since vunreleased*


Create a LinkedGeoPolygon describing the outline(s) of a set of hexagons, converts to EWKB.

Splits polygons when crossing 180th meridian.


