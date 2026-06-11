/*
 * Copyright 2025 Zacharias Knudsen
 * Copyright 2026 Darafei Praliaskouski
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
\echo Use "ALTER EXTENSION h3 UPDATE TO '4.5.0'" to load this file. \quit

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- Keep the public bigint <-> signature upgrade-safe and wire in the GiST KNN
-- operator class support functions used below.  User-defined dependent objects
-- must be maintained explicitly after the upgrade, not by this script.
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

CREATE OR REPLACE FUNCTION h3index_distance(h3index, h3index) RETURNS bigint
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON OPERATOR <-> (h3index, h3index) IS
  'Returns the distance in grid cells between the two indices after refining the coarser input to its center child at the finer resolution. Returns the maximum bigint value when gridDistance fails (e.g. near pentagons).';

/*
 * Do not automatically UPDATE, REINDEX, or REFRESH user-defined objects that
 * depend on h3index_distance() or <-> here.  Those operations can evaluate
 * user-controlled generated-column, expression-index, or materialized-view
 * expressions with the privileges of the extension upgrade session.
 *
 * Administrators should arrange any required post-upgrade maintenance for
 * affected objects explicitly, under an appropriate application/object-owner
 * role, after reviewing the object definitions.
 */

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- Btree comparator sign fix: existing btree indexes on h3index columns are
-- physically sorted in the wrong order.  REINDEX every btree index that uses
-- the h3index_ops operator class (in any column position) so the on-disk sort
-- matches the corrected comparator.
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT DISTINCT ci.oid::regclass AS idx
        FROM pg_index i
        JOIN pg_class ci ON ci.oid = i.indexrelid
        JOIN pg_am am ON am.oid = ci.relam
        JOIN pg_opclass opc ON opc.oid = ANY(i.indclass)
        WHERE am.amname = 'btree'
          AND ci.relkind = 'i'
          AND opc.opcname = 'h3index_ops'
    LOOP
        EXECUTE pg_catalog.format('REINDEX INDEX %s', r.idx);
    END LOOP;
END
$$;

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- GiST Operator Class (opclass_gist.c)
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

CREATE OR REPLACE FUNCTION h3index_gist_consistent(entry internal, query h3index, strategy smallint, subtype oid, recheck internal) RETURNS boolean
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_union(internal, internal) RETURNS h3index
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_penalty(internal, internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_picksplit(internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_same(h3index, h3index, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_distance(entry internal, query h3index, strategy smallint, subtype oid, recheck internal) RETURNS float8
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_sortsupport(internal) RETURNS void
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS h3index_gist_ops_experimental
FOR TYPE h3index USING gist
AS
    OPERATOR  3   &&  ,
    OPERATOR  6   =   ,
    OPERATOR  7   @>  ,
    OPERATOR  8   <@  ,
    OPERATOR  15  <-> (h3index, h3index) FOR ORDER BY integer_ops,
    FUNCTION  1  h3index_gist_consistent(internal, h3index, smallint, oid, internal),
    FUNCTION  2  h3index_gist_union(internal, internal),
    FUNCTION  5  h3index_gist_penalty(internal, internal, internal),
    FUNCTION  6  h3index_gist_picksplit(internal, internal),
    FUNCTION  7  h3index_gist_same(h3index, h3index, internal),
    FUNCTION  8  (h3index, h3index) h3index_gist_distance(internal, h3index, smallint, oid, internal);

DO $$
BEGIN
    IF pg_catalog.current_setting('server_version_num')::int >= 140000 THEN
        EXECUTE $sql$
            ALTER OPERATOR FAMILY h3index_gist_ops_experimental USING gist ADD
                FUNCTION 11 (h3index, h3index) h3index_gist_sortsupport(internal)
        $sql$;
    END IF;
END
$$;

COMMENT ON FUNCTION
    h3_get_resolution(h3index)
IS 'Returns the H3 resolution encoded in the index (0 through 15).';

COMMENT ON FUNCTION
    h3_get_base_cell_number(h3index)
IS 'Returns the base cell number (0 through 121) associated with the index.';

CREATE OR REPLACE FUNCTION
    h3_get_index_digit(h3index, resolution integer) RETURNS integer
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_get_index_digit(h3index, integer)
IS 'Returns the index digit at a specific resolution step. Resolution numbering is 1-based: pass 1 for the first digit below the base cell, 2 for the next, and so on.';

CREATE OR REPLACE FUNCTION
    h3_construct_cell(resolution integer, base_cell_number integer, digits integer[]) RETURNS h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_construct_cell(integer, integer, integer[])
IS 'Builds a valid H3 cell from explicit components: the target resolution, the base cell number, and a digits array ordered from resolution 1 up to the target resolution. The digits array must contain exactly one non-NULL entry per resolution step.';

CREATE OR REPLACE FUNCTION
    h3_is_valid_index(h3index) RETURNS boolean
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION
    h3_is_valid_cell(h3index)
IS 'Returns true only for valid H3 cell indexes (hexagons or pentagons). Directed edges, vertices, and malformed values return false.';

COMMENT ON FUNCTION
    h3_is_valid_index(h3index)
IS 'Returns true for any valid H3 index mode: cell, directed edge, or vertex.';

COMMENT ON FUNCTION
    h3_is_res_class_iii(h3index)
IS 'Returns true when the index is at a Class III resolution.';

COMMENT ON FUNCTION
    h3_get_icosahedron_faces(h3index)
IS 'Returns the icosahedron face numbers intersected by the index. Some cells span more than one face.';

COMMENT ON FUNCTION
    h3_grid_disk(h3index, integer)
IS 'Preferred disk API. Returns all cells with grid distance less than or equal to k from origin, including cases near pentagons. Row order is not guaranteed.';

COMMENT ON FUNCTION
    h3_grid_disk_distances(h3index, integer)
IS 'Preferred disk API with distances. Like h3_grid_disk(), but also returns the grid distance from origin for each returned cell. Handles pentagon distortion internally. Row order is not guaranteed.';

CREATE OR REPLACE FUNCTION
    h3_grid_ring(origin h3index, k integer DEFAULT 1) RETURNS SETOF h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_grid_ring(h3index, integer)
IS 'Preferred ring API. Returns the cells exactly "k" grid steps from origin. Continues to work near pentagons, but row order is not guaranteed and the result may contain fewer than 6*k cells when pentagonal distortion removes positions from the ring.';

COMMENT ON FUNCTION
    h3_grid_ring_unsafe(h3index, integer)
IS 'Fast-path ring traversal. When it succeeds it walks the ring in traversal order, but it throws if origin or the traversed ring hits pentagonal distortion. Prefer h3_grid_ring() unless you specifically want fail-fast semantics or ring-walk ordering.';

COMMENT ON FUNCTION
    h3_grid_path_cells(h3index, h3index)
IS 'Returns one shortest grid path from origin to destination, including both endpoints.

This function may fail to find the line between two indexes, for
example if they are very far apart. It may also fail when finding
distances for indexes on opposite sides of a pentagon.';

COMMENT ON FUNCTION
    h3_grid_distance(h3index, h3index)
IS 'Returns the shortest grid distance between two cells. Raises an error when the cells are not comparable, too far apart, or the path crosses pentagonal distortion.';

CREATE OR REPLACE FUNCTION
    h3_reverse_directed_edge(edge h3index) RETURNS h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_reverse_directed_edge(edge h3index)
IS 'Returns the directed edge with origin and destination cells reversed.';

COMMENT ON FUNCTION
    h3_cell_to_children(h3index, integer)
IS 'Returns the ordered set of children of the given index at the target resolution.';

COMMENT ON FUNCTION
    h3_cell_to_children(h3index)
IS 'Returns the ordered set of children of the given index at the next resolution.';

COMMENT ON FUNCTION
    h3_cell_to_children_slow(h3index, integer)
IS 'Compatibility wrapper that recursively expands one resolution step at a time.';

COMMENT ON FUNCTION
    h3_cell_to_children_slow(h3index)
IS 'Compatibility wrapper that recursively expands one resolution step at a time.';

COMMENT ON FUNCTION
    h3_cell_to_local_ij(h3index, h3index)
IS 'Converts a cell to local IJ coordinates in the coordinate system anchored at origin.';

COMMENT ON FUNCTION
    h3_local_ij_to_cell(h3index, point)
IS 'Converts local IJ coordinates in the coordinate system anchored at origin back to a cell.';
