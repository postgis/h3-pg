\pset tuples_only on
\set hexagon '\'831c02fffffffff\'::h3index'
\set other_hexagon '\'831c04fffffffff\'::h3index'
\set pentagon '\'831c00fffffffff\'::h3index'

CREATE TABLE h3_test_gist (hex h3index);
CREATE INDEX h3_test_gist_idx
          ON h3_test_gist
       USING gist(hex h3index_gist_ops_experimental);

-- GiST sortsupport proc 11 is available on PostgreSQL 14+
SELECT (current_setting('server_version_num')::int >= 140000) = EXISTS (
    SELECT 1
    FROM pg_amproc ap
    JOIN pg_opfamily f ON f.oid = ap.amprocfamily
    JOIN pg_am am ON am.oid = f.opfmethod
    JOIN pg_proc p ON p.oid = ap.amproc
    WHERE am.amname = 'gist'
      AND f.opfname = 'h3index_gist_ops_experimental'
      AND ap.amprocnum = 11
      AND p.proname = 'h3index_gist_sortsupport'
      AND oidvectortypes(p.proargtypes) = 'internal'
);

-- insert parent, the hexagon itself, immediate children, and a deep center child
INSERT INTO h3_test_gist (hex) SELECT h3_cell_to_parent(:hexagon);
INSERT INTO h3_test_gist (hex) SELECT :hexagon;
INSERT INTO h3_test_gist (hex) SELECT h3_cell_to_children(:hexagon);
INSERT INTO h3_test_gist (hex) SELECT h3_cell_to_center_child(:hexagon, 15);

-- Force index usage for all subsequent queries
SET enable_seqscan = off;

--
-- TEST contains (@>)
--
-- parent and hexagon itself contain the hexagon
SELECT COUNT(*) = 2 FROM h3_test_gist WHERE hex @> :hexagon;

--
-- TEST contained by (<@)
--
-- hexagon itself, children, and center child are contained by hexagon
SELECT COUNT(*) = 9 FROM h3_test_gist WHERE hex <@ :hexagon;

--
-- TEST overlap (&&)
--
SELECT COUNT(*) > 0 FROM h3_test_gist WHERE hex && :hexagon;

--
-- TEST equality (=) via index
--
SELECT COUNT(*) = 1 FROM h3_test_gist WHERE hex = :hexagon;

--
-- TEST no results for unrelated cell
--
SELECT COUNT(*) = 0 FROM h3_test_gist WHERE hex <@ :other_hexagon;

--
-- TEST with larger dataset forcing picksplit
--
INSERT INTO h3_test_gist (hex) SELECT h3_cell_to_children(:hexagon, 6);
SELECT COUNT(*) > 9 FROM h3_test_gist WHERE hex <@ :hexagon;

--
-- TEST KNN distance ordering
--
INSERT INTO h3_test_gist (hex) SELECT h3_grid_disk(:hexagon, 2);
SELECT hex = :hexagon FROM h3_test_gist ORDER BY hex <-> :hexagon LIMIT 1;

RESET enable_seqscan;

-- ============================================================
-- Regression tests for bugs fixed in this PR
-- ============================================================

--
-- TEST self-containment: a <@ a must be true
-- Bug: the <@ path treated equality as false even though containment(a,a)
-- reports equality as containment.
-- This caused seqscan and index scan to disagree.
--
-- Verify via the operator directly (no index involvement)
SELECT :hexagon <@ :hexagon;

-- Verify seqscan and index scan agree on self-containment
CREATE TABLE h3_test_self (hex h3index);
INSERT INTO h3_test_self VALUES (:hexagon);
CREATE INDEX h3_test_self_idx ON h3_test_self USING gist(hex h3index_gist_ops_experimental);

-- seqscan result
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT COUNT(*) = 1 AS self_contained_seqscan FROM h3_test_self WHERE hex <@ :hexagon;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- index scan result must match
SET enable_seqscan = off;
SELECT COUNT(*) = 1 AS self_contained_indexscan FROM h3_test_self WHERE hex <@ :hexagon;
RESET enable_seqscan;

DROP TABLE h3_test_self;

--
-- TEST self-containment for @> as well (symmetric check)
--
SELECT :hexagon @> :hexagon;

--
-- TEST public <-> mixed-resolution semantics directly
-- The coarser input should be refined to its center child at the finer
-- resolution, and the operator should stay commutative across resolutions.
--
SELECT h3_cell_to_parent(:hexagon, 1) <-> :hexagon
    = h3_cell_to_center_child(
        h3_cell_to_parent(:hexagon, 1),
        h3_get_resolution(:hexagon)
      ) <-> :hexagon;

SELECT h3_cell_to_parent(:hexagon, 1) <-> :hexagon
    = :hexagon <-> h3_cell_to_parent(:hexagon, 1);

--
-- TEST KNN returns correct ordering beyond the first result
-- Bug: center-child distance was not a lower bound, so KNN could
-- skip correct nearest neighbors or return wrong ordering.
--
CREATE TABLE h3_test_knn (hex h3index);
INSERT INTO h3_test_knn SELECT h3_grid_disk(:hexagon, 3);
CREATE INDEX h3_test_knn_idx ON h3_test_knn USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;

-- first result must be the query cell itself (distance 0)
SELECT hex = :hexagon FROM h3_test_knn ORDER BY hex <-> :hexagon LIMIT 1;

-- top 7 results should be the query cell + its 6 immediate neighbors (ring 1)
-- all at distance <= 1
SELECT COUNT(*) = 7 FROM (
    SELECT hex FROM h3_test_knn ORDER BY hex <-> :hexagon LIMIT 7
) t
WHERE h3_grid_distance(hex, :hexagon) <= 1;

-- distances must be monotonically non-decreasing
SELECT bool_and(d1 <= d2) FROM (
    SELECT hex <-> :hexagon AS d1,
           lead(hex <-> :hexagon) OVER (ORDER BY hex <-> :hexagon) AS d2
    FROM h3_test_knn
) t WHERE d2 IS NOT NULL;

RESET enable_seqscan;
DROP TABLE h3_test_knn;

--
-- TEST KNN with query coarser than stored data
-- Bug: finer-key branch used cellToParent(query, keyRes) which fails
-- when keyRes > queryRes. Distance returned INFINITY, breaking ordering.
--
CREATE TABLE h3_test_knn_finer (hex h3index);
-- store res-4 children, query with res-3 parent
INSERT INTO h3_test_knn_finer SELECT h3_cell_to_children(:hexagon);
CREATE INDEX h3_test_knn_finer_idx ON h3_test_knn_finer USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;

-- KNN with a coarser query should still return results (not INFINITY)
SELECT COUNT(*) = 7 FROM (
    SELECT hex FROM h3_test_knn_finer ORDER BY hex <-> :hexagon LIMIT 7
) t;

-- the center child should be closest (or tied for closest)
SELECT hex = h3_cell_to_center_child(:hexagon)
FROM h3_test_knn_finer
ORDER BY hex <-> :hexagon LIMIT 1;

RESET enable_seqscan;
DROP TABLE h3_test_knn_finer;

--
-- TEST multi-base-cell data (picksplit balance and H3_NULL handling)
-- Bug: when entries spanned multiple base cells, picksplit assigned
-- everything to the right side, creating a degenerate tree.
-- Also: KNN returned INFINITY for H3_NULL internal nodes, skipping
-- entire mixed-base subtrees.
--
CREATE TABLE h3_test_multibase (hex h3index);
-- insert children from two different base cells
INSERT INTO h3_test_multibase SELECT h3_cell_to_children(:hexagon);
INSERT INTO h3_test_multibase SELECT h3_cell_to_children(:other_hexagon);
-- insert enough data to force picksplit
INSERT INTO h3_test_multibase SELECT h3_cell_to_children(:hexagon, 5);
INSERT INTO h3_test_multibase SELECT h3_cell_to_children(:other_hexagon, 5);
CREATE INDEX h3_test_multibase_idx ON h3_test_multibase USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;

-- containment queries must work across base cells
SELECT COUNT(*) = 7 FROM h3_test_multibase WHERE hex <@ :hexagon AND h3_get_resolution(hex) = 4;
SELECT COUNT(*) = 7 FROM h3_test_multibase WHERE hex <@ :other_hexagon AND h3_get_resolution(hex) = 4;

-- no cross-contamination
SELECT COUNT(*) = 0 FROM h3_test_multibase
WHERE hex <@ :hexagon AND hex <@ :other_hexagon;

-- KNN should find results from both base cells
SELECT COUNT(*) > 0 FROM (
    SELECT hex FROM h3_test_multibase ORDER BY hex <-> :hexagon LIMIT 10
) t;
SELECT COUNT(*) > 0 FROM (
    SELECT hex FROM h3_test_multibase ORDER BY hex <-> :other_hexagon LIMIT 10
) t;

RESET enable_seqscan;
DROP TABLE h3_test_multibase;

--
-- TEST picksplit with H3_NULL entries in seed selection
-- Bug: when an internal page contained H3_NULL keys (from prior splits
-- spanning multiple base cells), the seed-waste loop called containment(),
-- getResolution(), and cellToChildrenSize() on H3_NULL — undefined behavior.
-- Use many base cells to force deep re-splitting where H3_NULL appears.
--
CREATE TABLE h3_test_nullseed (hex h3index);
-- insert children from 6 different base cells to force H3_NULL internal nodes
INSERT INTO h3_test_nullseed SELECT h3_cell_to_children('831c02fffffffff'::h3index, 5);
INSERT INTO h3_test_nullseed SELECT h3_cell_to_children('831c04fffffffff'::h3index, 5);
INSERT INTO h3_test_nullseed SELECT h3_cell_to_children('831c06fffffffff'::h3index, 5);
INSERT INTO h3_test_nullseed SELECT h3_cell_to_children('831c08fffffffff'::h3index, 5);
INSERT INTO h3_test_nullseed SELECT h3_cell_to_children('831c0afffffffff'::h3index, 5);
INSERT INTO h3_test_nullseed SELECT h3_cell_to_children('831c0cfffffffff'::h3index, 5);
-- building the index must not crash (triggers picksplit on pages with H3_NULL)
CREATE INDEX h3_test_nullseed_idx ON h3_test_nullseed USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;

-- queries must still return correct results after the split
SELECT COUNT(*) > 0 FROM h3_test_nullseed
WHERE hex <@ '831c02fffffffff'::h3index;

SELECT COUNT(*) > 0 FROM h3_test_nullseed
WHERE hex <@ '831c0cfffffffff'::h3index;

RESET enable_seqscan;
DROP TABLE h3_test_nullseed;

--
-- TEST resolution 0 (base cell) boundary
-- Bug: finest_common_ancestor loop used i > 0, missing res 0.
-- Two cells in the same base cell diverging at res 1 returned H3_NULL.
--
CREATE TABLE h3_test_res0 (hex h3index);
INSERT INTO h3_test_res0 SELECT h3_cell_to_parent(:hexagon, 0);
INSERT INTO h3_test_res0 SELECT h3_cell_to_parent(:hexagon, 1);
INSERT INTO h3_test_res0 SELECT :hexagon;
CREATE INDEX h3_test_res0_idx ON h3_test_res0 USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;

-- res-0 base cell contains everything
SELECT COUNT(*) = 3 FROM h3_test_res0
WHERE hex <@ h3_cell_to_parent(:hexagon, 0);

-- the hexagon is contained by the base cell
SELECT COUNT(*) = 1 FROM h3_test_res0
WHERE hex @> :hexagon AND h3_get_resolution(hex) = 0;

RESET enable_seqscan;
DROP TABLE h3_test_res0;

--
-- TEST resolution 15 (finest) boundary
-- Bug: cellToCenterChild at res 15 is the finest possible.
-- Distance function must handle this without error.
--
CREATE TABLE h3_test_res15 (hex h3index);
INSERT INTO h3_test_res15 SELECT h3_cell_to_center_child(:hexagon, 15);
INSERT INTO h3_test_res15 SELECT h3_cell_to_center_child(:hexagon, 14);
INSERT INTO h3_test_res15 SELECT :hexagon;
CREATE INDEX h3_test_res15_idx ON h3_test_res15 USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;

-- containment at finest resolution
SELECT COUNT(*) = 1 FROM h3_test_res15 WHERE hex @> :hexagon;

-- KNN ordering with res-15 cells should not error
SELECT COUNT(*) = 3 FROM (
    SELECT hex FROM h3_test_res15 ORDER BY hex <-> :hexagon LIMIT 3
) t;

RESET enable_seqscan;
DROP TABLE h3_test_res15;

--
-- TEST pentagon cells
-- Pentagons have 6 children instead of 7 and gridDistance can fail
-- across pentagon boundaries.
--
CREATE TABLE h3_test_pentagon (hex h3index);
INSERT INTO h3_test_pentagon SELECT :pentagon;
INSERT INTO h3_test_pentagon SELECT h3_cell_to_children(:pentagon);
INSERT INTO h3_test_pentagon SELECT h3_cell_to_center_child(:pentagon, 10);
CREATE INDEX h3_test_pentagon_idx ON h3_test_pentagon USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;

-- pentagon contains itself
SELECT COUNT(*) = 1 FROM h3_test_pentagon WHERE hex = :pentagon;

-- pentagon has 6 children (not 7)
SELECT COUNT(*) = 6 FROM h3_test_pentagon
WHERE hex <@ :pentagon AND h3_get_resolution(hex) = 4;

-- self-containment works for pentagons
SELECT :pentagon <@ :pentagon;

-- KNN from a pentagon should not error
SELECT COUNT(*) > 0 FROM (
    SELECT hex FROM h3_test_pentagon ORDER BY hex <-> :pentagon LIMIT 3
) t;

RESET enable_seqscan;
DROP TABLE h3_test_pentagon;

--
-- TEST <-> returns a large positive sentinel (not -1) when gridDistance fails
-- Bug: the <-> operator returned -1 on gridDistance failure (common near
-- pentagons), which sorted before 0 and broke all KNN ordering.
--
-- :hexagon's grid disk includes cells where gridDistance fails;
-- verify those now sort last instead of producing a negative distance
SELECT COUNT(*) = 0 FROM (
    SELECT hex, hex <-> :hexagon AS dist
    FROM (SELECT h3_grid_disk(:hexagon, 3) AS hex) t
) t2
WHERE dist < 0;

SELECT COUNT(*) > 0 FROM (
    SELECT hex, hex <-> :hexagon AS dist
    FROM (SELECT h3_grid_disk(:hexagon, 3) AS hex) t
) t2
WHERE dist = 9223372036854775807;

-- verify the self-distance is exactly 0
SELECT :hexagon <-> :hexagon = 0;

-- verify KNN with the GiST index returns self first even when
-- the dataset contains cells with sentinel-max distance
CREATE TABLE h3_test_dist (hex h3index);
INSERT INTO h3_test_dist SELECT h3_grid_disk(:hexagon, 3);
CREATE INDEX h3_test_dist_idx ON h3_test_dist USING gist(hex h3index_gist_ops_experimental);

SET enable_seqscan = off;
SELECT hex = :hexagon FROM h3_test_dist ORDER BY hex <-> :hexagon LIMIT 1;

RESET enable_seqscan;
DROP TABLE h3_test_dist;

-- cleanup
DROP TABLE h3_test_gist;
