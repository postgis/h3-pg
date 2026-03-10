\pset tuples_only on
\set hexagon '\'831c02fffffffff\'::h3index'

CREATE TABLE h3_test_spgist (hex h3index);
CREATE INDEX SPGIST_IDX ON h3_test_spgist USING spgist(hex h3index_ops_experimental);
INSERT INTO h3_test_spgist (hex) SELECT h3_cell_to_parent(:hexagon);
INSERT INTO h3_test_spgist (hex) SELECT h3_cell_to_children(:hexagon);
INSERT INTO h3_test_spgist (hex) SELECT h3_cell_to_center_child(:hexagon, 15);

--
-- TEST SP-GiST basic containment
--
SELECT COUNT(*) = 1 FROM h3_test_spgist WHERE hex @> :hexagon;
SELECT COUNT(*) = 8 FROM h3_test_spgist WHERE hex <@ :hexagon;

--
-- TEST SP-GiST self-containment through the index path
-- The indexed path must agree with seqscan semantics for a <@ a.
--
CREATE TABLE h3_test_spgist_self (hex h3index);
INSERT INTO h3_test_spgist_self VALUES (:hexagon);
CREATE INDEX SPGIST_SELF_IDX ON h3_test_spgist_self USING spgist(hex h3index_ops_experimental);

SET enable_seqscan = off;
SELECT COUNT(*) = 1 FROM h3_test_spgist_self WHERE hex = :hexagon;
SELECT COUNT(*) = 1 FROM h3_test_spgist_self WHERE hex @> :hexagon;
SELECT COUNT(*) = 1 FROM h3_test_spgist_self WHERE hex <@ :hexagon;
RESET enable_seqscan;

DROP TABLE h3_test_spgist_self;

--
-- TEST SP-GiST with large descendant set
--
TRUNCATE TABLE h3_test_spgist;
INSERT INTO h3_test_spgist (hex) SELECT h3_cell_to_children(h3_cell_to_center_child(:hexagon, 10), 15);
SELECT COUNT(*) = 16807 FROM h3_test_spgist WHERE hex <@ :hexagon;

--
-- TEST SP-GiST with multiple base cells (allTheSame correctness)
-- When cells from a single base cell fill a picksplit batch, PG sets
-- allTheSame and distributes subsequent inserts randomly.  The choose
-- function must set levelAdd=0 for allTheSame so the tree self-corrects.
-- Without this, containment queries silently drop results.
--
TRUNCATE TABLE h3_test_spgist;
INSERT INTO h3_test_spgist (hex)
  SELECT h3_cell_to_children(bc, 3)
  FROM (VALUES ('8001fffffffffff'::h3index), ('8003fffffffffff'::h3index)) v(bc);

-- Force index usage
SET enable_seqscan = off;

-- Each base cell should find exactly its own children (343 = 7^3)
SELECT COUNT(*) = 343 FROM h3_test_spgist
  WHERE hex <@ '8001fffffffffff'::h3index;
SELECT COUNT(*) = 343 FROM h3_test_spgist
  WHERE hex <@ '8003fffffffffff'::h3index;

-- Combined: all rows belong to one of the two base cells
SELECT COUNT(*) = 686 FROM h3_test_spgist
  WHERE hex <@ '8001fffffffffff'::h3index OR hex <@ '8003fffffffffff'::h3index;

RESET enable_seqscan;

--
-- TEST SP-GiST with tree depth exceeding cell resolution
-- When many duplicated cells from multiple base cells fill the tree,
-- picksplit can be called at a level deeper than the cell resolution.
-- This previously crashed with E_RES_MISMATCH (error code 12).
--
TRUNCATE TABLE h3_test_spgist;
INSERT INTO h3_test_spgist (hex)
  SELECT c
  FROM (
    SELECT h3_cell_to_children(bc, 3) AS c
    FROM (VALUES ('8001fffffffffff'::h3index), ('8003fffffffffff'::h3index)) v(bc)
  ) sub
  CROSS JOIN generate_series(1, 200);

-- Index must build without error
REINDEX INDEX SPGIST_IDX;

-- Cross-validate: index scan must match seq scan for each base cell
SET enable_seqscan = off;
SELECT COUNT(*) AS idx_count INTO TEMP idx_result
  FROM h3_test_spgist
  WHERE hex <@ '8001fffffffffff'::h3index OR hex <@ '8003fffffffffff'::h3index;
RESET enable_seqscan;

SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT COUNT(*) AS seq_count INTO TEMP seq_result
  FROM h3_test_spgist
  WHERE hex <@ '8001fffffffffff'::h3index OR hex <@ '8003fffffffffff'::h3index;
RESET enable_indexscan;
RESET enable_bitmapscan;

SELECT i.idx_count = s.seq_count FROM idx_result i, seq_result s;
DROP TABLE idx_result, seq_result;

-- Also verify the total is what we expect
SELECT COUNT(*) = (SELECT COUNT(*) FROM h3_test_spgist)
  FROM h3_test_spgist
  WHERE hex <@ '8001fffffffffff'::h3index OR hex <@ '8003fffffffffff'::h3index;

DROP TABLE h3_test_spgist;
