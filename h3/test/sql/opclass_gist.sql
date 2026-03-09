\pset tuples_only on
\set hexagon '\'831c02fffffffff\'::h3index'
\set other_hexagon '\'831c04fffffffff\'::h3index'

CREATE TABLE h3_test_gist (hex h3index);
CREATE INDEX h3_test_gist_idx
          ON h3_test_gist
       USING gist(hex h3index_gist_ops_experimental);

-- insert parent, the hexagon itself, immediate children, and a deep center child
INSERT INTO h3_test_gist (hex) SELECT h3_cell_to_parent(:hexagon);
INSERT INTO h3_test_gist (hex) SELECT :hexagon;
INSERT INTO h3_test_gist (hex) SELECT h3_cell_to_children(:hexagon);
INSERT INTO h3_test_gist (hex) SELECT h3_cell_to_center_child(:hexagon, 15);

--
-- TEST contains (@>)
--
-- parent and hexagon itself contain the hexagon
SELECT COUNT(*) = 2 FROM h3_test_gist WHERE hex @> :hexagon;

--
-- TEST contained by (<@)
--
-- children and center child are contained by hexagon (strict containment)
SELECT COUNT(*) = 8 FROM h3_test_gist WHERE hex <@ :hexagon;

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
SELECT COUNT(*) > 0 FROM h3_test_gist WHERE hex <-> :hexagon = 0;

-- cleanup
DROP TABLE h3_test_gist;
