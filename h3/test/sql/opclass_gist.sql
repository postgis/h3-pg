\pset tuples_only on
\set hexagon '\'831c02fffffffff\'::h3index'

CREATE TABLE h3_test_gist (cell h3index);
CREATE INDEX h3_test_gist_index
          ON h3_test_gist
       USING gist(cell experimental_h3index_ops);

-- insert data
INSERT INTO h3_test_gist (cell) SELECT h3_cell_to_parent(:hexagon);
INSERT INTO h3_test_gist (cell) SELECT h3_cell_to_children(:hexagon);
INSERT INTO h3_test_gist (cell) SELECT h3_cell_to_center_child(:hexagon, 15);

--
-- TEST GiST
--
SELECT COUNT(*) = 1 FROM h3_test_gist WHERE cell @> :hexagon;
SELECT COUNT(*) = 8 FROM h3_test_gist WHERE cell <@ :hexagon;

INSERT INTO h3_test_gist (cell) SELECT h3_grid_disk(:hexagon, 2);
SELECT COUNT(*) = 8 FROM h3_test_gist WHERE cell <-> :hexagon = 2;

--
-- BREAK GIST
--

SELECT COUNT(*) = 8 FROM h3_test_gist WHERE cell <@ :hexagon;
SELECT COUNT(*) FROM h3_test_gist WHERE cell <@ :hexagon;
INSERT INTO h3_test_gist (cell) SELECT h3_cell_to_children(:hexagon, 8);
SELECT COUNT(*) > 8 FROM h3_test_gist WHERE cell <@ :hexagon;
