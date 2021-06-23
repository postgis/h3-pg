\pset tuples_only on
\set hexagon '\'880326b88dfffff\''
\set origin '\'880326b887fffff\''
\set pentagon '\'831c00fffffffff\''

--
-- TEST h3_grid_disk and h3_grid_ring_unsafe
--

-- gridDisk 0 is input index

SELECT h3_grid_disk(:hexagon, 0) = :hexagon;

-- gridDisk 2 is same as sum of gridRing 0, 1 and 2

SELECT array_agg(r) is null FROM (
    SELECT h3_grid_disk(:hexagon, 2) r
    EXCEPT (
        SELECT h3_grid_ring_unsafe(:hexagon, 0) r
        UNION SELECT h3_grid_ring_unsafe(:hexagon, 1) r
        UNION SELECT h3_grid_ring_unsafe(:hexagon, 2) r
    )
) q;

--
-- TEST h3_grid_disk_distances
--

-- correct number of indexes at distances 0, 1 and 2 for k=2
SELECT COUNT(index) filter (WHERE distance = 0) = 1
AND COUNT(index) filter (WHERE distance = 1) = 6
AND COUNT(index) filter (WHERE distance = 2) = 12
FROM (
    SELECT index, distance FROM h3_grid_disk_distances(:hexagon, 2)
) q;

-- same for pentagon
SELECT COUNT(index) filter (WHERE distance = 0) = 1
AND COUNT(index) filter (WHERE distance = 1) = 5
AND COUNT(index) filter (WHERE distance = 2) = 10
FROM (
    SELECT index, distance FROM h3_grid_disk_distances(:pentagon, 2)
) q;

--
-- TEST h3_grid_path_cells
--

SELECT ARRAY(SELECT h3_grid_path_cells('841c023ffffffff', '841c025ffffffff'))
    = ARRAY['841c023ffffffff','841c027ffffffff','841c025ffffffff']::h3index[];

--
-- TEST h3_grid_distance
--

-- returns 1 for indexes with one index between them
SELECT h3_grid_distance('880326b881fffff', '880326b885fffff') = 1;

-- returns -1 for invalid inputs
SELECT h3_grid_distance('880326b881fffff', h3_cell_to_parent('880326b885fffff')) = -1;

--
-- TEST h3_experimental_h3_to_local_ij and h3_experimental_local_ij_to_h3
--

-- they are inverse of each others
SELECT :hexagon = h3_experimental_local_ij_to_h3(:origin, h3_experimental_h3_to_local_ij(:origin, :hexagon));