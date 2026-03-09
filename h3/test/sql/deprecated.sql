\pset tuples_only on

-- neighbouring indexes (one hexagon, one pentagon) at resolution 3
\set geo POINT(-144.52399108028, 49.7165031828995)
\set hexagon '\'831c02fffffffff\'::h3index'
\set resolution 3

SELECT h3_cell_to_lat_lng(:hexagon) ~= :geo
    AND h3_cell_to_lat_lng(:hexagon) ~= :geo;
SELECT h3_lat_lng_to_cell(:geo, :resolution) = :hexagon
    AND h3_lat_lng_to_cell(:geo, :resolution) = :hexagon;
SELECT count(*) = 10 FROM (
    SELECT h3_lat_lng_to_cell(
        POINT(-144.52399108028 + i * 1e-9, 49.7165031828995),
        :resolution
    ) AS cell
    FROM generate_series(1, 10) AS i
) s
WHERE s.cell = :hexagon;
