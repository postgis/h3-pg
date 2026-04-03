\pset tuples_only on

\set resolution 9
\set coverage_size 2
\set raster_size 25
\set pixel_size 0.0005
\set value_num 5

\set lat 51.5
\set lng -0.025

-- Regression stability:
-- This check is sensitive to session state after grouped raster summaries.
-- Keep it isolated in its own regression script.
SET max_parallel_workers_per_gather TO 0;

CREATE TABLE h3_test_rasters (id SERIAL, rast raster);

INSERT INTO h3_test_rasters (rast) (
    WITH
        vals AS (
            SELECT array_agg(row ORDER BY y) AS vals
            FROM (
                SELECT
                    y,
                    array_agg((x + y) % :value_num + 1 ORDER BY x) AS row
                FROM
                    generate_series(1, :raster_size) AS x,
                    generate_series(1, :raster_size) AS y
                GROUP BY y
            ) t),
        rasts AS (
            SELECT
                ST_AddBand(
                    ST_MakeEmptyCoverage(
                        :raster_size, :raster_size,
                        :raster_size * :coverage_size, :raster_size * :coverage_size,
                        :lng, :lat,
                        :pixel_size, -(:pixel_size),
                        0, 0,
                        4326),
                    ARRAY[ROW(1, '8BUI', 1, 0)]::addbandarg[]
                ) AS rast)
    SELECT ST_SetValues(r.rast, 1, 1, 1, v.vals)
    FROM rasts r, vals v
);

CREATE FUNCTION h3_test_equal(
    v1 double precision,
    v2 double precision)
RETURNS boolean
AS $$
    SELECT ABS(v1 - v2) < 1e-12;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION h3_test_raster_summary_stats_equal(
    s1 h3_raster_summary_stats,
    s2 h3_raster_summary_stats)
RETURNS boolean
AS $$
    SELECT s1 IS NOT NULL AND s2 IS NOT NULL
        AND h3_test_equal((s1).count, (s2).count)
        AND h3_test_equal((s1).sum, (s2).sum)
        AND h3_test_equal((s1).mean, (s2).mean)
        AND h3_test_equal((s1).stddev, (s2).stddev)
        AND h3_test_equal((s1).min, (s2).min)
        AND h3_test_equal((s1).max, (s2).max);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- Stats aggregation check:
-- stats for a cell intersecting multiple rasters (with aggregation) should be
-- the same when calculated on a union of rasters (without aggregation).
WITH
    rast AS (
        -- Union all test rasters
        SELECT ST_Union(rast ORDER BY id) AS rast FROM h3_test_rasters),
    middle AS (
        -- Find an H3 cell in a bottom-right corner of a first raster
        -- (intersecting 4 rasters)
        SELECT
            h3_latlng_to_cell(
                ST_MakePoint(
                    ST_RasterToWorldCoordX(rast, :raster_size),
                    ST_RasterToWorldCoordY(rast, :raster_size)),
                :resolution
            ) AS h3
        FROM rast),
    summary1 AS (
        -- Get summary from combined raster
        SELECT t.stats
        FROM (
            -- h3, stats
            SELECT (h3_raster_summary_clip(rast, :resolution)).*
            FROM rast
        ) t, middle m
        WHERE t.h3 = m.h3),
    summary2 AS (
        -- Get aggregates summary from separate rasters
        SELECT h3_raster_summary_stats_agg(t.stats ORDER BY t.id) AS stats
        FROM (
            -- id, h3, stats
            SELECT r.id, (h3_raster_summary_clip(r.rast, :resolution)).*
            FROM h3_test_rasters r
        ) t, middle m
        WHERE t.h3 = m.h3
        GROUP BY t.h3)
SELECT h3_test_raster_summary_stats_equal(s1.stats, s2.stats)
FROM summary1 s1, summary2 s2;

DROP FUNCTION h3_test_raster_summary_stats_equal(
    h3_raster_summary_stats,
    h3_raster_summary_stats);
DROP FUNCTION h3_test_equal(double precision, double precision);

DROP TABLE h3_test_rasters;
