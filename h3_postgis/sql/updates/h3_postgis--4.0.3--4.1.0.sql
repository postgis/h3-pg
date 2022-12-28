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
\echo Use "ALTER EXTENSION h3_postgis UPDATE TO '4.1.0'" to load this file. \quit

CREATE OR REPLACE FUNCTION
    h3_cell_to_boundary_wkb(cell h3index) RETURNS bytea
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_cell_to_boundary_wkb(h3index)
IS 'Finds the boundary of the index, converts to EWKB.

Splits polygons when crossing 180th meridian.

This function has to return WKB since Postgres does not provide multipolygon type.';

CREATE OR REPLACE FUNCTION
    h3_cells_to_multi_polygon_wkb(h3index[]) RETURNS bytea
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_cells_to_multi_polygon_wkb(h3index[])
IS 'Create a LinkedGeoPolygon describing the outline(s) of a set of hexagons, converts to EWKB.

Splits polygons when crossing 180th meridian.';

-- Raster processing

-- Get nodata value for ST_Clip function
CREATE OR REPLACE FUNCTION __h3_raster_band_nodata(
    rast raster,
    nband integer)
RETURNS double precision
AS $$
    SELECT coalesce(
        ST_BandNoDataValue(rast, nband),
        ST_MinPossibleValue(ST_BandPixelType(rast, nband)));
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_pixel_area(rast raster)
RETURNS double precision
AS $$
    SELECT ST_Area(ST_PixelAsPolygon(rast, 1, 1)::geography);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Get area of H3 cell close to the center of the raster
CREATE OR REPLACE FUNCTION __h3_raster_cell_area(rast raster, resolution integer)
RETURNS double precision
AS $$
DECLARE
    rast_geom CONSTANT geometry := __h3_raster_to_polygon(rast, nband);
BEGIN
    SELECT ST_Area(
        h3_cell_to_boundary_geography(
            ST_Transform(ST_Centroid(rast_geom), 4326),
            resolution));
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_to_polygon(rast raster, nband integer)
RETURNS geometry
AS $$
    SELECT ST_MinConvexHull(rast, nband);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Get all H3 cells potentially intersecting the polygon,
-- result may contain cells outside of polygon
CREATE OR REPLACE FUNCTION __h3_raster_polygon_to_cells(
    poly geometry,
    resolution integer)
RETURNS SETOF h3index
AS $$
    SELECT h3_polygon_to_cells(
        ST_Buffer(
            ST_Transform(poly, 4326)::geography,
            h3_get_hexagon_edge_length_avg(resolution, 'm') * 1.3),
        resolution);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Get H3 cell geometries intersecting the raster
CREATE OR REPLACE FUNCTION __h3_raster_to_cell_boundaries(
    rast raster,
    resolution integer,
    nband integer)
RETURNS TABLE (h3 h3index, geom geometry)
AS $$
DECLARE
    rast_geom CONSTANT geometry := __h3_raster_to_polygon(rast, nband);
BEGIN
    RETURN QUERY
    WITH
        geoms AS (
            SELECT
                c.h3,
                ST_Transform(h3_cell_to_boundary_geometry(c.h3), ST_SRID(rast)) AS geom
            FROM (
                SELECT __h3_raster_polygon_to_cells(rast_geom, resolution) AS h3
            ) c)
    SELECT g.*
    FROM geoms g
    WHERE ST_Intersects(g.geom, rast_geom);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Get H3 cell centroids within the raster
CREATE OR REPLACE FUNCTION __h3_raster_to_cell_centroids(
    rast raster,
    resolution integer,
    nband integer)
RETURNS TABLE (h3 h3index, geom geometry)
AS $$
DECLARE
    rast_geom CONSTANT geometry := __h3_raster_to_polygon(rast, nband);
BEGIN
    RETURN QUERY
    SELECT
        c.h3,
        ST_Transform(h3_cell_to_geometry(c.h3), ST_SRID(rast)) AS geom
    FROM (
        SELECT h3_polygon_to_cells(rast_geom, resolution) AS h3
    ) c;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Raster processing: continuous data

CREATE TYPE h3_raster_summary_stats AS (
    count double precision,
    sum double precision,
    mean double precision,
    stddev double precision,
    min double precision,
    max double precision
);

-- ST_SummaryStats result type to h3_raster_summary_stats
CREATE OR REPLACE FUNCTION __h3_raster_to_summary_stats(stats summarystats)
RETURNS h3_raster_summary_stats
AS $$
    SELECT ROW(
        (stats).count,
        (stats).sum,
        (stats).mean,
        (stats).stddev,
        (stats).min,
        (stats).max
    )::h3_raster_summary_stats
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_summary_stats_agg_transfn(
    s1 h3_raster_summary_stats,
    s2 h3_raster_summary_stats)
RETURNS h3_raster_summary_stats
AS $$
    WITH total AS (
        SELECT
            (s1).count + (s2).count AS count,
            (s1).sum + (s2).sum AS sum)
    SELECT ROW(
        t.count,
        t.sum,
        t.sum / t.count,
        sqrt(
            (
                -- sum of squared values: (variance + mean squared) * count
                (((s1).stddev * (s1).stddev + (s1).mean * (s1).mean)) * (s1).count
                + (((s2).stddev * (s2).stddev + (s2).mean * (s2).mean)) * (s2).count
            )
            / t.count
            - ((t.sum * t.sum) / (t.count * t.count)) -- mean squared
        ),
        least((s1).min, (s2).min),
        greatest((s1).max, (s2).max)
    )::h3_raster_summary_stats
    FROM total AS t
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE AGGREGATE h3_raster_summary_stats_agg(h3_raster_summary_stats) (
    sfunc = __h3_raster_summary_stats_agg_transfn,
    stype = h3_raster_summary_stats,
    parallel = safe
);

CREATE OR REPLACE FUNCTION h3_raster_summary_clip(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    nodata CONSTANT double precision := __h3_raster_band_nodata(rast, nband);
BEGIN
    RETURN QUERY
    WITH parts AS (
        SELECT
            g.h3,
            ST_Clip(rast, nband, g.geom, nodata) AS part
        FROM (
            -- h3, geom
            SELECT (__h3_raster_to_cell_boundaries(rast, resolution, nband)).*
        ) g)
    SELECT
        p.h3,
        __h3_raster_to_summary_stats(ST_SummaryStats(part, nband, TRUE)) AS stats
    FROM parts AS p
    WHERE NOT ST_BandIsNoData(part, 1);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary_clip(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Clips the raster by H3 cell geometries and processes each part separately.';

CREATE OR REPLACE FUNCTION h3_raster_summary_centroids(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
    WITH pixels AS (
        -- x, y, val, geom
        SELECT (ST_PixelAsCentroids(rast, nband)).*)
    SELECT
        h3_lat_lng_to_cell(geom, resolution) AS h3,
        ROW(
            count(val),
            sum(val),
            avg(val),
            stddev_pop(val),
            min(val),
            max(val)
        )::h3_raster_summary_stats AS stats
    FROM pixels
    GROUP BY 1
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary_centroids(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Finds corresponding H3 cell for each pixel, then groups values by H3 index.';

CREATE OR REPLACE FUNCTION __h3_raster_summary_subpixel(
    rast raster,
    resolution integer,
    nband integer,
    pixel_area double precision,
    cell_area double precision)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
    WITH
        vals AS (
            SELECT
                h3,
                ST_Value(
                    rast,
                    nband,
                    ST_WorldToRasterCoordX(rast, geom),
                    ST_WorldToRasterCoordY(rast, geom)
                ) AS val
            FROM (
                -- h3, geom
                SELECT (__h3_raster_to_cell_centroids(rast, resolution, nband)).*
            ) t)
    SELECT
        h3,
        ROW(
            cell_area / pixel_area, -- count
            val, -- sum
            val, -- mean
            0,   -- stddev
            val, -- min
            val  -- max
        )::h3_rasters_summary_stats AS stats
    FROM vals;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_raster_summary_subpixel(
    rast raster,
    resolution integer,
    nband integer DEFAUlT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    pixel_area CONSTANT double precision := __h3_raster_pixel_area(rast);
    cell_area CONSTANT double precision := __h3_raster_cell_area(rast, resolution);
BEGIN
    RETURN QUERY SELECT (__h3_raster_summary_subpixel(rast, resolution, nband, pixel_area, cell_area)).*;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary_subpixel(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Assumes H3 cell is smaller than a pixel. Finds corresponding pixel for each H3 cell in raster.';

CREATE OR REPLACE FUNCTION h3_raster_summary(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    pixel_area CONSTANT double precision := __h3_raster_pixel_area(rast);
    cell_area CONSTANT double precision := __h3_raster_cell_area(rast, resolution);
    pixels_per_cell CONSTANT double precision := cell_area / pixel_area;
BEGIN
    IF pixels_per_cell > 350 THEN 
        RETURN QUERY SELECT (h3_raster_summary_clip(rast, resolution, nband)).*;
    ELSIF pixels_per_cell > 1 THEN
        RETURN QUERY SELECT (h3_raster_summary_centroids(rast, resolution, nband)).*;
    ELSE
        RETURN QUERY SELECT (__h3_raster_summary_subpixel(rast, resolution, nband, pixel_area, cell_area)).*;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Attempts to select an appropriate method based on number of pixels per H3 cell.';

-- Raster processing: discrete data

CREATE TYPE h3_raster_class_summary_item AS (
    val integer,
    count double precision,
    area double precision
);

CREATE OR REPLACE FUNCTION h3_raster_class_summary_item_to_jsonb(
    item h3_raster_class_summary_item)
RETURNS jsonb
AS $$
    SELECT jsonb_build_object(
        'value', (item).val,
        'count', (item).count,
        'area', (item).area
    );
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_class_summary_item_to_jsonb(h3_raster_class_summary_item)
IS 'Convert raster summary to binary JSON.';

CREATE OR REPLACE FUNCTION __h3_raster_class_summary_item_agg_transfn(
    s1 h3_raster_class_summary_item,
    s2 h3_raster_class_summary_item)
RETURNS h3_raster_class_summary_item
AS $$
    SELECT
        s1.val,
        s1.count + s2.count,
        s1.area + s2.area
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE AGGREGATE h3_raster_class_summary_item_agg(h3_raster_class_summary_item) (
    stype = h3_raster_class_summary_item,
    sfunc = __h3_raster_class_summary_item_agg_transfn,
    parallel = safe
);

-- Get summary items for a raster clipped by H3 cell geometry
CREATE OR REPLACE FUNCTION __h3_raster_class_summary_part(
    rast raster,
    nband integer,
    pixel_area double precision)
RETURNS SETOF h3_raster_class_summary_item
AS $$
    WITH
        vals AS (SELECT unnest(ST_DumpValues(rast, nband)) AS val)
    SELECT
        vals.val::integer,
        count(*)::double precision,
        count(*) * pixel_area
    FROM vals
    WHERE val IS NOT NULL
    GROUP BY 1
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_class_summary_clip(
    rast raster,
    resolution integer,
    nband integer,
    pixel_area double precision)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    nodata CONSTANT double precision := __h3_raster_band_nodata(rast, nband);
BEGIN
    RETURN QUERY
    WITH
        parts AS (
            SELECT
                g.h3,
                ST_Clip(rast, nband, g.geom, nodata, TRUE) AS part
            FROM (
                -- h3, geom
                SELECT (__h3_raster_to_cell_boundaries(rast, resolution, nband)).*
            ) g),
        summary AS (
            SELECT
                p.h3,
                __h3_raster_class_summary_part(part, nband, pixel_area) AS summary
            FROM parts p)
    SELECT s.h3, (s.summary).val, s.summary
    FROM summary s;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_raster_class_summary_clip(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    pixel_area CONSTANT double precision := __h3_raster_pixel_area(rast);
BEGIN
    RETURN QUERY SELECT (__h3_raster_class_summary_clip(rast, resolution, nband, pixel_area)).*;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_class_summary_clip(raster, integer, integer)
IS 'Returns `h3_raster_class_summary_item` for each H3 cell and value for a given band. Clips the raster by H3 cell geometries and processes each part separately.';


CREATE OR REPLACE FUNCTION __h3_raster_class_summary_centroids(
    rast raster,
    resolution integer,
    nband integer,
    pixel_area double precision)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
    SELECT
        h3_lat_lng_to_cell(geom, resolution) AS h3,
        val::integer AS val,
        ROW(
            val::integer,
            count(*)::double precision,
            count(*) * pixel_area
        )::h3_raster_class_summary_item AS summary
    FROM (
        -- x, y, val, geom
        SELECT (ST_PixelAsCentroids(rast, nband)).*
    ) c
    GROUP BY 1, 2;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


-- For each pixel determine which H3 cell it belongs to then group by H3 index and value.
CREATE OR REPLACE FUNCTION h3_raster_class_summary_centroids(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    pixel_area CONSTANT double precision := __h3_raster_pixel_area(rast);
BEGIN
    RETURN QUERY SELECT (__h3_raster_class_summary_centroids(rast, resolution, nband, pixel_area)).*;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_class_summary_centroids(raster, integer, integer)
IS 'Returns `h3_raster_class_summary_item` for each H3 cell and value for a given band. Finds corresponding H3 cell for each pixel, then groups by H3 and value.';

CREATE OR REPLACE FUNCTION __h3_raster_class_summary_subpixel(
    rast raster,
    resolution integer,
    nband integer,
    pixel_area double precision,
    cell_area double precision)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
    WITH
        vals AS (
            SELECT
                h3,
                ST_Value(
                    rast,
                    nband,
                    ST_WorldToRasterCoordX(rast, geom),
                    ST_WorldToRasterCoordY(rast, geom)
                ) AS val
            FROM (
                -- h3, geom
                SELECT (__h3_raster_to_cell_centroids(rast, resolution, nband)).*
            ) c)
    SELECT
        h3,
        val::integer AS val,
        ROW(
            val::integer,
            cell_area / pixel_area,
            cell_area
        )::h3_raster_class_summary_item AS summary
    FROM vals v;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Get summary items for each H3 index and value.
-- For each H3 cell centroid determine which pixel it belongs to.
CREATE OR REPLACE FUNCTION h3_raster_class_summary_subpixel(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    cell_area CONSTANT double precision := __h3_raster_cell_area(rast, resolution);
    pixel_area CONSTANT double precision := __h3_raster_pixel_area(rast);
BEGIN
    RETURN QUERY SELECT (__h3_raster_class_summary_subpixel(rast, resolution, nband, pixel_area, cell_area)).*;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_class_summary_subpixel(raster, integer, integer)
IS 'Returns `h3_raster_class_summary_item` for each H3 cell and value for a given band. Assumes H3 cell is smaller than a pixel. Finds corresponding pixel for each H3 cell in raster.';


-- Get summary items for each H3 index and value.
-- Select appropriate method based on number of pixels per H3 cell.
CREATE OR REPLACE FUNCTION h3_raster_class_summary(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
     pixel_area CONSTANT double precision := __h3_raster_pixel_area(rast);
     cell_area CONSTANT double precision := __h3_raster_cell_area(rast, resolution);
     pixels_per_cell CONSTANT double precision := cell_area / pixel_area;
BEGIN
    IF pixels_per_cell > 350 THEN
        RETURN QUERY SELECT (__h3_raster_class_summary_clip(rast, resolution, nband, pixel_area)).*;
    ELSIF pixels_per_cell > 1 THEN
        RETURN QUERY SELECT (__h3_raster_class_summary_centroids(rast, resolution, nband, pixel_area)).*;
    ELSE
        RETURN QUERY SELECT (__h3_raster_class_summary_subpixel(rast, resolution, nband, pixel_area, cell_area)).*;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION h3_raster_class_summary(raster, integer, integer)
IS 'Returns `h3_raster_class_summary_item` for each H3 cell and value for a given band. Attempts to select an appropriate method based on number of pixels per H3 cell.';
