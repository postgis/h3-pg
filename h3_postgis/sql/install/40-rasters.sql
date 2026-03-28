/*
 * Copyright 2023-2025 Zacharias Knudsen
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

--| # Raster processing functions

-- Get nodata value for ST_Clip function
-- ST_Clip sets nodata pixel values to minimum value by default, but it won't
-- set band nodata value in this case, which we need later for filtering dumped
-- values.
CREATE OR REPLACE FUNCTION __h3_raster_band_nodata(
    rast raster,
    nband integer)
RETURNS double precision
AS $$
    SELECT coalesce(
        @extschema:postgis_raster@.ST_BandNoDataValue(rast, nband),
        @extschema:postgis_raster@.ST_MinPossibleValue(@extschema:postgis_raster@.ST_BandPixelType(rast, nband)));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_to_polygon(
    rast raster,
    nband integer)
RETURNS geometry
AS $$
    SELECT @extschema:postgis_raster@.ST_MinConvexHull(rast, nband);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- Area of a pixel close to the center of raster polygon, in meters
CREATE OR REPLACE FUNCTION __h3_raster_polygon_pixel_area(
    rast raster,
    poly geometry)
RETURNS double precision
AS $$
    SELECT @extschema:postgis@.ST_Area(
        @extschema:postgis@.ST_Transform(
            @extschema:postgis_raster@.ST_PixelAsPolygon(
                rast,
                @extschema:postgis_raster@.ST_WorldToRasterCoordX(
                    rast,
                    @extschema:postgis@.ST_X(c),
                    @extschema:postgis@.ST_Y(c)
                ),
                @extschema:postgis_raster@.ST_WorldToRasterCoordY(
                    rast,
                    @extschema:postgis@.ST_X(c),
                    @extschema:postgis@.ST_Y(c)
                )),
            4326)::@extschema:postgis@.geography)
    FROM (
        SELECT @extschema:postgis@.ST_Centroid(poly) AS c
    ) AS centroid
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_polygon_centroid_cell(
    poly geometry,
    resolution integer)
RETURNS h3index
AS $$
    WITH centroid_cell AS (
        SELECT @extschema:h3@.h3_latlng_to_cell(
            (@extschema:postgis@.ST_Transform(@extschema:postgis@.ST_Centroid(poly), 4326))::point,
            resolution
        ) AS cell
    )
    SELECT CASE
        WHEN @extschema:h3@.h3_is_pentagon(cell) THEN (
            SELECT neighbor
            FROM @extschema:h3@.h3_grid_disk(cell) AS neighbor
            WHERE neighbor OPERATOR(@extschema:h3@.<>) cell
            LIMIT 1
        )
        ELSE
            cell
    END
    FROM centroid_cell;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Area of a cell close to the center of raster polygon, in meters
CREATE OR REPLACE FUNCTION __h3_raster_polygon_centroid_cell_area(
    poly geometry,
    resolution integer)
RETURNS double precision
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    area double precision;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT @extschema:postgis@.ST_Area(
            %1$I.h3_cell_to_boundary_geography(
                %1$I.__h3_raster_polygon_centroid_cell($1, $2)
            )
        )',
        self_schema
    )
    INTO area
    USING poly, resolution;

    RETURN area;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

-- Get list of cells inside of the raster polygon,
-- buffered by `buffer` value (in meters).
-- If SRID != 4326 then additionally buffer by 1 pixel to account for transformation.
CREATE OR REPLACE FUNCTION __h3_raster_polygon_to_cells(
    rast raster,
    poly geometry,
    resolution integer,
    buffer double precision)
RETURNS SETOF h3index
AS $$
    WITH buffered AS (
        SELECT CASE
            WHEN @extschema:postgis_raster@.ST_SRID(rast) != 4326 THEN
                @extschema:postgis@.ST_Transform(
                    @extschema:postgis@.ST_Buffer(
                        poly,
                        greatest(
                            @extschema:postgis_raster@.ST_PixelWidth(rast),
                            @extschema:postgis_raster@.ST_PixelHeight(rast)
                        ),
                        'join=mitre'
                    ),
                    4326
                )
            ELSE
                poly
        END AS geom
    ),
    searched AS (
        SELECT CASE
            WHEN buffer > 0.0 THEN
                @extschema:postgis@.ST_Buffer(
                    buffered.geom::@extschema:postgis@.geography,
                    buffer,
                    'join=mitre'
                )::@extschema:postgis@.geometry
            ELSE
                buffered.geom
        END AS geom
        FROM buffered
    )
    SELECT @extschema:h3@.h3_polygon_to_cells(exterior, holes, resolution)
    FROM (
        SELECT
            @extschema:postgis@.ST_MakePolygon(
                @extschema:postgis@.ST_ExteriorRing(poly_geom)
            )::polygon AS exterior,
            (
                SELECT pg_catalog.array_agg(hole)
                FROM (
                    SELECT @extschema:postgis@.ST_MakePolygon(
                        @extschema:postgis@.ST_InteriorRingN(
                            poly_geom,
                            pg_catalog.generate_series(
                                1,
                                @extschema:postgis@.ST_NumInteriorRings(poly_geom)
                            )
                        )
                    )::polygon AS hole
                ) AS q_hole
            ) AS holes
        FROM (
            SELECT (@extschema:postgis@.ST_Dump(searched.geom)).geom AS poly_geom
            FROM searched
        ) AS q_poly
        GROUP BY poly_geom
    ) AS h3_polygon_to_cells;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Get geometries of H3 cells interesecting raster polygon.
CREATE OR REPLACE FUNCTION __h3_raster_polygon_to_cell_boundaries_intersects(
    rast raster,
    poly geometry,
    resolution integer)
RETURNS TABLE (h3 h3index, geom geometry)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT h3, geom
         FROM
             %1$I.__h3_raster_polygon_to_cells(
                 $1,
                 $2,
                 $3,
                 @extschema:h3@.h3_get_hexagon_edge_length_avg($3, ''m'') * 1.3
             ) AS h3,
             @extschema:postgis@.ST_Transform(
                 %1$I.h3_cell_to_boundary_geometry(h3),
                 @extschema:postgis_raster@.ST_SRID($1)
             ) AS geom
         WHERE @extschema:postgis@.ST_Intersects(geom, $2)',
        self_schema
    )
    USING rast, poly, resolution;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

-- Get raster coordinates of H3 cells with centroids inside the raster polygon
CREATE OR REPLACE FUNCTION __h3_raster_polygon_to_cell_coords_centroid(
    rast raster,
    poly geometry,
    resolution integer)
RETURNS TABLE (h3 h3index, x integer, y integer)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'WITH
            geoms AS (
                SELECT
                    h3,
                    @extschema:postgis@.ST_Transform(
                        %1$I.h3_cell_to_geometry(h3),
                        @extschema:postgis@.ST_SRID($2)
                    ) AS geom
                FROM (
                    SELECT %1$I.__h3_raster_polygon_to_cells(
                        $1,
                        $2,
                        $3,
                        0.0
                    ) AS h3
                ) AS t
            ),
            coords AS (
                SELECT
                    h3,
                    @extschema:postgis_raster@.ST_WorldToRasterCoordX(
                        $1,
                        @extschema:postgis@.ST_X(geom),
                        @extschema:postgis@.ST_Y(geom)
                    ) AS x,
                    @extschema:postgis_raster@.ST_WorldToRasterCoordY(
                        $1,
                        @extschema:postgis@.ST_X(geom),
                        @extschema:postgis@.ST_Y(geom)
                    ) AS y
                FROM geoms
            )
         SELECT h3, x, y
         FROM coords
         WHERE
             x BETWEEN 1 AND @extschema:postgis_raster@.ST_Width($1)
             AND y BETWEEN 1 AND @extschema:postgis_raster@.ST_Height($1)',
        self_schema
    )
    USING rast, poly, resolution;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_polygon_to_cell_parts(
    rast raster,
    poly geometry,
    resolution integer,
    nband integer)
RETURNS TABLE (h3 h3index, part raster)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'WITH nodata AS (
             SELECT %1$I.__h3_raster_band_nodata($1, $4) AS value
         )
         SELECT c.h3, p AS part
         FROM
             nodata,
             %1$I.__h3_raster_polygon_to_cell_boundaries_intersects(
                 $1,
                 $2,
                 $3
             ) AS c,
             LATERAL @extschema:postgis_raster@.ST_Clip(
                 $1,
                 $4,
                 c.geom,
                 nodata.value,
                 TRUE
             ) AS p
         WHERE NOT @extschema:postgis_raster@.ST_BandIsNoData(p, $4)',
        self_schema
    )
    USING rast, poly, resolution, nband;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Get values corresponding to all H3 cells with centroids inside the
-- raster polygon. Assumes cell area is less than pixel area.
CREATE OR REPLACE FUNCTION __h3_raster_polygon_subpixel_cell_values(
    rast raster,
    poly geometry,
    resolution integer,
    nband integer)
RETURNS TABLE (h3 h3index, val double precision)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT
             h3,
             @extschema:postgis_raster@.ST_Value($1, $4, x, y) AS val
         FROM %1$I.__h3_raster_polygon_to_cell_coords_centroid($1, $2, $3)',
        self_schema
    )
    USING rast, poly, resolution, nband;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

--| ## Continuous raster data
--|
--| For rasters with pixel values representing continuous data (temperature, humidity,
--| elevation), the data inside H3 cells can be summarized by calculating number of
--| pixels, sum, mean, standard deviation, min and max for each cell inside a raster
--| and grouping these stats across multiple rasters by H3 index.
--|
--| ```
--| SELECT
--|     (summary).h3 AS h3,
--|     (h3_raster_summary_stats_agg((summary).stats)).*
--| FROM (
--|     SELECT h3_raster_summary(rast, 8) AS summary
--|     FROM rasters
--| ) t
--| GROUP BY 1;
--|
--|        h3        | count |        sum         |        mean         |       stddev       |  min  |       max
--| -----------------+-------+--------------------+---------------------+--------------------+-------+------------------
--|  882d638189fffff |    10 |  4.607657432556152 | 0.46076574325561526 | 1.3822972297668457 |     0 | 4.607657432556152
--|  882d64c4d1fffff |    10 | 3.6940908953547478 |  0.3694090895354748 |  1.099336879464068 |     0 | 3.667332887649536
--|  882d607431fffff |    11 |  6.219290263950825 |  0.5653900239955295 | 1.7624673707119065 |     0 | 6.13831996917724
--| <...>
--| ```

-- NOTE: `count` can be < 1 when cell area is less than pixel area
--@ availability: 4.1.1
CREATE TYPE h3_raster_summary_stats AS (
    count double precision,
    sum double precision,
    mean double precision,
    stddev double precision,
    min double precision,
    max double precision
);

-- ST_SummaryStats result type to h3_raster_summary_stats
CREATE OR REPLACE FUNCTION __h3_raster_to_summary_stats(stats @extschema:postgis_raster@.summarystats)
RETURNS h3_raster_summary_stats
AS $$
    SELECT
        (stats).count,
        (stats).sum,
        (stats).mean,
        (stats).stddev,
        (stats).min,
        (stats).max;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_summary_stats_agg_transfn(
    s1 h3_raster_summary_stats,
    s2 h3_raster_summary_stats)
RETURNS h3_raster_summary_stats
AS $$
    WITH total AS (
        SELECT
            (s1).count + (s2).count AS count,
            (s1).sum + (s2).sum AS sum)
    SELECT
        t.count,
        t.sum,
        t.sum / t.count,
        pg_catalog.sqrt(
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
    FROM total AS t;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

--@ availability: 4.1.1
CREATE AGGREGATE h3_raster_summary_stats_agg(h3_raster_summary_stats) (
    sfunc = __h3_raster_summary_stats_agg_transfn,
    stype = h3_raster_summary_stats,
    parallel = safe
);

CREATE OR REPLACE FUNCTION __h3_raster_polygon_summary_clip(
    rast raster,
    poly geometry,
    resolution integer,
    nband integer)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT
             h3,
             %1$I.__h3_raster_to_summary_stats(
                 @extschema:postgis_raster@.ST_SummaryStats(part, $4, TRUE)
             ) AS stats
         FROM %1$I.__h3_raster_polygon_to_cell_parts($1, $2, $3, $4)',
        self_schema
    )
    USING rast, poly, resolution, nband;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_summary_clip(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT summary.h3, summary.stats
         FROM %1$I.__h3_raster_polygon_summary_clip(
             $1,
             %1$I.__h3_raster_to_polygon($1, $3),
             $2,
             $3
         ) AS summary',
        self_schema
    )
    USING rast, resolution, nband;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary_clip(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Clips the raster by H3 cell geometries and processes each part separately.';

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_summary_centroids(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
    SELECT
        @extschema:h3@.h3_latlng_to_cell(
            (@extschema:postgis@.ST_Transform(geom, 4326))::point,
            resolution
        ) AS h3,
        ROW(
            pg_catalog.count(val),
            pg_catalog.sum(val),
            pg_catalog.avg(val),
            pg_catalog.stddev_pop(val),
            pg_catalog.min(val),
            pg_catalog.max(val)
        ) AS stats
    FROM @extschema:postgis_raster@.ST_PixelAsCentroids(rast, nband)
    GROUP BY 1;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary_centroids(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Finds corresponding H3 cell for each pixel, then groups values by H3 index.';

CREATE OR REPLACE FUNCTION __h3_raster_polygon_summary_subpixel(
    rast raster,
    poly geometry,
    resolution integer,
    nband integer,
    pixels_per_cell double precision)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT
             h3,
             ROW(
                 $5,
                 val,
                 val,
                 0.0,
                 val,
                 val
             )::%1$I.h3_raster_summary_stats AS stats
         FROM %1$I.__h3_raster_polygon_subpixel_cell_values($1, $2, $3, $4)',
        self_schema
    )
    USING rast, poly, resolution, nband, pixels_per_cell;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_summary_subpixel(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    poly @extschema:postgis@.geometry;
    pixel_area double precision;
    cell_area double precision;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_to_polygon($1, $2)',
        self_schema
    )
    INTO poly
    USING rast, nband;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_pixel_area($1, $2)',
        self_schema
    )
    INTO pixel_area
    USING rast, poly;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_centroid_cell_area($1, $2)',
        self_schema
    )
    INTO cell_area
    USING poly, resolution;

    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT (%1$I.__h3_raster_polygon_summary_subpixel(
            $1,
            $2,
            $3,
            $4,
            $5
        )).*',
        self_schema
    )
    USING rast, poly, resolution, nband, cell_area / pixel_area;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary_subpixel(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Assumes H3 cell is smaller than a pixel. Finds corresponding pixel for each H3 cell in raster.';

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_summary(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    poly @extschema:postgis@.geometry;
    cell_area double precision;
    pixel_area double precision;
    pixels_per_cell double precision;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_to_polygon($1, $2)',
        self_schema
    )
    INTO poly
    USING rast, nband;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_centroid_cell_area($1, $2)',
        self_schema
    )
    INTO cell_area
    USING poly, resolution;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_pixel_area($1, $2)',
        self_schema
    )
    INTO pixel_area
    USING rast, poly;

    pixels_per_cell := cell_area / pixel_area;

    IF pixels_per_cell > 70
        AND (
            @extschema:postgis@.ST_Area(
                @extschema:postgis@.ST_Transform(poly, 4326)::@extschema:postgis@.geography
            ) / cell_area
        ) > 10000 / (pixels_per_cell - 70)
    THEN
        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT (%1$I.__h3_raster_polygon_summary_clip(
                $1,
                $2,
                $3,
                $4
            )).*',
            self_schema
        )
        USING rast, poly, resolution, nband;
    ELSIF pixels_per_cell > 1 THEN
        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT (%1$I.h3_raster_summary_centroids(
                $1,
                $2,
                $3
            )).*',
            self_schema
        )
        USING rast, resolution, nband;
    ELSE
        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT (%1$I.__h3_raster_polygon_summary_subpixel(
                $1,
                $2,
                $3,
                $4,
                $5
            )).*',
            self_schema
        )
        USING rast, poly, resolution, nband, pixels_per_cell;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_summary(raster, integer, integer)
IS 'Returns `h3_raster_summary_stats` for each H3 cell in raster for a given band. Attempts to select an appropriate method based on number of pixels per H3 cell.';

--| ## Discrete raster data
--|
--| For rasters where pixels have discrete values corresponding to different classes
--| of land cover or land use, H3 cell data summary can be represented by a JSON object
--| with separate fields for each class. First, value, number of pixels and approximate
--| area are calculated for each H3 cell and value in a raster, then the stats are
--| grouped across multiple rasters by H3 index and value, and after that stats for
--| different values in a cell are combined into a single JSON object.
--|
--| The following example query additionally calculates a fraction of H3 cell pixels
--| for each value, using a window function to get a total number of pixels:
--| ```
--| WITH
--|     summary AS (
--|         -- get aggregated summary for each H3 index/value pair
--|         SELECT h3, val, h3_raster_class_summary_item_agg(summary) AS item
--|         FROM
--|             rasters,
--|             h3_raster_class_summary(rast, 8)
--|         GROUP BY 1, 2),
--|     summary_total AS (
--|         -- add total number of pixels per H3 cell
--|         SELECT h3, val, item, sum((item).count) OVER (PARTITION BY h3) AS total
--|         FROM summary)
--| SELECT
--|     h3,
--|     jsonb_object_agg(
--|         concat('class_', val::text),
--|         h3_raster_class_summary_item_to_jsonb(item)                 -- val, count, area
--|             || jsonb_build_object('fraction', (item).count / total) -- add fraction value
--|         ORDER BY val
--|     ) AS summary
--| FROM summary_total
--| GROUP BY 1;
--|
--|       h3        |                                                                            summary
--| ----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------
--| 88194e6f3bfffff | {"class_1": {"area": 75855.5748, "count": 46, "value": 1, "fraction": 0.4509}, "class_2": {"area": 92345.9171, "count": 56, "value": 2, "fraction": 0.5490}}
--| 88194e6f37fffff | {"class_1": {"area": 255600.3064, "count": 155, "value": 1, "fraction": 0.5}, "class_2": {"area": 255600.3064, "count": 155, "value": 2, "fraction": 0.5}}
--| 88194e6f33fffff | {"class_1": {"area": 336402.9840, "count": 204, "value": 1, "fraction": 0.5125}, "class_2": {"area": 319912.6416, "count": 194, "value": 2, "fraction": 0.4874}}
--| <...>
--| ```
--|
--| Area covered by pixels with the most frequent value in each cell:
--| ```
--| SELECT DISTINCT ON (h3)
--|     h3, val, (item).area
--| FROM (
--|     SELECT
--|         h3, val, h3_raster_class_summary_item_agg(summary) AS item
--|     FROM
--|         rasters,
--|         h3_raster_class_summary(rast, 8)
--|     GROUP BY 1, 2
--| ) t
--| ORDER BY h3, (item).count DESC;
--|
--|        h3        | val |        area
--| -----------------+-----+--------------------
--|  88194e6f3bfffff |   5 | 23238.699360251427
--|  88194e6f37fffff |   9 |  60863.26022922993
--|  88194e6f33fffff |   8 |  76355.72646939754
--| <...>
--| ```

-- NOTE: `count` can be < 1 when cell area is less than pixel area
--@ availability: 4.1.1
CREATE TYPE h3_raster_class_summary_item AS (
    val integer,
    count double precision,
    area double precision
);

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_class_summary_item_to_jsonb(
    item h3_raster_class_summary_item)
RETURNS jsonb
AS $$
    SELECT pg_catalog.jsonb_build_object(
        'value', (item).val,
        'count', (item).count,
        'area', (item).area
    );
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_class_summary_item_to_jsonb(h3_raster_class_summary_item)
IS 'Convert raster summary to JSONB, example: `{"count": 10, "value": 2, "area": 16490.3423}`';

CREATE OR REPLACE FUNCTION __h3_raster_class_summary_item_agg_transfn(
    s1 h3_raster_class_summary_item,
    s2 h3_raster_class_summary_item)
RETURNS h3_raster_class_summary_item
AS $$
    SELECT
        (s1).val,
        (s1).count + (s2).count,
        (s1).area + (s2).area;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

--@ availability: 4.1.1
CREATE AGGREGATE h3_raster_class_summary_item_agg(h3_raster_class_summary_item) (
    stype = h3_raster_class_summary_item,
    sfunc = __h3_raster_class_summary_item_agg_transfn,
    parallel = safe
);

CREATE OR REPLACE FUNCTION __h3_raster_class_summary_part(
    rast raster,
    nband integer,
    pixel_area double precision)
RETURNS SETOF h3_raster_class_summary_item
AS $$
    SELECT
        value::integer,
        count::double precision,
        count * pixel_area
    FROM @extschema:postgis_raster@.ST_ValueCount(rast, nband, TRUE) t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_class_polygon_summary_clip(
    rast raster,
    poly geometry,
    resolution integer,
    nband integer,
    pixel_area double precision)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'WITH summary AS (
             SELECT
                 h3,
                 %1$I.__h3_raster_class_summary_part(part, $5, $6) AS summary
             FROM %1$I.__h3_raster_polygon_to_cell_parts($1, $2, $3, $4)
         )
         SELECT h3, (summary).val, summary
         FROM summary',
        self_schema
    )
    USING rast, poly, resolution, nband, nband, pixel_area;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_class_summary_clip(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    poly @extschema:postgis@.geometry;
    pixel_area double precision;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_to_polygon($1, $2)',
        self_schema
    )
    INTO poly
    USING rast, nband;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_pixel_area($1, $2)',
        self_schema
    )
    INTO pixel_area
    USING rast, poly;

    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT (%1$I.__h3_raster_class_polygon_summary_clip(
            $1,
            $2,
            $3,
            $4,
            $5
        )).*',
        self_schema
    )
    USING rast, poly, resolution, nband, pixel_area;
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
        @extschema:h3@.h3_latlng_to_cell(
            (@extschema:postgis@.ST_Transform(geom, 4326))::point,
            resolution
        ) AS h3,
        val::integer AS val,
        ROW(
            val::integer,
            pg_catalog.count(*)::double precision,
            pg_catalog.count(*) * pixel_area
        ) AS summary
    FROM @extschema:postgis_raster@.ST_PixelAsCentroids(rast, nband)
    GROUP BY 1, 2;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_class_summary_centroids(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    poly @extschema:postgis@.geometry;
    pixel_area double precision;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_to_polygon($1, $2)',
        self_schema
    )
    INTO poly
    USING rast, nband;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_pixel_area($1, $2)',
        self_schema
    )
    INTO pixel_area
    USING rast, poly;

    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT (%1$I.__h3_raster_class_summary_centroids(
            $1,
            $2,
            $3,
            $4
        )).*',
        self_schema
    )
    USING rast, resolution, nband, pixel_area;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_class_summary_centroids(raster, integer, integer)
IS 'Returns `h3_raster_class_summary_item` for each H3 cell and value for a given band. Finds corresponding H3 cell for each pixel, then groups by H3 and value.';

CREATE OR REPLACE FUNCTION __h3_raster_class_polygon_summary_subpixel(
    rast raster,
    poly geometry,
    resolution integer,
    nband integer,
    cell_area double precision,
    pixel_area double precision)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
BEGIN
    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT
             h3,
             val::integer AS val,
             ROW(
                 val::integer,
                 $5 / $6,
                 $5
             )::%1$I.h3_raster_class_summary_item AS summary
         FROM %1$I.__h3_raster_polygon_subpixel_cell_values($1, $2, $3, $4)',
        self_schema
    )
    USING rast, poly, resolution, nband, cell_area, pixel_area;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_class_summary_subpixel(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    poly @extschema:postgis@.geometry;
    cell_area double precision;
    pixel_area double precision;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_to_polygon($1, $2)',
        self_schema
    )
    INTO poly
    USING rast, nband;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_centroid_cell_area($1, $2)',
        self_schema
    )
    INTO cell_area
    USING poly, resolution;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_pixel_area($1, $2)',
        self_schema
    )
    INTO pixel_area
    USING rast, poly;

    RETURN QUERY EXECUTE pg_catalog.format(
        'SELECT (%1$I.__h3_raster_class_polygon_summary_subpixel(
            $1,
            $2,
            $3,
            $4,
            $5,
            $6
        )).*',
        self_schema
    )
    USING rast, poly, resolution, nband, cell_area, pixel_area;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_raster_class_summary_subpixel(raster, integer, integer)
IS 'Returns `h3_raster_class_summary_item` for each H3 cell and value for a given band. Assumes H3 cell is smaller than a pixel. Finds corresponding pixel for each H3 cell in raster.';

--@ availability: 4.1.1
CREATE OR REPLACE FUNCTION h3_raster_class_summary(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    poly @extschema:postgis@.geometry;
    cell_area double precision;
    pixel_area double precision;
    pixels_per_cell double precision;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_to_polygon($1, $2)',
        self_schema
    )
    INTO poly
    USING rast, nband;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_centroid_cell_area($1, $2)',
        self_schema
    )
    INTO cell_area
    USING poly, resolution;

    EXECUTE pg_catalog.format(
        'SELECT %1$I.__h3_raster_polygon_pixel_area($1, $2)',
        self_schema
    )
    INTO pixel_area
    USING rast, poly;

    pixels_per_cell := cell_area / pixel_area;

    IF pixels_per_cell > 70
        AND (
            @extschema:postgis@.ST_Area(
                @extschema:postgis@.ST_Transform(poly, 4326)::@extschema:postgis@.geography
            ) / cell_area
        ) > 10000 / (pixels_per_cell - 70)
    THEN
        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT (%1$I.__h3_raster_class_polygon_summary_clip(
                $1,
                $2,
                $3,
                $4,
                $5
            )).*',
            self_schema
        )
        USING rast, poly, resolution, nband, pixel_area;
    ELSIF pixels_per_cell > 1 THEN
        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT (%1$I.__h3_raster_class_summary_centroids(
                $1,
                $2,
                $3,
                $4
            )).*',
            self_schema
        )
        USING rast, resolution, nband, pixel_area;
    ELSE
        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT (%1$I.__h3_raster_class_polygon_summary_subpixel(
                $1,
                $2,
                $3,
                $4,
                $5,
                $6
            )).*',
            self_schema
        )
        USING rast, poly, resolution, nband, cell_area, pixel_area;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION h3_raster_class_summary(raster, integer, integer)
IS 'Returns `h3_raster_class_summary_item` for each H3 cell and value for a given band. Attempts to select an appropriate method based on number of pixels per H3 cell.';
