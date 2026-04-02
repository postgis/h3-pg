/*
 * Copyright 2022-2025 Zacharias Knudsen
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

--| # PostGIS Region Functions
--|
--| Note: `h3_polygon_to_cells*` assumes valid polygonal input in SRID 4326.
--| If results look surprising, start by checking `ST_IsValid()` and `ST_SRID()`
--| (and consider `SET h3.strict TO true` to catch out-of-range lon/lat).
--| For collections, extract polygonal parts first: `ST_CollectionExtract(geom, 3)`.
--| The "PostGIS Integration" section includes a validation/repair pattern.

--@ availability: 4.0.0
--@ refid: h3_polygon_to_cells_geometry
CREATE OR REPLACE FUNCTION h3_polygon_to_cells(multi geometry, resolution integer) RETURNS SETOF h3index
    AS $$ SELECT @extschema:h3@.h3_polygon_to_cells(exterior, holes, resolution) FROM (
        SELECT 
            -- extract exterior ring of each polygon
            @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_ExteriorRing(poly))::polygon exterior,
            -- extract holes of each polygon
            (SELECT pg_catalog.array_agg(hole)
                FROM (
                    SELECT @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_InteriorRingN(
                        poly,
                        pg_catalog.generate_series(1, @extschema:postgis@.ST_NumInteriorRings(poly))
                    ))::polygon AS hole
                ) q_hole
            ) holes
        -- extract single polygons from multipolygon
        FROM (
            SELECT (@extschema:postgis@.ST_Dump(multi)).geom AS poly
        ) q_poly GROUP BY poly
    ) h3_polygon_to_cells; $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT; -- NOT STRICT
COMMENT ON FUNCTION
    h3_polygon_to_cells(geometry, integer)
IS 'Converts polygonal geometry to H3 cells.

See "PostGIS Integration" for SRID/validity requirements.';

--@ availability: 4.0.0
--@ refid: h3_polygon_to_cells_geography
CREATE OR REPLACE FUNCTION h3_polygon_to_cells(multi geography, resolution integer) RETURNS SETOF h3index
    AS $$ SELECT @extschema:h3@.h3_polygon_to_cells(exterior, holes, resolution) FROM (
        SELECT
            @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_ExteriorRing(poly))::polygon exterior,
            (SELECT pg_catalog.array_agg(hole)
                FROM (
                    SELECT @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_InteriorRingN(
                        poly,
                        pg_catalog.generate_series(1, @extschema:postgis@.ST_NumInteriorRings(poly))
                    ))::polygon AS hole
                ) q_hole
            ) holes
        FROM (
            SELECT (@extschema:postgis@.ST_Dump($1::@extschema:postgis@.geometry)).geom AS poly
        ) q_poly GROUP BY poly
    ) h3_polygon_to_cells; $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT; -- NOT STRICT
COMMENT ON FUNCTION
    h3_polygon_to_cells(geography, integer)
IS 'Converts polygonal geography to H3 cells.

See "PostGIS Integration" for SRID/validity requirements.';

--@ availability: 4.1.0
--@ refid: h3_cells_to_multi_polygon_geometry
CREATE OR REPLACE FUNCTION
    h3_cells_to_multi_polygon_geometry(h3index[]) RETURNS geometry
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    wkb bytea;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %I.h3_cells_to_multi_polygon_wkb($1)',
        self_schema
    )
    INTO wkb
    USING $1;

    RETURN wkb::@extschema:postgis@.geometry;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

--@ availability: 4.1.0
--@ refid: h3_cells_to_multi_polygon_geography
CREATE OR REPLACE FUNCTION
    h3_cells_to_multi_polygon_geography(h3index[]) RETURNS geography
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    wkb bytea;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT %I.h3_cells_to_multi_polygon_wkb($1)',
        self_schema
    )
    INTO wkb
    USING $1;

    RETURN wkb::@extschema:postgis@.geography;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

--@ availability: 4.1.0
--@ refid: h3_cells_to_multi_polygon_geometry_agg
CREATE AGGREGATE h3_cells_to_multi_polygon_geometry(h3index) (
    sfunc = pg_catalog.array_append,
    stype = h3index[],
    finalfunc = h3_cells_to_multi_polygon_geometry,
    parallel = safe
);

--@ availability: 4.1.0
--@ refid: h3_cells_to_multi_polygon_geography_agg
CREATE AGGREGATE h3_cells_to_multi_polygon_geography(h3index) (
    sfunc = pg_catalog.array_append,
    stype = h3index[],
    finalfunc = h3_cells_to_multi_polygon_geography,
    parallel = safe
);

--@ availability: 4.2.0
--@ refid: h3_polygon_to_cells_geometry_experimental
CREATE OR REPLACE FUNCTION h3_polygon_to_cells_experimental(multi geometry, resolution integer, containment_mode text DEFAULT 'center') RETURNS SETOF h3index
    AS $$ SELECT @extschema:h3@.h3_polygon_to_cells_experimental(exterior, holes, resolution, containment_mode) FROM (
        SELECT 
            -- extract exterior ring of each polygon
            @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_ExteriorRing(poly))::polygon exterior,
            -- extract holes of each polygon
            (SELECT pg_catalog.array_agg(hole)
                FROM (
                    SELECT @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_InteriorRingN(
                        poly,
                        pg_catalog.generate_series(1, @extschema:postgis@.ST_NumInteriorRings(poly))
                    ))::polygon AS hole
                ) q_hole
            ) holes
        -- extract single polygons from multipolygon
        FROM (
            SELECT (@extschema:postgis@.ST_Dump(
                -- After ST_TileEnvelope(...) -> 4326, low-zoom world-edge tiles
                -- can turn into long geodesic edges. Segmentize keeps the
                -- intended tile cap for overlapping_bbox polyfill. This stays
                -- on the geometry overload only: doing the same after casting
                -- geography to geometry would rewrite geodesic edges in lon/lat
                -- and change public-API semantics.
                @extschema:postgis@.ST_Segmentize(multi, 90.0)
            )).geom AS poly
        ) q_poly GROUP BY poly
    ) h3_polygon_to_cells; $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT; -- NOT STRICT
COMMENT ON FUNCTION
    h3_polygon_to_cells_experimental(geometry, integer, text)
IS 'Converts polygonal geometry to H3 cells using experimental containment modes.

See "PostGIS Integration" for SRID/validity requirements.';

--@ availability: 4.2.0
--@ refid: h3_polygon_to_cells_geography_experimental
CREATE OR REPLACE FUNCTION h3_polygon_to_cells_experimental(multi geography, resolution integer, containment_mode text DEFAULT 'center') RETURNS SETOF h3index
    AS $$ SELECT @extschema:h3@.h3_polygon_to_cells_experimental(exterior, holes, resolution, containment_mode) FROM (
        SELECT
            @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_ExteriorRing(poly))::polygon exterior,
            (SELECT pg_catalog.array_agg(hole)
                FROM (
                    SELECT @extschema:postgis@.ST_MakePolygon(@extschema:postgis@.ST_InteriorRingN(
                        poly,
                        pg_catalog.generate_series(1, @extschema:postgis@.ST_NumInteriorRings(poly))
                    ))::polygon AS hole
                ) q_hole
            ) holes
        FROM (
            SELECT (@extschema:postgis@.ST_Dump($1::@extschema:postgis@.geometry)).geom AS poly
        ) q_poly GROUP BY poly
    ) h3_polygon_to_cells; $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT; -- NOT STRICT
COMMENT ON FUNCTION
    h3_polygon_to_cells_experimental(geography, integer, text)
IS 'Converts polygonal geography to H3 cells using experimental containment modes.

See "PostGIS Integration" for SRID/validity requirements.';
