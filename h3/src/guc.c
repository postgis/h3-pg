/*
 * Copyright 2023 Zacharias Knudsen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <postgres.h>

#include <utils/guc.h> // DefineCustom*Variable

bool		h3_guc_strict = false;
bool		h3_guc_extend_antimeridian = false;

void
_guc_init(void)
{
	/*
	 * @guc-doc h3.strict
	 * Recommended: true for most PostGIS/SQL analytics sessions.
	 *
	 * true: reject longitude outside [-180, 180] and latitude outside [-90, 90].
	 * Use this to catch wrong coordinate-system inputs early (for example
	 * projected coordinates passed as lon/lat).
	 *
	 * false: keep upstream H3 default behavior (including wrapped coordinates).
	 * Use only when wrapped-around data is intentional.
	 *
	 * Example:
	 *   SET h3.strict TO true;
	 *   SELECT h3_latlng_to_cell(POINT(6196902.235, 1413172.083), 10);
	 */
	DefineCustomBoolVariable("h3.strict",
						 "Enable strict indexing (fail on invalid lng/lat).",
							 "Controls coordinate validation for h3_latlng_to_cell.",
							 &h3_guc_strict,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * @guc-doc h3.extend_antimeridian
	 * Recommended: false for planar PostGIS geometry operations.
	 *
	 * false: use split-across-antimeridian behavior, usually preferred for
	 * planar operations like overlays/intersections.
	 *
	 * true: keep upstream H3 antimeridian continuity behavior as-is.
	 * Use for H3-first workflows that expect continuity semantics.
	 *
	 * Example:
	 *   SET h3.extend_antimeridian TO false;
	 *   SELECT h3_cell_to_boundary('8003fffffffffff'::h3index);
	 */
	DefineCustomBoolVariable("h3.extend_antimeridian",
					   "Extend boundaries by 180th meridian, when possible.",
							 "Controls antimeridian handling for h3_cell_to_boundary.",
							 &h3_guc_extend_antimeridian,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}
