/*
 * Copyright 2022-2024 Zacharias Knudsen
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
#include <h3api.h>

#include <fmgr.h>			// PG_FUNCTION_INFO_V1
#include <utils/builtins.h> // cstring_to_text
#include <utils/lsyscache.h> // get_typbyval

#include "type.h"
#include "config.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_get_extension_version);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3_pg_migrate_pass_by_reference);

/* Return version number for this extension (not main h3 lib) */
Datum
h3_get_extension_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(POSTGRESQL_H3_VERSION));
}

/*
 * Migration from pass-by-reference to pass-by-value
 * https://github.com/postgis/h3-pg/issues/31
 *
 * Dereferences an h3index datum that is still pass-by-reference. The guard
 * reads the catalog rather than USE_FLOAT8_BYVAL: where the type is already
 * pass-by-value the datum is the cell itself, and dereferencing it segfaults.
 */
Datum
h3_pg_migrate_pass_by_reference(PG_FUNCTION_ARGS)
{
	Oid			argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	H3Index		cell;

	if (!OidIsValid(argtype))
		elog(ERROR, "could not determine argument type");

	if (get_typbyval(argtype))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("h3index is already pass-by-value in this database")));

	cell = *((H3Index *) DatumGetPointer(PG_GETARG_DATUM(0)));

	PG_RETURN_H3INDEX(cell);
}
