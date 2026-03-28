#include <postgres.h>
#include <h3api.h>

void
h3_assert(int error)
{
	if (error)
		ereport(ERROR, (
						errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg(
							"H3 error %i: %s",
							error,
							describeH3Error(error)
						),
						errhint("https://h3geo.org/docs/library/errors#table-of-error-codes")));
}
