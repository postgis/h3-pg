/*
 * Copyright 2024-2025 Zacharias Knudsen
 * Copyright 2026 Eric Schoffstall
 * Copyright 2026 Darafei Praliaskouski
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
#include <port/pg_bitutils.h>

#include <h3api.h>
#include "algos.h"
#include "upstream_macros.h"

/* Low 45 bits holding all 15 encoded H3 index digits. */
#define H3_INDEX_DIGITS_MASK UINT64_C(0x1fffffffffff)

/*
 * Compare only the index digits that participate in the shared-resolution
 * prefix, ignoring deeper child digits from the finer input.
 */
static inline uint64
h3index_prefix_digit_diff(H3Index a, H3Index b, int sharedRes)
{
	int			ignoredBits = (MAX_H3_RES - sharedRes) * H3_PER_DIGIT_OFFSET;

	return ((((uint64) (a ^ b)) & H3_INDEX_DIGITS_MASK) >> ignoredBits);
}

/*
 * Bitwise equivalent of upstream cellToParent for already-validated input.
 * The H3 encoding fills child digits beyond the new resolution with 7.
 */
static inline H3Index
h3index_cell_to_parent_fast(H3Index h, int parentRes)
{
	H3_SET_RESOLUTION(h, parentRes);
	return h | (H3_INDEX_DIGITS_MASK >> (parentRes * H3_PER_DIGIT_OFFSET));
}

H3Index
finest_common_ancestor(H3Index a, H3Index b)
{
	/* guard against invalid indexes */
	if (a == H3_NULL || b == H3_NULL)
		return H3_NULL;

	if (a == b)
		return a;

	/* do not even share the basecell */
	if (getBaseCellNumber(a) != getBaseCellNumber(b))
		return H3_NULL;

	{
		int			aRes = getResolution(a);
		int			bRes = getResolution(b);
		int			coarsestRes = (aRes < bRes) ? aRes : bRes;
		uint64		digitDiff = h3index_prefix_digit_diff(a, b, coarsestRes);

		if (digitDiff == 0)
			return (aRes <= bRes) ? a : b;

		{
			int commonRes = coarsestRes -
				(pg_leftmost_one_pos64(digitDiff) / H3_PER_DIGIT_OFFSET) - 1;

			return h3index_cell_to_parent_fast(a, commonRes);
		}
	}
}

/*
 * Returns +1 if a contains b (or a == b), -1 if b contains a, 0 otherwise.
 * This is derived from the shared ancestor relation so all callers use the
 * same containment semantics.
 */
int
containment(H3Index a, H3Index b)
{
	H3Index		ancestor = finest_common_ancestor(a, b);

	if (ancestor == a)
		return 1;
	if (ancestor == b)
		return -1;
	return 0;
}
