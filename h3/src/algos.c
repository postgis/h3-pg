/*
 * Copyright 2024-2025 Zacharias Knudsen, Eric Schoffstall
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

#include <h3api.h>
#include "algos.h"
#include "error.h"

H3Index
finest_common_ancestor(H3Index a, H3Index b)
{
	int			aRes,
				bRes,
				coarsestRes;
	H3Index		aParent,
				bParent;

	/* guard against invalid indexes */
	if (a == H3_NULL || b == H3_NULL)
		return H3_NULL;

	if (a == b)
		return a;

	/* do not even share the basecell */
	if (getBaseCellNumber(a) != getBaseCellNumber(b))
		return H3_NULL;

	aRes = getResolution(a);
	bRes = getResolution(b);
	coarsestRes = (aRes < bRes) ? aRes : bRes;

	/*
	 * Binary search for the finest resolution where parents match.
	 * H3 parent containment is monotonic: if parents match at resolution R,
	 * they also match at all resolutions < R. This gives us O(log(maxRes))
	 * cellToParent calls instead of O(maxRes).
	 *
	 * This is also a good candidate for upstream h3 — a bitwise approach
	 * using the 3-bit digit layout could do this in O(1) without any
	 * cellToParent calls at all.
	 */
	{
		int			lo = 0,
					hi = coarsestRes;
		H3Index		result = H3_NULL;

		while (lo <= hi)
		{
			int			mid = (lo + hi) / 2;

			h3_assert(cellToParent(a, mid, &aParent));
			h3_assert(cellToParent(b, mid, &bParent));
			if (aParent == bParent)
			{
				result = aParent;
				lo = mid + 1;	/* try finer */
			}
			else
			{
				hi = mid - 1;	/* try coarser */
			}
		}

		return result;
	}
}
