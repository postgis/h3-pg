/*
 * Copyright 2023 Bytes & Brains
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

	if (a == b)
		return a;

	/* do not even share the basecell */
	if (getBaseCellNumber(a) != getBaseCellNumber(b))
		return H3_NULL;

	aRes = getResolution(a);
	bRes = getResolution(b);
	coarsestRes = (aRes < bRes) ? aRes : bRes;

	/* iterate backwards through resolutions */
	for (int i = coarsestRes; i > 0; i--)
	{
		h3_assert(cellToParent(a, i, &aParent));
		h3_assert(cellToParent(b, i, &bParent));
		if (aParent == bParent)
			return aParent;
	}

	return H3_NULL;
}
