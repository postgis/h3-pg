#ifndef PGH3_WKB_SPLIT_H
#define PGH3_WKB_SPLIT_H

#include <h3api.h>
#include <stdbool.h>

bool
			is_linked_polygon_crossed_by_180(const LinkedGeoPolygon * multiPolygon);

LinkedGeoPolygon *
			split_linked_polygon_by_180(const LinkedGeoPolygon * multiPolygon);

double
			split_180_lat(const LatLng * coord1, const LatLng * coord2);

#endif
