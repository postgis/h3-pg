# PostgreSQL bindings for H3

This library provides PostgreSQL bindings for the [H3 Core Library](https://github.com/uber/h3). For API reference, please see the [H3 Documentation](https://uber.github.io/h3).

Developed in collaboration with [Scandinavian Highlands](http://www.scandinavian-highlands.com).

## Prerequisites

- PostgreSQL 9.6 or higher (_including server headers_). It might work with earlier versions, we have not tested earlier than 9.6.
- C compiler (e.g., gcc)
- GNU Make
- Git & CMake (for libh3)

## Quick Overview

If the prerequisites are met you can use the PGXN Client to download, build, and install, e.g.:

```
$ pgxn install h3
$ pgxn load h3
$ psql
=# SELECT h3_geo_to_h3(POINT('37.3615593,-122.0553238'), 5);
  h3_geo_to_h3
-----------------
 85e35e73fffffff
(1 row)
```

#### On macOS with the Postgres.app Universal Binary

```
brew install pgxnclient make cmake
export CMAKE_OSX_ARCHITECTURES="arm64;x86_64"
pgxn install h3
```


## Usage

Generally, all functions have been renamed from camelCase in H3 to snake*case in SQL with an added `h3*`prefix (except when that would result in a double`h3\_`prefix). For example`geoToH3`becomes`h3_geo_to_h3`and`h3ToChildren`becomes`h3_cell_to_children`.

See [API reference](docs/api.md) for all provided functions.

## For developers

See [Development](docs/development.md).

## License

This project is released under the [Apache 2.0 License](LICENSE.md).
