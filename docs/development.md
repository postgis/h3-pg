## Development

We provide a Dockerfile for development without installation of H3 and Postgres. The following requires that your system has `docker` installed.

Simply run `./.github/docker/tools.sh -t`.

It will mount the code as a volume, and also mount the test output directory,
so output can be inspected.

Documentation is generated from the sql files, using the script `scripts/documentaion.sh` (requires poetry).

## Release Process

1. Update version number
   - Don't follow semver, simply use major and minor from H3 core and increment patch.
   - Version number should be changed in `h3.control`, `h3_postgis.control` and `META.json`.
   - Update files suffixed `--unreleased` should be renamed.
   - Create new update files with `--unreleased` suffix.
   - Update changelog by moving from `Unreleased` to a new section
2. Create a release on GitHub
   - Draft new release "vX.Y.Z"
   - Copy CHANGELOG.md entry into release description
3. Distribute the extension on PGXN
   - Run `make dist` to package the release
   - Upload the distribution on [PGXN Manager](https://manager.pgxn.org/) (username: `bytesandbrains`)
4. Revert version numbers in `h3.control`, `h3_postgis.control` and `META.json` to `unreleased`
