## Development

In order to build and test your changes, simply run `./scripts/develop`.

For local upgrade-validation coverage, install `pg_validate_extupgrade` so
`ctest` can run the same extension-upgrade checks as CI. Without it, CTest
registers explicit `*_validate_extupgrade_unavailable` placeholder tests and
those placeholders fail when your worktree touches upgrade-sensitive SQL or
CMake files:

```bash
cargo install --git https://github.com/rjuju/pg_validate_extupgrade pg_validate_extupgrade
```

When `pg_validate_extupgrade` is available, the test suite runs it against a
temporary build-tree PostgreSQL cluster instead of the developer's default
local server.

Documentation is generated from the SQL files using `scripts/documentation` (requires poetry).
This command also validates that all extension GUCs are documented in `h3/src/guc.c`.

## Release Process

1. Prepare a release branch
   - Don't follow semver blindly: use major and minor from H3 core and increment
     the h3-pg patch independently.
   - Run `scripts/release X.Y.Z`. Set `RELEASE_DATE=YYYY-MM-DD` only when the
     release date should differ from today.
   - The script creates `release-X.Y.Z`, updates version metadata, renames
     `--unreleased` update SQL files, updates availability/docs/changelog,
     updates release-sensitive regression targets, and runs release metadata
     checks.
   - Review, commit, push, and merge the `release-X.Y.Z` branch.
2. Create a release on GitHub
   - Draft new release "vX.Y.Z"
   - Copy CHANGELOG.md entry into release description
3. Distribute the extension on PGXN
   - Run `scripts/bundle` to package the release
   - Upload the distribution on [PGXN Manager](https://manager.pgxn.org/)
4. Prepare for development
   - Run `scripts/postrelease` to restore `INSTALL_VERSION=unreleased`, create
     the next empty update SQL files, and add them to the extension CMake files.
   - Review, commit, push, and merge the post-release development branch.
