## Development

In order to build and test your changes, simply run `./scripts/develop`.

During development, repository source files keep `INSTALL_VERSION` and the
latest update SQL filenames at `unreleased`. CMake derives the installed
PostgreSQL extension version from that placeholder as `${PROJECT_VERSION}dev`,
so a development build of project version `4.5.0` installs control, SQL, and
module metadata for `4.5.0dev`. Release scripts still replace the source
placeholder with the exact release version.

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

1. Prepare the release branch
   - Pick `X.Y.Z`. The `X.Y` part must match the bundled H3 core version; bump
     only the h3-pg patch when H3 core has not changed.
   - Start from a clean tracked worktree on the branch you want to release from.
   - Run `scripts/release X.Y.Z`. Set `RELEASE_DATE=YYYY-MM-DD` only when the
     changelog and citation date should differ from today.
   - The script creates `release-X.Y.Z` and leaves the release changes
     uncommitted for review.
2. Review the release diff
   - Root `CMakeLists.txt` has `VERSION X.Y.Z`, sets `INSTALL_VERSION` to
     `${PROJECT_VERSION}`, and therefore installs exactly `X.Y.Z` instead of
     the generated development version.
   - The `h3` and `h3_postgis` update files that ended in `--unreleased.sql`
     have been renamed to end in `--X.Y.Z.sql`, and their CMake references were
     renamed with them.
   - Installer SQL availability comments, generated API documentation,
     `CITATION.cff`, `CHANGELOG.md`, and the extension upgrade regression target
     all refer to `X.Y.Z`.
   - `scripts/check-metadata` and `git diff --check` passed at the end of the
     script run.
3. Commit and merge the release branch
   - Commit the reviewed release changes.
   - Push and merge `release-X.Y.Z`.
4. Publish the GitHub release
   - This is a maintainer action, not part of `scripts/release`. The release
     manager, or another maintainer with GitHub release permissions, publishes
     the release after `release-X.Y.Z` is merged.
   - Create or draft release `vX.Y.Z` from the merged release commit on `main`.
     If GitHub creates the tag, verify that tag `vX.Y.Z` points at that commit.
   - Copy the `CHANGELOG.md` entry into the release description, including the
     link reference definitions used by that entry, and preview the rendered
     Markdown before publishing.
5. Distribute the extension on PGXN
   - Run `scripts/bundle` to package the release
   - Upload the distribution on [PGXN Manager](https://manager.pgxn.org/)
6. Prepare the next development cycle
   - After the release branch is merged, start from the updated main branch.
   - Run `scripts/postrelease`. The script restores `INSTALL_VERSION` to
     `unreleased`, creates the next empty `h3--X.Y.Z--unreleased.sql` and
     `h3_postgis--X.Y.Z--unreleased.sql` files, adds them to the extension CMake
     files, keeps the upgrade regression target pointed at the default
     extension version, and runs the release metadata checks.
   - Review, commit, push, and merge the post-release development branch.
