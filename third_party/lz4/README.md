# LZ4 source dependency

This directory contains the unmodified LZ4 library files from upstream release
[`v1.10.0`](https://github.com/lz4/lz4/releases/tag/v1.10.0), commit
`ebb370ca83af193212df4dcbadcc5d87bc0de2f0`:

- `lib/lz4.c` -> `lz4.c`
- `lib/lz4.h` -> `lz4.h`
- `lib/LICENSE` -> `LICENSE`

Both native Visual Studio and CMake builds use these same files. They are compiled
only in an LZ4-enabled configuration. Keeping the source here allows builds from
a checkout or ZIP without a package manager or network download.

## Updating

1. Create a dependency-update branch and choose a tagged upstream release.
2. Read its release notes. Replace `lz4.c` and `lz4.h` together with the matching
   files from `lib/`, and retain the current upstream library license.
3. Update the version and commit recorded here and in `THIRD_PARTY_NOTICES.md`,
   and the version in the root README's dependency table.
4. Build and test both with and without LZ4 on Windows and Linux. For native
   Visual Studio, check `Debug`, `Release`, `Debug-LZ4`, and `Release-LZ4`.
5. Review and commit the dependency update separately from application changes.

The packet codec uses `LZ4_compress_fast_extState_fastReset`, a static-only API.
Review this call when upgrading and compile the matching source and header
together. Switching to a shared-library package requires checking which APIs it
exports.

The build uses the checked-in version until a reviewed update changes these
files. It does not automatically select the newest upstream release.
