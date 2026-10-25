# Third-party notices

## ENet

- Upstream: https://github.com/lsalzman/enet
- Baseline: tag `v1.3.18`, commit `2662c0de09e36f2a2030ccc2c528a3e4c9e8138a`
- License: MIT, reproduced in `third_party/enet/LICENSE`
- Local changes: described in `third_party/enet/CHANGES.md`

ENet is compiled from source as part of the build. The framework uses its public API. No precompiled ENet binaries are required.

## LZ4 library

- Upstream: https://github.com/lz4/lz4
- Version: 1.10.0
- Upstream commit: `ebb370ca83af193212df4dcbadcc5d87bc0de2f0`
- Included files: library `lz4.c`, `lz4.h`, and its BSD-2-Clause license
- Built only in an LZ4-enabled configuration

The LZ4 command-line tools, examples, and test programs are not included.
See [LZ4 maintenance notes](third_party/lz4/README.md) for the update procedure.
