# enet-server

A C++20 server foundation built on ENet, with per-connection packet scheduling, configurable limits, and a working client/server example.

## Features

- One I/O thread owns ENet hosts and peers
- Separate realtime, logic, and background worker pools
- Serialized packet handlers per connection
- Async barriers that pause logic packets while allowing realtime work
- Packet dispatch tables and optional coalescing of replaceable updates
- Weighted polling across multiple server instances
- Configurable rate limits, packet sizes, and queue limits
- Shared packet payloads for broadcasting without copying the payload per recipient
- Optional LZ4 packet compression

## Build

Requires a C++20 compiler. ENet and optional LZ4 sources are included; no package manager or network download is needed to build.

**Visual Studio 2022 — no CMake required**

1. Install the **Desktop development with C++** workload, including MSVC v143 and a Windows SDK.
2. Open `enet-server.sln`.
3. Select **Release | x64** and choose **Build > Build Solution**.
4. Set `echo_server` as the startup project and press **Ctrl+F5**.

Executables are written to `build\vs2022\x64\Release\`. `Debug` is also available. Choose `Release-LZ4` or `Debug-LZ4` to include the optional compression module; each configuration has its own output directory.

Run all tests from a PowerShell terminal in the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\Run-Tests.ps1 -Configuration Release
```

**CMake / Linux or Windows**

Requires CMake 3.24+. On Linux:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Alternatively, generate a Visual Studio build with CMake:

```powershell
cmake -S . -B build-cmake -G "Visual Studio 17 2022" -A x64
cmake --build build-cmake --config Release
ctest --test-dir build-cmake -C Release --output-on-failure
```

## Testing

Start the server, then run the client in another terminal:

```powershell
.\build\vs2022\x64\Release\echo_server.exe
.\build\vs2022\x64\Release\echo_client.exe 127.0.0.1 7777 "hello"
```

On Linux, use `./build/echo_server` and `./build/echo_client`. For the CMake Windows build above, use `build-cmake\Release\`.

The expected client output is `reply: hello`. The server creates `server.conf` in its working directory on first run. An explicit configuration path can be passed as the first server argument. Stop the server with Ctrl+C.

The client also supports `async` and `ping`:

```powershell
.\build\vs2022\x64\Release\echo_client.exe 127.0.0.1 7777 "hello" async
.\build\vs2022\x64\Release\echo_client.exe 127.0.0.1 7777 "" ping
```

The client uses the ENet API directly. The server example shows packet registration, application-owned `ClientState`, and asynchronous work with balanced completion.

## Configuration

Defaults bind to `127.0.0.1:7777`. Set `bind_address=0.0.0.0` to listen on all IPv4 interfaces and allow the configured UDP port through your firewall when connecting from another machine.

`server.conf.example` lists all settings. Configuration is loaded at startup. Invalid numeric values and duplicate keys are rejected; omitted keys use defaults.

## Use in your own project

The networking core can be integrated into your own application. The echo server example provides a complete starting point.

Add this directory to a CMake project and link the target:

```cmake
set(ENET_SERVER_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(external/enet-server)
target_link_libraries(my_server PRIVATE enet_server::enet_server)
```

For a native Visual Studio application, add references to the `enet_server` and `enet` projects, add `include` and `third_party/enet/include` to its include paths, use C++20, and link `ws2_32.lib` and `winmm.lib`. Keep its runtime library and configuration consistent with the supplied projects; `echo_server.vcxproj` is a complete example.

Create a `ServerInfo` with an application-defined `Id`, callbacks, and limits. Initialize `ServerManager`, call `CreateServer`, and keep callback owners alive until `Shutdown` returns. The echo example is a complete starting point.

The framework uses a small application packet header on top of ENet. A client must send that header and the payload expected by your handlers. See [the protocol and threading notes](docs/architecture.md).

## Optional compression

Select a Visual Studio `-LZ4` configuration, or configure CMake with `-DENET_SERVER_WITH_LZ4=ON` to build `PacketCompression`. Compression is explicit per packet; enabling the module does not change the example protocol. Both endpoints must agree on which payloads use the compression envelope.

ENet provides reliable UDP delivery. This project does not implement encrypted transport, account authentication, or persistence.

## Dependencies and license

| Component | Version | License |
| --- | --- | --- |
| ENet | 1.3.18 with documented local changes | MIT |
| LZ4 library, optional | 1.10.0 | BSD-2-Clause |

The framework uses the MIT license. See [LICENSE](LICENSE), [third-party notices](THIRD_PARTY_NOTICES.md), and [ENet changes](third_party/enet/CHANGES.md).
