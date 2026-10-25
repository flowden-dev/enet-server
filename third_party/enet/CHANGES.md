# Local changes relative to ENet 1.3.18

This directory vendors ENet's source with a small set of local changes. Its transport headers, command layouts, and version number remain based on ENet 1.3.18. Applications using only ENet's public API do not need custom client API calls.

- Cap outgoing send passes at four per call to prevent a continuously extending send loop from monopolizing service time
- Update `totalWaitingData` when discarded incoming commands release packets, with saturating subtraction
- Check unreliable fragments for zero length and reject impossible fragment counts relative to total length
- Initialize incoming fragmented payloads so overlapping fragment offsets cannot expose unwritten heap bytes to application handlers
- Use unsigned bit masks in sequence and fragment tracking
- Guard null data when growing a packet buffer
- Adjust interrupted or oversized socket-receive error handling on Unix and Windows
- Move C linkage declarations around callable interfaces and add them to the standalone callback/list headers
- Guard MSVC-specific warning pragmas and secure-CRT definitions
- Restore the upstream-style `include/enet` layout and supply a target-scoped CMake build

The upstream range coder implementation is included so the declared ENet compression API remains available. The application-level optional LZ4 module is separate from this ENet compressor.
