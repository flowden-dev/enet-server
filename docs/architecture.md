# Architecture and extension points

## Execution

`ServerManager` owns an I/O thread, realtime and logic executors, and a background executor. A numeric `TServerId` identifies each server instance. Hosts are polled with configurable weights and event budgets.

The I/O thread receives an ENet packet, checks its size and header, applies the connection rate limit, classifies its work, and queues it on the connection. Outbound host/peer operations are also executed by that I/O thread. Packet allocation and destruction use ENet's allocator and may occur on workers. Custom packet free callbacks must not throw; their original callback and `userData` are preserved by send tracking. Send functions consume a fresh packet with exclusive caller ownership, including on rejection. Create a separate ENet packet for each send; `SharedPacket::make()` shares the payload safely.

| Callback or API | Execution contract |
| --- | --- |
| `OnConnect`, `ClassifyPacket` | I/O thread; keep work short and non-blocking |
| `OnReceive` | Realtime or logic worker, selected by packet traits |
| `OnDisconnect` | Logic worker, serialized after active handlers and async completion |
| `PostBackground` | Independent background job; application state needs its own synchronization |
| `PostLogic`, `PostRealtime`, delayed variants | Standalone jobs; they do not acquire a connection's execution gate |
| `Initialize`, `CreateServer`, `Shutdown` | Call from a control thread, never from callbacks or workers |

Only one synchronous packet handler or disconnect callback runs for a connection at a time. Different connections can run concurrently. A background operation may overlap realtime packets on its connection; the application must protect state shared by those paths.

## Async barriers

Classify an asynchronous request as `WorkClass::AsyncLogic`. The scheduler arms its barrier before calling the handler. A handler that successfully submits work returns `HandleResult::Async`. That work must call `CompleteAsync(ServerId, Connection)` exactly once, including failure paths; use a scope guard as shown in the example.

While the barrier is set, realtime packets may run and logic packets wait. A handler that returns `Ok` or `Disconnect` has its pre-armed barrier cleared by the scheduler. A failed background enqueue must not return `Async`.

Application background work must eventually finish or cancel. Shutdown drains accepted worker jobs, so a job that blocks indefinitely prevents shutdown from finishing. Stop external producers before shutdown and destroy application state after it returns. Shutdown detaches remaining peers; application-wide cleanup belongs to the application owner.

Delayed jobs remain in the bounded timer queue if their worker queue is full and are retried after one millisecond. Their due time is an earliest execution time, not a deadline guarantee. Shutdown cancels jobs still waiting in the timer queue.

Disconnect callbacks are retried if the logic queue is full. A detached connection retains its admission slot until its callback finishes, including time spent waiting for an async barrier. This bounds cleanup memory by `MaxConnections`; overload can temporarily reject new connections until cleanup catches up.

## Coalescing and limits

Coalescing replaces an older queued packet with the same ID and ENet channel on the same connection. Enable it only for state snapshots where losing intermediate updates is valid. It is disabled for async requests.

Each connection has critical and normal queues. Realtime packets use the critical queue. With both queues available and no barrier, a critical streak yields to normal work after eight packets. Server-wide and per-connection packet counts bound queued input; a token bucket bounds the accepted packet rate.

ENet's maximum packet size and waiting-data limit are set when a host is created. Worker queues and the server I/O command queue have limits too. `MaxConnectionSendQueueSize` (configuration key `max_connection_send_queue`, default 128) bounds outgoing packets per connection across both the framework queue and ENet, including reliable packets waiting for acknowledgement. Capacity returns when ENet releases the packet; a slow peer cannot bypass the limit by moving packets into ENet. Tracking takes constant time and uses one small allocation per accepted packet, while shared payloads remain shared. A final `SendPacketAndDc` packet uses the same limit; rejection leaves the connection open. A failed `SendPacket` consumes and destroys the supplied packet; callers must not destroy it again. A successful send means it was accepted for sending, not acknowledged by the remote application.

`DisconnectLater` drains sends already accepted by the framework before asking ENet to disconnect. `SendPacketAndDc` appends a final packet and then uses the same drain behavior. Both reject subsequent sends. Immediate `Disconnect` discards queued sends. Reliable delivery still depends on the peer remaining reachable.

## Example wire format

All application fields in the example are little endian. ENet supplies packet boundaries. The buffer helpers require a little-endian host.

Byte reader/writer assignments and writes support source ranges inside their own buffers, including overlapping ranges and writes that grow the allocation. Sources must still describe valid byte ranges; unchecked indexing and raw destination pointers remain the caller's responsibility.

| Offset | Field | Width |
| --- | --- | --- |
| 0 | Magic `0x4E45`, bytes `45 4E` | 2 bytes |
| 2 | Packet ID | 2 bytes |
| 4 | Payload | Remaining bytes |

The library's application packet limit is 65,535 bytes, including the header. The example further limits request payloads to 1,024 bytes and accepts these IDs:

| Request | Response | Payload | Work class |
| --- | --- | --- | --- |
| 1, echo | 2 | Echoed bytes | Logic |
| 3, async echo | 4 | Echoed bytes after simulated background work | AsyncLogic |
| 5, ping | 6 | Empty request, `pong` response | Realtime |

This is an example application protocol, not part of ENet's transport protocol. Change the application header and dispatch table together with your client if you define another format.

## LZ4 module

`PacketCompression::Compress` preserves the application header and wraps the payload as a one-byte codec plus a four-byte uncompressed length, followed by the encoded payload. Codec 0 is passthrough; codec 1 is LZ4. `Decompress` expects the reader positioned after the application header. It validates the declared decoded length before allocating and checks the exact decompressed byte count.

The module does not negotiate codecs or automatically compress arbitrary incoming packets. A receiving handler must know that a particular payload is wrapped before decoding it. Raw echo and ping packets remain unchanged whether the module is built or not.
