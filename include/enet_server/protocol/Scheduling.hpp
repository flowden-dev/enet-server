#pragma once

namespace enet_server
{

enum class WorkClass
{
	Realtime,					// packets that must be handled immedietely; non-blocking
	Logic,						// normal application logic
	AsyncLogic				// executed in logic class, arms async barrier, submits background work to background worker
};

enum class PacketPriority
{
	Critical,					// use for realtime packets, don't use this for 'WorkClass::AsyncLogic' because this will stall the queue!
	Normal,
	Low
};

enum class ExecPriority
{
	High,
	Normal,
	Low
};

enum class HandleResult
{
	Ok,							// packet was handled successfully
	Async,						// success; don't handle any further packets until async work (background tasks) is done
	Disconnect					// error; disconnect the client
};

struct PacketTraits
{
	WorkClass Work = WorkClass::Logic;
	PacketPriority Priority = PacketPriority::Normal;
	bool Coalescable = false;	// old packet can be replaced by newer one with the same packet ID
};

inline constexpr ExecPriority ToExecPriority(PacketPriority Priority) noexcept
{
	switch (Priority)
	{

	case PacketPriority::Critical:	return ExecPriority::High;
	case PacketPriority::Low:		return ExecPriority::Low;
	case PacketPriority::Normal:
	default:						return ExecPriority::Normal;

	}
}

}
