#pragma once

#include <cstddef>
#include <cstdint>
#include <enet/enet.h>

namespace enet_server::Network
{
	using Channel = enet_uint8;
	inline constexpr Channel Main = 0;
	inline constexpr Channel Realtime = 1;
	inline constexpr size_t DefaultChannelCount = 2;
	inline constexpr size_t ServerIoWeightMax = 50;

	enum DisconnectReason
	{
		ClientDc,
		PacketSizeInvalid,
		PacketHeaderInvalid,
		PacketHandlerError,
		PacketAsyncGuard,
		UnhandledException,
		RateLimit,
		ServerFullCapacity
	};

	inline const char* DisconnectReasonToString(DisconnectReason Reason) noexcept
	{
		switch (Reason)
		{
		case ClientDc: return "client disconnected";
		case PacketSizeInvalid: return "invalid packet size";
		case PacketHeaderInvalid: return "invalid packet header";
		case PacketHandlerError: return "packet handler rejected request";
		case PacketAsyncGuard: return "invalid async result";
		case UnhandledException: return "unhandled exception";
		case RateLimit: return "rate limit exceeded";
		case ServerFullCapacity: return "server capacity exceeded";
		default: return "unknown";
		}
	}
}
