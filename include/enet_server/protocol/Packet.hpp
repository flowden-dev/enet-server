#pragma once

#include <cstddef>
#include <cstdint>

namespace enet_server
{
using TPacketId = uint16_t;
using TPacketMagic = uint16_t;
using TServerId = uint16_t;

struct PacketHeader
{
	TPacketMagic Magic;
	TPacketId Id;
};
static_assert(sizeof(PacketHeader) == 4);

namespace Packet
{
	inline constexpr TPacketMagic Magic = 0x4E45;
	inline constexpr size_t HeaderSize = sizeof(PacketHeader);
	inline constexpr size_t MinSize = HeaderSize;
	inline constexpr size_t MaxSize = 65535;
}
}
