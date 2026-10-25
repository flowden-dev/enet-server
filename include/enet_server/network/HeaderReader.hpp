#pragma once

#include <cstring>

#include <enet_server/protocol/Packet.hpp>
#include <enet_server/protocol/IncomingPacket.hpp>

namespace enet_server
{

inline bool ReadPacketHeader(IncomingPacket& Pkt, TPacketId& IdOut)
{
	if (Pkt.Buffer.size_remaining() < sizeof(PacketHeader)) [[unlikely]]
		return false;

	TPacketMagic magic = Pkt.Buffer.read<TPacketMagic>();
	if (magic != Packet::Magic) [[unlikely]] return false;

	IdOut = Pkt.Buffer.read<TPacketId>();
	return true;
}

inline bool PeekPacketHeader(const uint8_t* Buffer, size_t Length, TPacketId& IdOut)
{
	if (!Buffer || Length < sizeof(PacketHeader)) [[unlikely]]  return false;

	PacketHeader Header { };
	std::memcpy(&Header, Buffer, sizeof(Header));

	if (Header.Magic != Packet::Magic) [[unlikely]] return false;

	IdOut = Header.Id;
	return true;
}
}
