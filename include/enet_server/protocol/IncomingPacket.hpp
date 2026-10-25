#pragma once

#include <cstdint>
#include <memory>

#include <enet/enet.h>
#include <enet_server/network/Connection.hpp>
#include <enet_server/network/PacketBuffer.hpp>

namespace enet_server
{

struct IncomingPacket
{
	IncomingPacket() = default;

	IncomingPacket(std::shared_ptr<Connection> ConnPtr, PacketBuffer&& BufferIn, enet_uint8 Ch = 0)
		: Conn(std::move(ConnPtr)), Buffer(std::move(BufferIn)), Channel(Ch) { }

	std::shared_ptr<Connection> Conn;
	PacketBuffer Buffer;
	enet_uint8 Channel = 0;
};

template<typename TServer, typename TClientState>
struct PacketContext
{
	TServer& Self;
	std::shared_ptr<TClientState> Client;
	IncomingPacket& Packet;
};
}
