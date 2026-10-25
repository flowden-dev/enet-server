#pragma once

#include <atomic>
#include <memory>
#include <new>
#include <utility>

#include <enet_server/protocol/Packet.hpp>
#include <enet/enet.h>
#include <enet_server/util/ByteWriter.hpp>

namespace enet_server
{

class PacketSender
{

private:
	// will hold the packet data until it is being passed to ENet
	std::unique_ptr<ByteWriter> m_Stream = nullptr;

public:
	class SharedPacket
	{
		friend class PacketSender;

	private:
		struct Storage
		{
			std::atomic<uint32_t> RefCount { 1 };
			ByteWriter Stream;
			enet_uint32 Flags = ENET_PACKET_FLAG_RELIABLE | ENET_PACKET_FLAG_NO_ALLOCATE;
		};

	private:
		Storage* m_Storage = nullptr;

	public:
		SharedPacket() = default;
		~SharedPacket() { release(m_Storage); }

		SharedPacket(const SharedPacket& Other) noexcept : m_Storage(Other.m_Storage) { retain(m_Storage); }
		SharedPacket(SharedPacket&& Other) noexcept : m_Storage(std::exchange(Other.m_Storage, nullptr)) { }

		SharedPacket& operator=(const SharedPacket& Other) noexcept
		{
			if (this != &Other)
			{
				Storage* oldStorage = m_Storage;
				m_Storage = Other.m_Storage;

				retain(m_Storage);
				release(oldStorage);
			}

			return *this;
		}

		explicit operator bool() const noexcept { return m_Storage && m_Storage->Stream.begin<uint8_t>() && m_Storage->Stream.size() >= Packet::HeaderSize; }

		size_t size() const noexcept { return m_Storage ? m_Storage->Stream.size() : 0; }

		ENetPacket* make() const
		{
			if (!m_Storage) return nullptr;

			const uint8_t* data = m_Storage->Stream.begin<uint8_t>();
			size_t size = m_Storage->Stream.size();

			if (!data || size < Packet::HeaderSize) return nullptr;

			retain(m_Storage);

			ENetPacket* packet = enet_packet_create(data, size, m_Storage->Flags);
			if (!packet) { release(m_Storage); return nullptr; }

			packet->freeCallback = &SharedPacket::FreeSharedPacket;
			packet->userData = m_Storage;
			return packet;
		}

	private:
		explicit SharedPacket(Storage* StoragePtr) noexcept : m_Storage(StoragePtr) { }

		static void retain(Storage* StoragePtr) noexcept { if (StoragePtr) StoragePtr->RefCount.fetch_add(1, std::memory_order_relaxed); }

		static void release(Storage* StoragePtr) noexcept
		{
			if (!StoragePtr) return;

			if (StoragePtr->RefCount.fetch_sub(1, std::memory_order_acq_rel) == 1)
				delete StoragePtr;
		}

		static void ENET_CALLBACK FreeSharedPacket(ENetPacket* Packet)
		{
			if (!Packet) return;

			Storage* storage = static_cast<Storage*>(Packet->userData);

			// detach for safety before releasing storage
			Packet->data = nullptr;
			Packet->userData = nullptr;

			release(storage);
		}

	};

public:
	ByteWriter* begin(TPacketId Id, size_t Size)
	{
		if (Size > Packet::MaxSize - Packet::HeaderSize) { m_Stream.reset(); return nullptr; }
		ByteWriter* raw = new (std::nothrow) ByteWriter(Packet::HeaderSize + Size);
		if (!raw) return nullptr;

		m_Stream.reset(raw);

		m_Stream->write<TPacketMagic>(Packet::Magic);
		m_Stream->write<TPacketId>(Id);

		return raw;
	}

	ENetPacket* finalize(enet_uint32 Flag = ENET_PACKET_FLAG_RELIABLE)
	{
		if (!m_Stream) return nullptr;
		if (m_Stream->size() < Packet::HeaderSize || m_Stream->size() > Packet::MaxSize) return nullptr;

		// ensure enet won't reallocate the buffer
		enet_uint32 flag = Flag | ENET_PACKET_FLAG_NO_ALLOCATE;

		ENetPacket* packet = enet_packet_create(m_Stream->begin<uint8_t>(), m_Stream->size(), flag);
		if (!packet) return nullptr;

		// register free callback to ensure ByteWriter instance is deleted
		packet->freeCallback = &PacketSender::FreePacket;
		packet->userData = m_Stream.release(); // transfer ownership

		return packet;
	}

	SharedPacket finalize_shared(enet_uint32 Flag = ENET_PACKET_FLAG_RELIABLE)
	{
		if (!m_Stream) return SharedPacket{ };
		if (m_Stream->size() < Packet::HeaderSize) return SharedPacket{ };
		if (m_Stream->size() > Packet::MaxSize) return SharedPacket{ };

		auto* storage = new (std::nothrow) SharedPacket::Storage();
		if (!storage) return SharedPacket{ };

		storage->Flags = Flag | ENET_PACKET_FLAG_NO_ALLOCATE;
		storage->Stream = std::move(*m_Stream);
		m_Stream.reset();

		if (storage->Stream.size() < Packet::HeaderSize || !storage->Stream.begin<uint8_t>())
		{
			delete storage;
			return SharedPacket{ };
		}

		return SharedPacket(storage);
	}

private:
	static void ENET_CALLBACK FreePacket(ENetPacket* packet)
	{
		if (!packet) return;

		auto* stream = static_cast<ByteWriter*>(packet->userData);

		packet->userData = nullptr;
		packet->data = nullptr;

		delete stream;
	}

};
}
