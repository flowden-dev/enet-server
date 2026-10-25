#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include <enet/enet.h>
#include <enet_server/util/ScalarBuffer.hpp>

namespace enet_server
{

// note:
// ENet packets held by PacketBuffer may be destroyed on worker threads not only on the I/O thread
// this is intentional and poses no problem because it just calls free and does not meddle with internal ENet affairs

static_assert(std::endian::native == std::endian::little, "PacketBuffer assumes little-endian length fields");
class PacketBuffer : public ScalarBuffer
{

private:
	ENetPacket* m_Packet = nullptr;
	size_t m_BufferPos = 0;

public:
	PacketBuffer() = default;

	explicit PacketBuffer(ENetPacket* Packet) noexcept : m_Packet(Packet), m_BufferPos(0) { }

	PacketBuffer(ENetPacket* Packet, size_t Offset) noexcept
		: m_Packet(Packet)
	{
		size_t sz = size();
		m_BufferPos = (Offset > sz) ? sz : Offset;
	}

	~PacketBuffer() { reset(); }

	PacketBuffer(const PacketBuffer&) = delete;
	PacketBuffer& operator=(const PacketBuffer&) = delete;

	PacketBuffer(PacketBuffer&& Other) noexcept
		: m_Packet(Other.m_Packet), m_BufferPos(Other.m_BufferPos)
	{
		Other.m_Packet = nullptr;
		Other.m_BufferPos = 0;
	}

	PacketBuffer& operator=(PacketBuffer&& Other) noexcept
	{
		if (this != &Other)
		{
			reset();

			m_Packet = Other.m_Packet;
			m_BufferPos = Other.m_BufferPos;

			Other.m_Packet = nullptr;
			Other.m_BufferPos = 0;
		}

		return *this;
	}

public:
	void reset(ENetPacket* Packet = nullptr, size_t Offset = 0) noexcept
	{
		// protect against self-reset with the same packet pointer
		if (m_Packet == Packet)
		{
			size_t sz = size();
			m_BufferPos = (Offset > sz) ? sz : Offset;
			return;
		}

		ENetPacket* oldPacket = m_Packet;
		m_Packet = Packet;

		size_t sz = size();
		m_BufferPos = (Offset > sz) ? sz : Offset;

		if (oldPacket) enet_packet_destroy(oldPacket);
	}

	ENetPacket* release() noexcept
	{
		ENetPacket* out = m_Packet;
		m_Packet = nullptr;
		m_BufferPos = 0;
		return out;
	}

	ENetPacket* packet() noexcept { return m_Packet; }
	const ENetPacket* packet() const noexcept { return m_Packet; }

	template<typename T>
	void operator>>(T& Value) { Value = read<T>(); }

	uint8_t& operator[](size_t Idx) { return data()[Idx]; }
	const uint8_t& operator[](size_t Idx) const { return data()[Idx]; }

	bool empty() const noexcept { return size() == 0; }

	void drop(size_t Size) { if (Size <= size_remaining()) m_BufferPos += Size; }

	void revoke(size_t Size) { if (m_BufferPos >= Size) m_BufferPos -= Size; }

	template<typename T>
	T read()
	{
		static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
		if (sizeof(T) > size_remaining()) return { };

		T v { };
		std::memcpy(&v, pos(), sizeof(T));

		m_BufferPos += sizeof(T);
		return v;
	}

	template<typename T>
	bool read(T& Destination)
	{
		static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
		if (sizeof(T) > size_remaining()) return false;

		std::memcpy(&Destination, pos(), sizeof(T));
		m_BufferPos += sizeof(T);

		return true;
	}

	template<typename T>
	bool read(T* Destination, size_t Size)
	{
		static_assert(sizeof(T) == 1, "read expects byte-sized T");
		if (Size > size_remaining()) return false;
		if (Size && !Destination) return false;

		if (Size)
		{
			std::memcpy(Destination, pos(), Size);
			m_BufferPos += Size;
		}

		return true;
	}

	bool read(std::string& Destination, size_t Length)
	{
		if (Length > size_remaining()) return false;

		const char* p = reinterpret_cast<const char*>(pos());
		Destination.assign(p, p + Length);

		m_BufferPos += Length;
		return true;
	}

	template<typename T>
	bool read_scalar(T* Destination)
	{
		auto [size, dropCount] = ScalarBuffer::read_scalar(pos(), size_remaining());
		if (size == -1) return false;

		drop(dropCount);
		if (!size) return true;

		return read<T>(Destination, static_cast<size_t>(size));
	}

	bool read_scalar(std::string& Destination)
	{
		auto [size, dropCount] = ScalarBuffer::read_scalar(pos(), size_remaining());
		if (size == -1) return false;

		drop(dropCount);
		if (!size) { Destination.clear(); return true; }

		return read(Destination, static_cast<size_t>(size));
	}

	uint8_t* data() noexcept { return m_Packet ? m_Packet->data : nullptr; }
	const uint8_t* data() const noexcept { return m_Packet ? m_Packet->data : nullptr; }

	uint8_t* begin() noexcept { return data(); }
	const uint8_t* begin() const noexcept { return data(); }

	uint8_t* end() noexcept
	{
		uint8_t* d = data();
		return d ? (d + size()) : nullptr;
	}
	const uint8_t* end() const noexcept
	{
		const uint8_t* d = data();
		return d ? (d + size()) : nullptr;
	}

	uint8_t* pos() noexcept
	{
		uint8_t* d = data();
		return d ? (d + m_BufferPos) : nullptr;
	}
	const uint8_t* pos() const noexcept
	{
		const uint8_t* d = data();
		return d ? (d + m_BufferPos) : nullptr;
	}

	size_t size() const noexcept { return m_Packet ? static_cast<size_t>(m_Packet->dataLength) : 0; }

	size_t size_remaining() const noexcept
	{
		const size_t sz = size();
		return (m_BufferPos <= sz) ? (sz - m_BufferPos) : 0;
	}

private:
	// read-only class; no write implementation required
	void write_scalar_impl(const uint8_t*, size_t, const uint8_t*, size_t) override { }

};
}
