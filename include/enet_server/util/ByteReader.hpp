#pragma once

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <type_traits>
#include <vector>

#include <enet_server/util/ScalarBuffer.hpp>

namespace enet_server
{

static_assert(std::endian::native == std::endian::little, "ByteReader assumes little-endian length fields");
class ByteReader : public ScalarBuffer
{

private:
	std::vector<uint8_t> m_Buffer;
	size_t m_BufferPos = 0;

public:
	ByteReader() = default;

	ByteReader(const ByteReader& Other)
		: m_Buffer(Other.m_Buffer), m_BufferPos(Other.m_BufferPos)
	{
		if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();
	}

	ByteReader(const ByteReader& Other, size_t Offset)
		: m_Buffer(Other.m_Buffer)
	{
		const size_t sz = size();
		m_BufferPos = (Offset > sz) ? sz : Offset;
	}

	explicit ByteReader(const std::vector<uint8_t>& Buffer)
		: m_Buffer(Buffer), m_BufferPos(0) { }

	ByteReader(const std::vector<uint8_t>& Buffer, size_t Offset)
		: m_Buffer(Buffer)
	{
		const size_t sz = size();
		m_BufferPos = (Offset > sz) ? sz : Offset;
	}

	ByteReader(uint8_t* Buffer, size_t Size) { assign(Buffer, Size); }
	ByteReader(const uint8_t* Buffer, size_t Size) { assign(Buffer, Size); }

	ByteReader(uint8_t* Buffer, size_t Size, size_t Offset)
	{
		assign(Buffer, Size);

		const size_t sz = size();
		m_BufferPos = (Offset > sz) ? sz : Offset;
	}
	ByteReader(const uint8_t* Buffer, size_t Size, size_t Offset)
	{
		assign(Buffer, Size);

		const size_t sz = size();
		m_BufferPos = (Offset > sz) ? sz : Offset;
	}

	~ByteReader() = default;

	ByteReader(ByteReader&& Other) noexcept
		: m_Buffer(std::move(Other.m_Buffer)), m_BufferPos(Other.m_BufferPos)
	{
		if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();
		Other.m_BufferPos = 0;
	}

	ByteReader& operator=(const ByteReader& Other)
	{
		if (this != &Other)
		{
			m_Buffer = Other.m_Buffer;
			m_BufferPos = Other.m_BufferPos;

			if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();
		}

		return *this;
	}
	ByteReader& operator=(ByteReader&& Other) noexcept
	{
		if (this != &Other)
		{
			m_Buffer = std::move(Other.m_Buffer);
			m_BufferPos = Other.m_BufferPos;

			if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();
			Other.m_BufferPos = 0;
		}

		return *this;
	}

	template<typename T>
	void operator>>(T& Value) { Value = read<T>(); }

	uint8_t& operator[](size_t Idx) { return m_Buffer[Idx]; }
	const uint8_t& operator[](size_t Idx) const { return m_Buffer[Idx]; }

	bool operator==(const ByteReader& Other) const { return m_Buffer.size() == Other.m_Buffer.size() && std::equal(m_Buffer.begin(), m_Buffer.end(), Other.m_Buffer.begin()); }
	bool operator!=(const ByteReader& Other) const { return !(*this == Other); }

	bool empty() const noexcept { return !size(); }

	void resize(size_t Size)
	{
		m_Buffer.resize(Size);
		if (m_BufferPos > Size) m_BufferPos = Size;
	}

	void drop(size_t Size) { if (Size <= size_remaining()) m_BufferPos += Size; }

	bool drop_scalar()
	{
		auto [size, dropCount] = ScalarBuffer::read_scalar(pos(), size_remaining());
		if (size == -1) return false;

		m_BufferPos += dropCount + static_cast<size_t>(size);
		return true;
	}

	void revoke(size_t Size) { if (m_BufferPos >= Size) m_BufferPos -= Size; }

	template<typename T>
	void assign(T* Source, size_t Size) { assign(const_cast<const T*>(Source), Size); }

	template<typename T>
	void assign(const T* Source, size_t Size)
	{
		static_assert(sizeof(T) == 1, "assign expects byte-sized T");

		size_t offset = source_offset(Source);
		if (offset != static_cast<size_t>(-1))
		{
			if (Size > m_Buffer.size() - offset) return;
			if (Size) std::memmove(m_Buffer.data(), m_Buffer.data() + offset, Size);
		}
		m_Buffer.resize(Size);
		m_BufferPos = 0;

		if (Size && Source && offset == static_cast<size_t>(-1)) std::memcpy(m_Buffer.data(), Source, Size);
	}

	template<typename T>
	bool write_cpos(const T* Source, size_t Size, bool ShouldShrink = false)
	{
		static_assert(sizeof(T) == 1, "write_cpos expects byte-sized T");
		return write_at(Source, Size, m_BufferPos, ShouldShrink);
	}

	template<typename T>
	bool write_at(const T* Source, size_t Size, size_t Pos, bool ShouldShrink = false)
	{
		static_assert(sizeof(T) == 1, "write_at expects byte-sized T");
		if (Pos > m_Buffer.size()) return false;
		if (Size > static_cast<size_t>(-1) - Pos) return false; // overflow guard
		if (Size && !Source) return false;

		size_t offset = source_offset(Source);
		if (offset != static_cast<size_t>(-1) && Size > m_Buffer.size() - offset) return false;
		size_t required = Pos + Size;
		if (required > m_Buffer.size()) m_Buffer.resize(required);

		if (Size)
		{
			if (offset != static_cast<size_t>(-1)) std::memmove(m_Buffer.data() + Pos, m_Buffer.data() + offset, Size);
			else std::memcpy(m_Buffer.data() + Pos, Source, Size);
		}

		if (ShouldShrink && m_Buffer.size() > required) m_Buffer.resize(required);
		if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();

		return true;
	}

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

	std::vector<uint8_t>& buffer() noexcept { return m_Buffer; }
	const std::vector<uint8_t>& buffer() const noexcept { return m_Buffer; }

	uint8_t* data() noexcept { return m_Buffer.empty() ? nullptr : m_Buffer.data(); }
	const uint8_t* data() const noexcept { return m_Buffer.empty() ? nullptr : m_Buffer.data(); }

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

	size_t size() const noexcept { return m_Buffer.size(); }

	size_t size_remaining() const noexcept
	{
		size_t sz = size();
		return (m_BufferPos <= sz) ? (sz - m_BufferPos) : 0;
	}

private:
	// keep offsets across reallocations when a source refers to this buffer
	size_t source_offset(const void* Source) const noexcept
	{
		if (!Source || m_Buffer.empty()) return static_cast<size_t>(-1);
		auto* source = static_cast<const uint8_t*>(Source);
		std::less<const uint8_t*> before;
		if (before(source, m_Buffer.data()) || !before(source, m_Buffer.data() + m_Buffer.size()))
			return static_cast<size_t>(-1);
		return static_cast<size_t>(source - m_Buffer.data());
	}

	// read-only class; no write implementation required
	void write_scalar_impl(const uint8_t*, size_t, const uint8_t*, size_t) override { }

};
}
