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

static_assert(std::endian::native == std::endian::little, "ByteWriter assumes little-endian length fields");
class ByteWriter : private ScalarBuffer
{

private:
	std::vector<uint8_t> m_Buffer;
	size_t m_BufferPos = 0;

public:
	ByteWriter() = default;

	ByteWriter(const ByteWriter& Other)
		: m_Buffer(Other.m_Buffer), m_BufferPos(Other.m_BufferPos)
	{
		if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();
	}

	explicit ByteWriter(size_t Size, size_t StartPos = 0, bool IsZeroMem = false) { reset(Size, StartPos, IsZeroMem); }

	ByteWriter(uint8_t* Buffer, size_t Size) { assign(Buffer, Size); }
	ByteWriter(const uint8_t* Buffer, size_t Size) { assign(Buffer, Size); }

	ByteWriter(uint8_t* Buffer, size_t Size, size_t StartPos)
	{
		assign(Buffer, Size);

		size_t sz = size_buffer();
		m_BufferPos = (StartPos > sz) ? sz : StartPos;
	}
	ByteWriter(const uint8_t* Buffer, size_t Size, size_t StartPos)
	{
		assign(Buffer, Size);

		size_t sz = size_buffer();
		m_BufferPos = (StartPos > sz) ? sz : StartPos;
	}

	ByteWriter(const std::vector<uint8_t>& Buffer) { assign(Buffer.data(), Buffer.size()); }

	ByteWriter(const std::vector<uint8_t>& Buffer, size_t StartPos)
	{
		assign(Buffer.data(), Buffer.size());

		size_t sz = size_buffer();
		m_BufferPos = (StartPos > sz) ? sz : StartPos;
	}

	~ByteWriter() = default;

	ByteWriter(ByteWriter&& Other) noexcept
		: m_Buffer(std::move(Other.m_Buffer)), m_BufferPos(Other.m_BufferPos)
	{
		if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();
		Other.m_BufferPos = 0;
	}

	ByteWriter& operator=(const ByteWriter& Other)
	{
		if (this != &Other)
		{
			m_Buffer = Other.m_Buffer;
			m_BufferPos = Other.m_BufferPos;

			if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();
		}

		return *this;
	}
	ByteWriter& operator=(ByteWriter&& Other) noexcept
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

public:
	void reset(size_t Size = 0, size_t StartPos = 0, bool IsZeroMem = false)
	{
		if (IsZeroMem) m_Buffer.assign(Size, 0);
		else m_Buffer.resize(Size);

		m_BufferPos = (StartPos > Size) ? Size : StartPos;
	}

	template<typename T>
	void reset(const T* Source, size_t Size, size_t StartPos = static_cast<size_t>(-1))
	{
		assign(Source, Size);

		size_t sz = size_buffer();
		m_BufferPos = StartPos == static_cast<size_t>(-1) ? sz : ((StartPos > sz) ? sz : StartPos);
	}

	void reset(const std::vector<uint8_t>& Buffer, size_t StartPos = static_cast<size_t>(-1))
	{
		m_Buffer = Buffer;

		size_t sz = size_buffer();
		m_BufferPos = StartPos == static_cast<size_t>(-1) ? sz : ((StartPos > sz) ? sz : StartPos);
	}

	std::vector<uint8_t> release(bool IsShrinkToUsed = true)
	{
		if (IsShrinkToUsed && m_Buffer.size() > m_BufferPos) m_Buffer.resize(m_BufferPos);

		m_BufferPos = 0;
		return std::move(m_Buffer);
	}

public:
	template<typename T>
	ByteWriter& operator<<(T Value) { write(Value); return *this; }

	uint8_t& operator[](size_t Idx) { return m_Buffer[Idx]; }
	const uint8_t& operator[](size_t Idx) const { return m_Buffer[Idx]; }

	bool operator==(const ByteWriter& Other) const
	{
		if (size() != Other.size()) return false;
		if (!size()) return true;

		return !std::memcmp(data(), Other.data(), size());
	}
	bool operator!=(const ByteWriter& Other) const { return !(*this == Other); }

	bool empty() const noexcept { return !size(); }

	void resize(size_t Size)
	{
		m_Buffer.resize(Size);
		m_BufferPos = Size;
	}

	bool reserve(size_t Add)
	{
		if (Add > static_cast<size_t>(-1) - m_Buffer.size()) return false;

		m_Buffer.resize(m_Buffer.size() + Add);
		if (m_BufferPos > m_Buffer.size()) m_BufferPos = m_Buffer.size();

		return true;
	}

	template<typename T>
	void assign(T* Source, size_t Size) { assign(const_cast<const T*>(Source), Size); }

	template<typename T>
	void assign(const T* Source, size_t Size)
	{
		static_assert(sizeof(T) == 1, "assign expects byte-sized T and size in bytes");
		if (Size && !Source) return;

		size_t offset = source_offset(Source);
		if (offset != static_cast<size_t>(-1))
		{
			if (Size > m_Buffer.size() - offset) return;
			// copy before shrinking so the aliased source remains alive
			if (Size) std::memmove(m_Buffer.data(), m_Buffer.data() + offset, Size);
			m_Buffer.resize(Size);
		}
		else
		{
			m_Buffer.resize(Size);
			if (Size) std::memcpy(m_Buffer.data(), Source, Size);
		}

		m_BufferPos = Size;
	}

	template<typename T>
	bool set(size_t Pos, T Value)
	{
		static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
		if (Pos > size_buffer()) return false;
		if (sizeof(T) > size_buffer() - Pos) return false;

		std::memcpy(m_Buffer.data() + Pos, &Value, sizeof(T));
		return true;
	}

	template<typename T>
	size_t write(T Value)
	{
		static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
		if (!ensure_append_capacity(sizeof(T))) return m_BufferPos;

		std::memcpy(m_Buffer.data() + m_BufferPos, &Value, sizeof(T));
		m_BufferPos += sizeof(T);

		return m_BufferPos;
	}

	template<typename T>
	size_t write(T* Source, size_t Size) { return write(const_cast<const T*>(Source), Size); }

	template<typename T>
	size_t write(const T* Source, size_t Size)
	{
		static_assert(sizeof(T) == 1, "write expects byte-sized T and size in bytes");

		if (Size && !Source) return m_BufferPos;
		size_t offset = source_offset(Source);
		if (offset != static_cast<size_t>(-1) && Size > m_Buffer.size() - offset) return m_BufferPos;
		if (!ensure_append_capacity(Size)) return m_BufferPos;

		if (Size)
		{
			if (offset != static_cast<size_t>(-1)) std::memmove(m_Buffer.data() + m_BufferPos, m_Buffer.data() + offset, Size);
			else std::memcpy(m_Buffer.data() + m_BufferPos, Source, Size);
			m_BufferPos += Size;
		}

		return m_BufferPos;
	}

	template<typename T>
	size_t write(const std::vector<T>& Vec)
	{
		if (Vec.size() > static_cast<size_t>(-1) / sizeof(T)) return m_BufferPos;
		return write<uint8_t>(reinterpret_cast<const uint8_t*>(Vec.data()), Vec.size() * sizeof(T));
	}

	size_t write(const std::string& String) { return write<char>(String.data(), String.length()); }

	template<typename T>
	size_t write_fixed(T* Source, size_t SourceSize, size_t FixedSize, uint8_t FillValue = 0x00) { return write_fixed(const_cast<const T*>(Source), SourceSize, FixedSize, FillValue); }

	size_t write_fixed(const std::string& String, size_t FixedSize, uint8_t FillValue = 0x00) { return write_fixed<char>(String.data(), String.size(), FixedSize, FillValue); }

	template<typename T>
	size_t write_fixed(const T* Source, size_t SourceSize, size_t FixedSize, uint8_t FillValue = 0x00)
	{
		static_assert(sizeof(T) == 1, "write_fixed expects byte-sized T and size in bytes");

		if (SourceSize > FixedSize || (SourceSize && !Source)) return m_BufferPos;
		size_t offset = source_offset(Source);
		if (offset != static_cast<size_t>(-1) && SourceSize > m_Buffer.size() - offset) return m_BufferPos;
		if (!ensure_append_capacity(FixedSize)) return m_BufferPos;

		uint8_t* dst = m_Buffer.data() + m_BufferPos;

		if (SourceSize)
		{
			if (offset != static_cast<size_t>(-1)) std::memmove(dst, m_Buffer.data() + offset, SourceSize);
			else std::memcpy(dst, Source, SourceSize);
		}

		if (FixedSize > SourceSize)
			std::memset(dst + SourceSize, FillValue, FixedSize - SourceSize);

		m_BufferPos += FixedSize;
		return m_BufferPos;
	}

	template<typename T>
	size_t write_cstring_fixed(T* Source, size_t SourceSize, size_t FieldSize) { return write_cstring_fixed(const_cast<const T*>(Source), SourceSize, FieldSize); }

	size_t write_cstring_fixed(const std::string& String, size_t FieldSize) { return write_cstring_fixed<char>(String.data(), String.size(), FieldSize); }

	template<typename T>
	size_t write_cstring_fixed(const T* Source, size_t SourceSize, size_t FieldSize)
	{
		static_assert(sizeof(T) == 1, "write_cstring_fixed expects byte-sized T and size in bytes");

		// need at least one byte for the terminating/padding zero
		if (!FieldSize) return m_BufferPos;

		// a fixed C-string field of N bytes can carry at most N-1 payload bytes
		if (SourceSize >= FieldSize) return m_BufferPos;

		return write_fixed(Source, SourceSize, FieldSize, 0x00);
	}

	size_t write_value(size_t Size, uint8_t Value = 0x00)
	{
		if (!ensure_append_capacity(Size)) return m_BufferPos;

		if (Size)
		{
			std::memset(m_Buffer.data() + m_BufferPos, Value, Size);
			m_BufferPos += Size;
		}

		return m_BufferPos;
	}

	template<typename T>
	void write_scalar(T* Source, size_t Size) { ScalarBuffer::write_scalar(Source, Size); }

	template<typename T>
	void write_scalar(const T* Source, size_t Size) { ScalarBuffer::write_scalar(Source, Size); }

	void write_scalar(const std::string& String) { ScalarBuffer::write_scalar(String); }

	template<typename T>
	void write_scalar(const std::vector<T>& Vec)
	{
		if (Vec.size() > static_cast<size_t>(-1) / sizeof(T)) return;
		ScalarBuffer::write_scalar(reinterpret_cast<const uint8_t*>(Vec.data()), Vec.size() * sizeof(T));
	}

	void write_scalar(const ByteWriter& Stream) { ScalarBuffer::write_scalar<uint8_t>(Stream.begin<uint8_t>(), Stream.size()); }

	void write_scalar(const ByteWriter* Stream)
	{
		if (!Stream) { write_scalar_empty(); return; }

		ScalarBuffer::write_scalar<uint8_t>(Stream->begin<uint8_t>(), Stream->size());
	}

	void write_scalar_empty() { ScalarBuffer::write_scalar<uint8_t>(nullptr, 0); }

	std::vector<uint8_t>& buffer() noexcept { return m_Buffer; }
	const std::vector<uint8_t>& buffer() const noexcept { return m_Buffer; }

	uint8_t* data() noexcept { return m_Buffer.empty() ? nullptr : m_Buffer.data(); }
	const uint8_t* data() const noexcept { return m_Buffer.empty() ? nullptr : m_Buffer.data(); }

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

	uint8_t* begin() noexcept { return data(); }
	const uint8_t* begin() const noexcept { return data(); }

	template<typename T>
	T* begin() noexcept { return reinterpret_cast<T*>(data()); }

	template<typename T>
	const T* begin() const noexcept { return reinterpret_cast<const T*>(data()); }

	uint8_t* end() noexcept
	{
		uint8_t* d = data();
		return d ? (d + size_buffer()) : nullptr;
	}
	const uint8_t* end() const noexcept
	{
		const uint8_t* d = data();
		return d ? (d + size_buffer()) : nullptr;
	}

	template<typename T>
	T* end() noexcept
	{
		uint8_t* d = data();
		return d ? reinterpret_cast<T*>(d + size_buffer()) : nullptr;
	}
	template<typename T>
	const T* end() const noexcept
	{
		const uint8_t* d = data();
		return d ? reinterpret_cast<const T*>(d + size_buffer()) : nullptr;
	}

	template<typename T>
	T* pos() noexcept
	{
		uint8_t* d = data();
		return d ? reinterpret_cast<T*>(d + m_BufferPos) : nullptr;
	}
	template<typename T>
	const T* pos() const noexcept
	{
		const uint8_t* d = data();
		return d ? reinterpret_cast<const T*>(d + m_BufferPos) : nullptr;
	}

	size_t size() const noexcept { return m_BufferPos; }
	size_t size_buffer() const noexcept { return m_Buffer.size(); }

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

	bool ensure_append_capacity(size_t Count)
	{
		if (Count > static_cast<size_t>(-1) - m_BufferPos) return false;

		size_t need = m_BufferPos + Count;
		if (need > m_Buffer.size()) m_Buffer.resize(need);

		return true;
	}

private:
	void write_scalar_impl(const uint8_t* ScalarBuffer, size_t ScalarSize, const uint8_t* DataBuffer, size_t DataSize) override
	{
		if (ScalarSize > static_cast<size_t>(-1) - DataSize) return;

		size_t offset = source_offset(DataBuffer);
		if (offset != static_cast<size_t>(-1) && DataSize > m_Buffer.size() - offset) return;
		size_t total = ScalarSize + DataSize;
		if (!ensure_append_capacity(total)) return;

		// copy payload first because the length prefix may overlap an aliased source
		if (DataSize && DataBuffer)
		{
			uint8_t* destination = m_Buffer.data() + m_BufferPos + ScalarSize;
			if (offset != static_cast<size_t>(-1)) std::memmove(destination, m_Buffer.data() + offset, DataSize);
			else std::memcpy(destination, DataBuffer, DataSize);
		}

		if (ScalarSize)
		{
			std::memcpy(m_Buffer.data() + m_BufferPos, ScalarBuffer, ScalarSize);
			m_BufferPos += ScalarSize;
		}

		if (DataSize && DataBuffer)
		{
			m_BufferPos += DataSize;
		}
	}

};
}
