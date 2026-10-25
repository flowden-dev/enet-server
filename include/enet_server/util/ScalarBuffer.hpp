#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace enet_server
{

#define SCALAR_8_SIZE       2						// for payloads lengths up to 0xFF(h) 255(d)
#define SCALAR_16_SIZE      3						// for payloads lengths up to 0xFFFF(h) 65535(d)
#define SCALAR_32_SIZE      5						// for payloads lengths up to 0xFFFFFFFF(h) 4294967295(d)
#define SCALAR_64_SIZE      9						// for payloads lengths up to 0xFFFFFFFFFFFFFFFF(h) 18446744073709551615(d)
#define SCALAR_MAX_SIZE     SCALAR_64_SIZE

#define SCALAR_8(n)         (SCALAR_8_SIZE  + (n))
#define SCALAR_16(n)        (SCALAR_16_SIZE + (n))
#define SCALAR_32(n)        (SCALAR_32_SIZE + (n))
#define SCALAR_64(n)        (SCALAR_64_SIZE + (n))

inline size_t ScalarWireSize(size_t Length) noexcept
{
	if (Length <= 0xFFull)			return SCALAR_8_SIZE + Length;
	if (Length <= 0xFFFFull)		return SCALAR_16_SIZE + Length;
	if (Length <= 0xFFFFFFFFull)	return SCALAR_32_SIZE + Length;

	return SCALAR_64_SIZE + Length;
}

static_assert(std::endian::native == std::endian::little, "ScalarBuffer assumes little-endian length fields");
class ScalarBuffer
{

protected:
	virtual ~ScalarBuffer() = default;

	virtual void write_scalar_impl(const uint8_t* ScalarBuffer, size_t ScalarSize, const uint8_t* DataBuffer, size_t DataSize) = 0;

protected:
	template<typename T>
	void write_scalar(T* Source, size_t Size)
	{
		static_assert(sizeof(T) == 1, "write_scalar expects byte-sized T and size in bytes");
		construct_scalar_buffer(const_cast<const T*>(Source), Size);
	}

	template<typename T>
	void write_scalar(const T* Source, size_t Size)
	{
		static_assert(sizeof(T) == 1, "write_scalar expects byte-sized T and size in bytes");
		construct_scalar_buffer(Source, Size);
	}

	void write_scalar(const std::string& String) { construct_scalar_buffer(String.data(), String.length()); }

	template<typename T>
	void write_scalar(const std::vector<T>& Vec)
	{
		static_assert(std::is_trivially_copyable_v<T>, "vector<T> must be trivially copyable");
		if (Vec.size() > static_cast<std::size_t>(-1) / sizeof(T)) return;

		construct_scalar_buffer(reinterpret_cast<const uint8_t*>(Vec.data()), Vec.size() * sizeof(T));
	}

	static std::pair<long long /* scalar size */, size_t /* drop count */> read_scalar(const uint8_t* Buffer, size_t SizeRemaining) noexcept
	{
		if (!Buffer) return { -1, 0 };
		if (SizeRemaining < SCALAR_8_SIZE) return { -1, 0 };

		size_t dropCount = 0;
		uint64_t payloadSize = 0;

		switch (Buffer[0])
		{

		case ST_byte:
		{
			dropCount = SCALAR_8_SIZE;
			payloadSize = Buffer[1];
		} break;

		case ST_word:
		{
			if (SizeRemaining < SCALAR_16_SIZE) return { -1, 0 };

			uint16_t value;
			std::memcpy(&value, Buffer + 1, sizeof(value));

			payloadSize = value;
			dropCount = SCALAR_16_SIZE;
		} break;

		case ST_int:
		{
			if (SizeRemaining < SCALAR_32_SIZE) return { -1, 0 };

			uint32_t value;
			std::memcpy(&value, Buffer + 1, sizeof(value));

			payloadSize = value;
			dropCount = SCALAR_32_SIZE;
		} break;

		case ST_long:
		{
			if (SizeRemaining < SCALAR_64_SIZE) return { -1, 0 };

			uint64_t value;
			std::memcpy(&value, Buffer + 1, sizeof(value));

			payloadSize = value;
			dropCount = SCALAR_64_SIZE;
		} break;

		default:
			return { -1, 0 };
		}

		if (payloadSize > (SizeRemaining - dropCount)) return { -1, 0 };

		return { static_cast<long long>(payloadSize), dropCount };
	}

private:
	enum scalar_type : uint8_t
	{
		ST_byte = sizeof(uint8_t),
		ST_word = sizeof(uint16_t),
		ST_int = sizeof(uint32_t),
		ST_long = sizeof(uint64_t)
	};

private:
	template<typename T>
	void construct_scalar_buffer(const T* Source, size_t Size)
	{
		uint8_t header[SCALAR_MAX_SIZE];
		size_t scalarSize = 0;

		if (Size <= 0xFFull)
		{
			header[0] = ST_byte;
			header[1] = static_cast<uint8_t>(Size);
			scalarSize = SCALAR_8_SIZE;
		}
		else if (Size <= 0xFFFFull)
		{
			header[0] = ST_word;

			uint16_t value = static_cast<uint16_t>(Size);
			std::memcpy(header + 1, &value, sizeof(value));

			scalarSize = SCALAR_16_SIZE;
		}
		else if (Size <= 0xFFFFFFFFull)
		{
			header[0] = ST_int;

			uint32_t value = static_cast<uint32_t>(Size);
			std::memcpy(header + 1, &value, sizeof(value));

			scalarSize = SCALAR_32_SIZE;
		}
		else
		{
			header[0] = ST_long;

			uint64_t value = static_cast<uint64_t>(Size);
			std::memcpy(header + 1, &value, sizeof(value));

			scalarSize = SCALAR_64_SIZE;
		}

		write_scalar_impl(header, scalarSize, reinterpret_cast<const uint8_t*>(Source), Size);
	}

};
}
