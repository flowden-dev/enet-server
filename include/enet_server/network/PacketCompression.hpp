#pragma once

#include <cstddef>
#include <cstdint>

#include <enet_server/util/ByteReader.hpp>
#include <enet_server/util/ByteWriter.hpp>

#include <enet_server/network/PacketBuffer.hpp>

namespace enet_server
{

namespace PacketCompression
{
	enum class Codec : uint8_t
	{
		None = 0,
		LZ4 = 1
	};

#pragma pack(push, 1)
	struct PayloadHeader
	{
		uint8_t CodecId = static_cast<uint8_t>(Codec::None);
		uint32_t UncompressedSize = 0;
	};
	static_assert(sizeof(PayloadHeader) == 5, "PacketCompression::PayloadHeader size mismatch");
#pragma pack(pop)

	struct Options
	{
		// do not even try LZ4 below this payload size
		// packet will still be wrapped if Compress() is called
		size_t MinPayloadBytes = 128;

		// require at least this many bytes saved in the payload area otherwise fall back to wrapped raw payload
		size_t MinSavingsBytes = 12;

		// LZ4 acceleration
		// 1 = best ratio for fast mode -> higher = faster, less compression
		int Acceleration = 1;

		// when false, compression failure / no useful gain returns false instead of wrapping the payload as raw
		bool AllowPassthrough = true;
	};

	inline constexpr size_t WrappedPayloadHeaderSize() noexcept { return sizeof(PayloadHeader); }

	// compress the packet payload, header stays untouched
	// don't call Compress twice on a packet or it becomes double wrapped!
	//
	// on success the packet header becomes:
	//		[PacketHeader][PayloadHeader][raw-or-lz4-payload]
	bool Compress(ByteWriter& Stream, const Options& Opts = Options { });

	// decompress packet payload
	// requires Reader.pos() to point to the >> BEGINNING OF THE WRAPPED PAYLOAD <<
	// output stream contains ONLY the decompressed/raw payload starting at position = 0
	bool Decompress(const PacketBuffer& Reader, ByteReader& StreamOut);
	bool Decompress(const ByteReader& Reader, ByteReader& StreamOut);
}
}
