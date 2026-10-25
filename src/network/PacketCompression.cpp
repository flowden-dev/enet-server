#define LZ4_STATIC_LINKING_ONLY

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

#include <enet_server/protocol/Packet.hpp>
#include <lz4.h>

#include <enet_server/network/PacketCompression.hpp>

namespace enet_server
{

namespace
{
	constexpr size_t PayloadHeaderSize = sizeof(PacketCompression::PayloadHeader);

	struct DecodedPayloadHeader
	{
		uint8_t CodecId = 0;
		uint32_t UncompressedSize = 0;
	};

	struct LZ4ThreadContext
	{
		LZ4_stream_t Stream;
		bool Initialized = false;

		bool EnsureInitialized() noexcept
		{
			if (Initialized)
				return true;

			if (!LZ4_initStream(&Stream, sizeof(Stream)))
				return false;

			Initialized = true;
			return true;
		}
	};
	thread_local LZ4ThreadContext g_ThreadContext;

	struct CompressScratch
	{
		std::unique_ptr<uint8_t[]> Data;
		size_t Capacity = 0;

		// returns a writable buffer of at least Needed bytes
		// contents are uninitialized reused per thread, so after warmup there is neither an allocation nor a value-init
		uint8_t* Ensure(size_t Needed)
		{
			if (Needed > Capacity)
			{
				Data = std::make_unique_for_overwrite<uint8_t[]>(Needed);
				Capacity = Needed;
			}
			return Data.get();
		}
	};
	thread_local CompressScratch g_CompressScratch;

	int CompressLZ4Block(const uint8_t* Src, int SrcSize, char* Dst, int DstCapacity, int Acceleration) noexcept
	{
		if (!Src || !Dst || SrcSize <= 0 || DstCapacity <= 0)
			return 0;

		if (!g_ThreadContext.EnsureInitialized())
			return 0;

		if (Acceleration <= 0)
			Acceleration = 1;

		return LZ4_compress_fast_extState_fastReset(&g_ThreadContext.Stream, reinterpret_cast<const char*>(Src), Dst, SrcSize, DstCapacity, Acceleration);
	}

	bool ReadPayloadHeader(const uint8_t* Src, size_t Size, DecodedPayloadHeader& Out) noexcept
	{
		if (!Src || Size < PayloadHeaderSize)
			return false;

		Out.CodecId = Src[0];
		std::memcpy(&Out.UncompressedSize, Src + sizeof(uint8_t), sizeof(uint32_t));

		return true;
	}

	template<typename TReader>
	bool DecompressImpl(const TReader& Reader, ByteReader& Out)
	{
		Out = ByteReader();

		size_t remaining = Reader.size_remaining();
		if (remaining < PayloadHeaderSize) return false;

		const uint8_t* base = Reader.pos();

		// use DecodedPayloadHeader instead of PacketCompression::PayloadHeader
		// so the hot-path read uses aligned local storage
		DecodedPayloadHeader hdr;
		if (!ReadPayloadHeader(base, remaining, hdr)) return false;

		const uint8_t* src = base + PayloadHeaderSize;
		size_t srcSize = remaining - PayloadHeaderSize;
		size_t outSize = static_cast<size_t>(hdr.UncompressedSize);

		// decompressed payload itself must still fit protocol bounds
		if (outSize > (Packet::MaxSize - Packet::HeaderSize)) return false;

		switch (static_cast<PacketCompression::Codec>(hdr.CodecId))
		{

		case PacketCompression::Codec::None:
		{
			if (srcSize != outSize) return false;

			Out.assign(src, srcSize);
			return true;
		}

		case PacketCompression::Codec::LZ4:
		{
			if (srcSize > static_cast<size_t>((std::numeric_limits<int>::max)()) || outSize > static_cast<size_t>((std::numeric_limits<int>::max)()))
				return false;

			if (!outSize)
				return srcSize == 0;

			Out.resize(outSize);

			int decoded = LZ4_decompress_safe(reinterpret_cast<const char*>(src), reinterpret_cast<char*>(Out.data()), static_cast<int>(srcSize), static_cast<int>(outSize));
			if (decoded < 0 || static_cast<size_t>(decoded) != outSize) { Out.resize(0); return false; }

			return true;
		}

		default:
			return false;

		}
	}
}

bool PacketCompression::Compress(ByteWriter& Stream, const Options& Opts)
{
	size_t streamSize = Stream.size();
	if (streamSize < Packet::HeaderSize || streamSize > Packet::MaxSize) return false;

	size_t payloadSize = streamSize - Packet::HeaderSize;

	if (payloadSize > static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) return false;
	if (payloadSize > static_cast<size_t>((std::numeric_limits<uint32_t>::max)())) return false;

	const uint8_t* packetData = Stream.begin<const uint8_t>();
	if (!packetData) return false;

	const uint8_t* payloadData = packetData + Packet::HeaderSize;
	size_t rawWrappedPacketSize = Packet::HeaderSize + sizeof(PayloadHeader) + payloadSize;

	bool mustCompressToFit = rawWrappedPacketSize > Packet::MaxSize;
	bool shouldTryLz4 = payloadSize && (mustCompressToFit || payloadSize >= Opts.MinPayloadBytes);

	int compressedSize = 0;
	bool useLz4 = false;
	uint8_t* scratch = nullptr;

	if (shouldTryLz4)
	{
		// LZ4 worst-case output bound for the payload area
		int maxCompressed = LZ4_compressBound(static_cast<int>(payloadSize));
		if (maxCompressed <= 0) return false;

		// compress into a non-zeroed scratch, the large worst-case bound is never value-initialized
		scratch = g_CompressScratch.Ensure(static_cast<size_t>(maxCompressed));
		if (!scratch) return false;

		compressedSize = CompressLZ4Block(payloadData, static_cast<int>(payloadSize), reinterpret_cast<char*>(scratch), maxCompressed, Opts.Acceleration);
		if (compressedSize > 0)
		{
			size_t compressedPayloadSize = static_cast<size_t>(compressedSize);
			size_t finalPacketSize = Packet::HeaderSize + sizeof(PayloadHeader) + compressedPayloadSize;

			bool fitsCompressed = finalPacketSize <= Packet::MaxSize;
			bool savesEnough = compressedPayloadSize < payloadSize && (payloadSize - compressedPayloadSize) >= Opts.MinSavingsBytes;

			if (fitsCompressed && (mustCompressToFit || savesEnough))
				useLz4 = true;
		}
	}

	if (useLz4)
	{
		size_t compressedPayloadSize = static_cast<size_t>(compressedSize);
		size_t finalSize = Packet::HeaderSize + sizeof(PayloadHeader) + compressedPayloadSize;

		// sized to the actual output, only finalSize bytes are touched (and all overwritten below)
		ByteWriter out(finalSize);
		out.write(packetData, Packet::HeaderSize);
		out.write<uint8_t>(static_cast<uint8_t>(Codec::LZ4));
		out.write<uint32_t>(static_cast<uint32_t>(payloadSize));
		out.write(scratch, compressedPayloadSize);

		Stream = std::move(out);
		return true;
	}

	if (!Opts.AllowPassthrough || rawWrappedPacketSize > Packet::MaxSize)
		return false;

	ByteWriter out(rawWrappedPacketSize);
	out.write(packetData, Packet::HeaderSize);
	out.write<uint8_t>(static_cast<uint8_t>(Codec::None));
	out.write<uint32_t>(static_cast<uint32_t>(payloadSize));
	if (payloadSize) out.write(payloadData, payloadSize);

	Stream = std::move(out);
	return true;
}

bool PacketCompression::Decompress(const PacketBuffer& Reader, ByteReader& StreamOut) { return DecompressImpl(Reader, StreamOut); }
bool PacketCompression::Decompress(const ByteReader& Reader, ByteReader& StreamOut) { return DecompressImpl(Reader, StreamOut); }
}
