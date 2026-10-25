#include <enet_server/util/ByteReader.hpp>
#include <enet_server/util/ByteWriter.hpp>
#include <iostream>
#include <stdexcept>

using namespace enet_server;

namespace
{
	void Check(bool Value, const char* Message) { if (!Value) throw std::runtime_error(Message); }
	ByteWriter MakeWriter()
	{
		ByteWriter writer;
		writer.write(std::string(128, 'x'));
		writer.buffer().shrink_to_fit();
		return writer;
	}
}

int main()
{
	try
	{
		auto scalar = MakeWriter();
		scalar.write_scalar(scalar);
		ByteReader reader(scalar.buffer(), 128);
		std::string payload;
		Check(reader.read_scalar(payload) && payload == std::string(128, 'x'), "self scalar append failed");
		auto overlap = MakeWriter();
		overlap.reset(128, 0);
		overlap.write_scalar(overlap.data(), 128);
		ByteReader overlapReader(overlap.buffer());
		Check(overlapReader.read_scalar(payload) && payload == std::string(128, 'x'), "scalar prefix overwrote source bytes");
		auto bytes = MakeWriter();
		bytes.write(bytes.data(), bytes.size());
		Check(bytes.size() == 256 && std::string(reinterpret_cast<char*>(bytes.data()), bytes.size()) == std::string(256, 'x'), "self byte append failed");
		auto fixed = MakeWriter();
		fixed.write_fixed(fixed.data() + 8, 120, 128);
		Check(fixed.size() == 256 && fixed[128] == 'x' && fixed[247] == 'x' && fixed[248] == 0, "self fixed append failed");
		auto assigned = MakeWriter();
		assigned.assign(assigned.data() + 8, 120);
		Check(assigned.size() == 120 && assigned[119] == 'x', "overlapping writer assignment failed");
		ByteReader input(bytes.buffer());
		input.buffer().shrink_to_fit();
		Check(input.write_at(input.data(), input.size(), 1), "overlapping reader growth failed");
		input.assign(input.data() + 1, 256);
		Check(input.read(payload, 256) && payload == std::string(256, 'x'), "overlapping reader assignment failed");
		std::cout << "buffer alias tests passed\n";
		return 0;
	}
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
