#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>

#include <enet_server/logging/Log.hpp>

#include <enet_server/config/ConfigFile.hpp>

namespace enet_server
{

std::shared_ptr<ReturnEx> ConfigFile::read()
{
	std::string fname(filename());
	if (fname.empty()) return std::make_shared<ReturnEx>(1, "[config] filename is empty");

	std::ifstream file(fname);
	if (!file)
	{
		std::error_code error;
		bool exists = std::filesystem::exists(fname, error);
		if (error || exists) return std::make_shared<ReturnEx>(2, "[config] could not read file " + fname);
		auto res = create();
		if (res) return res;

		file.open(fname);
		if (!file) return std::make_shared<ReturnEx>(2, "[config] could not open file " + fname);
	}

	m_index.clear();

	auto result = std::make_shared<ReturnEx>();
	std::string line;
	unsigned int line_no = 1;

	while (std::getline(file, line))
	{
		parse_line(m_index, line, line_no, result);
		if (result->is_error()) return result;
		++line_no;
	}
	if (file.bad() || (!file.eof() && file.fail()))
		return std::make_shared<ReturnEx>(3, "[config] read failed for file " + fname);

	// back-fill missing defaults
	for (const auto& [key, def] : default_config())
	{
		if (key.empty() || key[0] == '#') continue;
		if (m_index.find(key) == m_index.end())
		{
			m_index[key] = def;
			ENET_SERVER_LOG(WARNING) << "[config] missing parameter '" << key << "'; using default in " << fname;
		}
	}

	return nullptr;
}

std::shared_ptr<ReturnEx> ConfigFile::create()
{
	std::string fname(filename());

	std::ofstream ofile(fname, std::ios::trunc);
	if (!ofile) return std::make_shared<ReturnEx>(1, "[config] could not create file: " + fname);

	ofile << header() << std::endl;

	// write defaults in order
	for (auto& [key, def] : default_config())
	{
		// blank line
		if (key.empty())
			ofile << std::endl;
		// comment line
		else if (key[0] == '#')
			ofile << key << (def.empty() ? "\n" : "=" + def + "\n");
		else
			ofile << key << "=" << def << std::endl;
	}

	ofile << footer();
	ofile.close();
	if (!ofile) return std::make_shared<ReturnEx>(4, "[config] could not finish writing file " + fname);
	return nullptr;
}

void ConfigFile::parse_line(std::map<std::string, std::string>& out, const std::string& line, unsigned int line_number, std::shared_ptr<ReturnEx> result_state) const
{
	result_state->clear();

	// skip blank or comment
	size_t pos = line.find_first_not_of(" \t\r");
	if (pos == std::string::npos) return;
	char c = line[pos];
	if (c == '#' || c == '*') return;

	// ensure only one '='
	if (std::count(line.begin() + pos, line.end(), '=') > 1)
	{
		std::stringstream ss;
		ss << "[config] syntax error: multiple '=' on line " << line_number << " in file " << filename();
		result_state->set(10, ss.str());
		return;
	}

	// find '='
	auto eq = line.find('=', pos);
	if (eq == std::string::npos)
	{
		std::stringstream ss;
		ss << "[config] syntax error: missing '=' on line " << line_number << " in file " << filename();
		result_state->set(11, ss.str());
		return;
	}

	// extract key
	std::string key(line.substr(pos, eq - pos));
	key.erase(std::remove_if(key.begin(), key.end(), [](unsigned char ch) { return std::isspace(ch); }), key.end());
	if (key.empty())
	{
		std::stringstream ss;
		ss << "[config] syntax error: empty key on line " << line_number << " in file " << filename();
		result_state->set(12, ss.str());
		return;
	}

	// extract value
	std::string value(line.substr(eq + 1));
	value.erase(std::remove_if(value.begin(), value.end(), [](unsigned char ch) { return std::isspace(ch); }), value.end());

	// insert
	auto inserted = out.emplace(std::move(key), std::move(value));
	if (!inserted.second)
	{
		std::stringstream ss;
		ss << "[config] syntax error: duplicate key '" << inserted.first->first << "' on line " << line_number << " in file " << filename();
		result_state->set(13, ss.str());
	}
}
}
