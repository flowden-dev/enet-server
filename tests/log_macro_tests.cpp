// reproduce Windows SDK macros even on non-Windows builds
#define ERROR 0
#include <enet_server/logging/Log.hpp>
int main()
{
	ENET_SERVER_LOG(INFO) << "info macro test";
	ENET_SERVER_LOG(WARNING) << "warning macro test";
	ENET_SERVER_LOG(ERROR) << "error macro test";
	ENET_SERVER_LOG_EVERY("macro/test", ERROR, std::chrono::seconds(1)) << "throttled macro test";
	static_assert(ERROR == 0, "the logger must not undefine Windows macros");
}
