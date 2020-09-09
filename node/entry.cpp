#include "app_delegate.hpp"
#include <boost/program_options.hpp>
#include <lib/utility.hpp>
#include "secure/utility.hpp"
#include <sstream>
#include <iostream> 

#include <hawktracer.h>
sgns::AppDelegate g_app_delegate;
int main (int argc, char * const * argv)
{
	std::cout << "--------------main()---------------" << std::endl;
    int result (0);
	/* initialize HawkTracer library */
    ht_init(argc, (char **)argv);
	HT_ErrorCode error_code;
	/* Create a listener and register it to a timeline, it'll handle all the HawkTracer events */
	ht_file_dump_listener_register(ht_global_timeline_get(), "sgns-node.htdump", 2048, &error_code);
	
	/* Creating listener might fail (e.g. file can't be open),
	* so we have to check the status
	*/
	if (error_code != HT_ERR_OK)
	{
		printf("Unable to create listener. Error code: %d\n", error_code);
		ht_deinit();
		return -1;
	}
	/* We start measuring the code */
    ht_feature_callstack_start_string(ht_global_timeline_get(), "init()");

    g_app_delegate.init(argc, argv);

	ht_feature_callstack_stop(ht_global_timeline_get());
    // boost::program_options::variables_map vm;
    // auto data_path_it = vm.find ("data_path");
	// if (data_path_it == vm.end ())
	// {
	// 	std::string error_string;
	// 	if (!sgns::migrate_working_path (error_string))
	// 	{
	// 		std::cerr << error_string << std::endl;

	// 		return 1;
	// 	}
	// }
    // boost::filesystem::path data_path ((data_path_it != vm.end ()) ? data_path_it->second.as<std::string> () : sgns::working_path ());
    g_app_delegate.run(/*data_path*/);
    g_app_delegate.exit();

	/* Uninitialize HawkTracer library */
    ht_deinit();
	
    return result;
}