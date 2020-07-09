#include "app_delegate.hpp"
#include <boost/process/child.hpp>
#include <lib/utility.hpp>
#include <csignal>
#include <iostream>
#include "cli.hpp"
#include "daemonconfig.hpp"
namespace
{
void my_abort_signal_handler (int signum)
{
	std::signal (signum, SIG_DFL);
	sgns::dump_crash_stacktrace ();
	// sgns::create_load_memory_address_files ();
}
}
namespace sgns
{
    AppDelegate::AppDelegate (){

    }

    AppDelegate::~AppDelegate (){

    }

    void AppDelegate::init(int argc, char * const * argv){
        std::cout << "--------------AppDelegate::init()---------------" << std::endl;
        sgns::set_umask ();
        boost::program_options::options_description description ("Command line options");
        // clang-format off
        description.add_options ()
            ("help", "Print out options")
            ("version", "Prints out version")
            ("config", boost::program_options::value<std::vector<std::string>>()->multitoken(), "Pass node configuration values. This takes precedence over any values in the configuration file. This option can be repeated multiple times.")
            ("daemon", "Start node daemon")
            ("debug_block_count", "Display the number of block");
        // clang-format on
        // sgns::add_node_options (description);
        // sgns::add_node_flag_options (description);
    }

    void AppDelegate::run(boost::filesystem::path const & data_path/*, sgns::node_flags const & flags*/){
        std::cout << "--------------AppDelegate::run()---------------" << std::endl;
        	// Override segmentation fault and aborting.
	    std::signal (SIGSEGV, &my_abort_signal_handler);
	    std::signal (SIGABRT, &my_abort_signal_handler);
        
        boost::filesystem::create_directories (data_path);
	    boost::system::error_code error_chmod;
        // sgns::set_secure_perm_directory (data_path, error_chmod);

        // sgns::daemon_config config (data_path);
    }

    void AppDelegate::exit(){
        std::cout << "--------------AppDelegate::exit()---------------" << std::endl;
    }

} // namespace sgns
