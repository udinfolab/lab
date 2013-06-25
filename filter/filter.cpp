#include <inttypes.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <iostream>
#include <cstdio>
#include <time.h>
#include <boost/unordered_map.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <fstream>

#include "streamcorpus_types.h"
#include "streamcorpus_constants.h"

#include <protocol/TBinaryProtocol.h>
#include <protocol/TDenseProtocol.h>
#include <protocol/TJSONProtocol.h>
#include <transport/TTransportUtils.h>
#include <transport/TFDTransport.h>
#include <transport/TFileTransport.h>

#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>

using namespace std;
using namespace boost;
using namespace boost::filesystem;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace po = boost::program_options;

using namespace streamcorpus;

int main(int argc, char **argv) {

    clog << "Starting program" <<endl;
    bool negate(false);

    // Supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "help message")
    ;

    // Parse command line options
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
        return 1;
    }

    // Setup thrift reading and writing from stdin and stdout
    int input_fd = 0;
    int output_fd = 1;

    // input
    shared_ptr<TFDTransport> innerTransportInput(new TFDTransport(input_fd));
    shared_ptr<TBufferedTransport> transportInput(new TBufferedTransport(innerTransportInput));
    shared_ptr<TBinaryProtocol> protocolInput(new TBinaryProtocol(transportInput));
    transportInput->open();

    // output 
    shared_ptr<TFDTransport> transportOutput(new TFDTransport(output_fd));
    shared_ptr<TBinaryProtocol> protocolOutput(new TBinaryProtocol(transportOutput));
    transportOutput->open();

    // Read and process all stream items
    StreamItem stream_item;
    int cnt=0;

    while (true) {
        try {
            // Read stream_item from stdin
            stream_item.read(protocolInput.get());
	        cout << stream_item.stream_id << " clean_visible size: " << stream_item.body.clean_visible.size() << endl;
	        cnt++;
        }
        catch (TTransportException e) {
            // Vital to flush the buffered output or you will lose the last one
            transportOutput->flush();
            clog << "Total stream items processed: " << cnt << endl;
            break;
        }
    }
    return 0;
}

