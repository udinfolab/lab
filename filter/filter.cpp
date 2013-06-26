#include <inttypes.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <cstdio>
#include <time.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

#include <boost/unordered_map.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>

#include <protocol/TBinaryProtocol.h>
#include <protocol/TDenseProtocol.h>
#include <protocol/TJSONProtocol.h>
#include <transport/TTransportUtils.h>
#include <transport/TFDTransport.h>
#include <transport/TFileTransport.h>

#include "streamcorpus_types.h"
#include "streamcorpus_constants.h"

//using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace po = boost::program_options;

//using namespace streamcorpus;

// global constant variables
const char QUERY_FILE[] = "query/query.txt";
const int QUERY_NUM = 170;

// function declaration
int load_query(std::vector<std::string> & query_vec);
std::string url2ent(std::string & url);

int main(int argc, char **argv) {
  std::vector<std::string> query_vec;
  query_vec.reserve(QUERY_NUM);
  ::load_query(query_vec);

  std::clog << "Starting program" << std:: endl;
  bool negate(false);

  // Supported options.
  po::options_description desc("Allowed options");
  desc.add_options()("help,h", "help message");

  // Parse command line options
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 1;
  }

  // Setup thrift reading and writing from stdin and stdout
  int input_fd = 0;
  int output_fd = 1;

  // initialize input stream
  boost::shared_ptr<TFDTransport> innerTransportInput(new TFDTransport(input_fd));
  boost::shared_ptr<TBufferedTransport> transportInput(
        new TBufferedTransport(innerTransportInput));
  boost::shared_ptr<TBinaryProtocol> protocolInput(
      new TBinaryProtocol(transportInput));
  transportInput->open();

  // initialize output stream
  boost::shared_ptr<TFDTransport> transportOutput(new TFDTransport(output_fd));
  boost::shared_ptr<TBinaryProtocol> protocolOutput(new TBinaryProtocol(transportOutput));
  transportOutput->open();

  // Read and process all stream items
  streamcorpus::StreamItem stream_item;
  int cnt=0;

  while (true) {
    try {
      // Read stream_item from stdin
      stream_item.read(protocolInput.get());
      std::cout << stream_item.stream_id << " clean_visible size: ";
      std::cout << stream_item.body.clean_visible.size() << std::endl;
      ++cnt;
    }
    catch (TTransportException e) {
      // Vital to flush the buffered output or you will lose the last one
      transportOutput->flush();
      std::clog << "Total stream items processed: " << cnt << std::endl;
      break;
    }
  }
  return 0;
}

int load_query(std::vector<std::string> & query_vec){
  std::ifstream query_file(QUERY_FILE);
  if(false == query_file.is_open()){
    std::cerr << "Failed to load query file: " << QUERY_FILE << std::endl;
    exit(-1);
  }

  std::string line;
  int index = 0;
  while(std::getline(query_file, line)){
    std::istringstream iss(line);
    std::string url;
    if (!(iss >> url)){
      std::cerr << "Error when parsing query file: " << QUERY_FILE << std::endl;
      break;
    }
    std::string query = ::url2ent(url);
    std::cout << "Query #" << ++index << " " << query << std::endl;
    query_vec.push_back(query);
  }
  return 0;
}

/*
 * Parse URL and extract the entity name
 */
std::string url2ent(std::string & url){
  // split the string
  // http://stackoverflow.com/a/236976

  std::vector<std::string> strs;
  boost::split(strs, url, boost::is_any_of("/"));
  std::string ent = strs.back();
  return ent;
}

