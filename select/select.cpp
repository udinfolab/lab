/*
 * Select the document from stream corpus. The stream_id is specified in the
 * document list file.
 *
 * Thanks to:
 * https://groups.google.com/d/msg/streamcorpus/fi8Y8yseF8o/viJjiFNVLNsJ
 *
 * Streaming corpus is passed in through stdin, and selected documents are 
 * passed out through stdout in intact thrift format.
 */

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

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
//using namespace streamcorpus;
namespace po = boost::program_options;

// global constant variables
const char DOC_LIST_FILE[] = "query/doc-list.txt";
//const char DOC_LIST_FILE[] = "query/simple/doc-list.txt";
const bool VERBOSE = true;

// document list
std::map<std::string, int> g_doc_list_map;

// function declaration
int load_doc_list();

int main(int argc, char **argv) {
  // load the query
  ::load_doc_list();

  bool negate(false);
  // Supported options.
  po::options_description desc("Allowed options");
  desc.add_options()("help,h", "help message");

  // Parse command line options
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::clog << desc << "\n";
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
  boost::shared_ptr<TBinaryProtocol> protocolOutput(
      new TBinaryProtocol(transportOutput));
  transportOutput->open();

  // Read and process all stream items
  streamcorpus::StreamItem stream_item;
  int cnt = 0;
  int matched = 0;

  while (true) {
    try {
      // Read stream_item from stdin
      stream_item.read(protocolInput.get());
      ++cnt;

      if(g_doc_list_map.end() != g_doc_list_map.find(stream_item.stream_id)){
        if(VERBOSE){
          std::clog << "Matched: [" << stream_item.stream_id << "] " << std::endl;
        }
        // save the document into the output stream
        stream_item.write(protocolOutput.get());
        ++matched;
      }
    } catch (TTransportException e) {
      // Vital to flush the buffered output or you will lose the last one
      transportOutput->flush();
      std::clog << "Total stream items processed: " << cnt << std::endl;
      std::clog << "Total stream items matched: " << matched << std::endl;
      break;
    }
  }
  return 0;
}

int load_doc_list(){
  std::ifstream doc_list_file(DOC_LIST_FILE);
  if(false == doc_list_file.is_open()){
    std::cerr << "Failed to load doc_list file: " << DOC_LIST_FILE << std::endl;
    exit(-1);
  }

  std::string url;
  while(std::getline(doc_list_file, url)){
    g_doc_list_map[url] = 1;
  }
 
  if(VERBOSE){
    int num = g_doc_list_map.size();
    std::clog << "# of documents: " << num << std::endl;
  }
  return 0;
}

