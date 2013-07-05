/*
 * Apply thrift streaming filtering with query entities and their alternative
 * names.
 *
 * Thanks to:
 * https://groups.google.com/d/msg/streamcorpus/fi8Y8yseF8o/viJjiFNVLNsJ
 *
 * Streaming corpus is passed in through stdin, and filtered corpus is passed
 * out through stdout in intact thrift format
 */

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
#include <string>

#include <vector>
#include <map>
#include <algorithm>
#include <iterator>

#include <boost/regex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/iter_find.hpp>

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
//using namespace streamcorpus;
namespace po = boost::program_options;

// global constant variables
const char QUERY_FILE[] = "query/query.txt";
//const char QUERY_FILE[] = "query/simple/query.txt";
const char ALT_NAME_FILE[] = "query/alt-name.txt";
//const char ALT_NAME_FILE[] = "query/simple/alt-name.txt";
const int QUERY_NUM = 170;
const char EMPTY_STR[] = "";
const char NA_STR[] = "N/A";

//const bool VERBOSE = true;
const bool VERBOSE = false;
//const bool VERBOSE = false;

// original query list
std::vector<std::string> g_query_vec;
// query entity list
std::map<std::string, std::string> g_qent_map;
// query entity alternative name list
std::map<std::string, std::vector<std::string> > g_alt_name_map;

// function declaration
int load_query();
int load_alt_name();
std::string url2ent(std::string& url);
std::string& trim(std::string& str);
std::string url_encode(const std::string& url);
std::string url_decode(const std::string& in);
bool is_relevant(const std::string& query, const std::string& doc);

int main(int argc, char **argv) {
  // load the query
  g_query_vec.reserve(QUERY_NUM);
  ::load_query();
  ::load_alt_name();

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

      std::string doc = stream_item.body.clean_visible;
      // skip empty documents
      if(0 == doc.size()){
        continue;
      }
      // apply trimming
      ::trim(doc);

      // search over all the query entities
      std::vector<std::string>::iterator first = g_query_vec.begin();
      std::vector<std::string>::iterator last = g_query_vec.end();
      while(first != last){
        if(is_relevant(*first, doc)){
          if(VERBOSE){
            std::clog << "Matched: [" << stream_item.stream_id << "] ";
            std::clog << "(" << *first << ")" << std::endl;
          }
          // save the document into the output stream
          stream_item.write(protocolOutput.get());
          ++matched;
          break;
        }
        ++first;
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

/*
 * Given a query and string, judge whether the document is relevant w.r.t. the
 * query
 */
bool is_relevant(const std::string& query, const std::string& doc){
  // DEBUG ONLY
  //return true;

  if(g_qent_map.end() == g_qent_map.find(query)){
    std::cerr << "No qent found for query: " << query << std::endl;
    return false;
  }
  std::string qent = g_qent_map[query];

  // use std::string::find() to do quick sub-string based search
  if(std::string::npos != doc.find(qent)){
    return true;
  }else{
    // we then try to match its alternative name
    if(g_alt_name_map.end() == g_alt_name_map.find(query)){
      return false;
    }
    std::vector<std::string> alt_name_vec = g_alt_name_map[query];
    //search over all alternative names
    bool matched = false;
    std::vector<std::string>::iterator first = alt_name_vec.begin();
    std::vector<std::string>::iterator last = alt_name_vec.end();
    while(first != last){
      if(std::string::npos != doc.find(*first)){
        matched = true;
        break;
      }
      ++first;
    }
    return matched;
  }
}

int load_query(){
  std::ifstream query_file(QUERY_FILE);
  if(false == query_file.is_open()){
    std::cerr << "Failed to load query file: " << QUERY_FILE << std::endl;
    exit(-1);
  }

  std::string url;
  int index = 0;
  while(std::getline(query_file, url)){
    g_query_vec.push_back(url);

    // extract the query entity
    std::string qent = ::url2ent(url);
    g_qent_map[url] = qent;

    // verbose output
    if(VERBOSE){
      std::clog << "Query #" << ++index << " [" << qent << "]" << std::endl;
    }
  }
  return 0;
}

int load_alt_name(){
  std::ifstream alt_name_file(ALT_NAME_FILE);
  if(false == alt_name_file.is_open()){
    std::cerr << "Failed to load alt-name file: " << ALT_NAME_FILE << std::endl;
    exit(-1);
  }

  std::string line;
  while(std::getline(alt_name_file, line)){
    // skip empty lines
    if(0 == line.compare(EMPTY_STR)){
      continue;
    }

    std::vector<std::string> strs;
    //boost::split(strs, line, boost::is_any_of(" : "));
    // Split by substring
    // Thanks to http://stackoverflow.com/a/5710242/219617
    boost::iter_split(strs, line, boost::first_finder(" : "));
    if(2 != strs.size()){
      std::cerr << "Invalid alternative name record: " << line << std::endl;
      // dump strs
      std::copy(strs.begin(), strs.end(), 
          std::ostream_iterator<std::string>(std::cout, ","));
      exit(-1); // zero error tolerance
    }
    std::string url = strs[0];
    std::string alt_name = strs[1];

    // skip those queries without any alternative name
    if(0 == alt_name.compare(NA_STR)){
      continue;
    }

    if(g_alt_name_map.end() == g_alt_name_map.find(url)){
      g_alt_name_map[url] = std::vector<std::string>();
    }
    ::trim(alt_name);
    g_alt_name_map[url].push_back(alt_name);

    // verbose output
    if(VERBOSE){
      std::clog << "ALT-Name" << " [" << url << "] -> [";
      std::clog << alt_name << "]" << std::endl;
    }
  }
  return 0;
}

/*
 * Parse URL and extract the entity name
 */
std::string url2ent(std::string & url){
  // split the url, and get the sub-string after tha last '/'
  // http://stackoverflow.com/a/236976
  std::vector<std::string> strs;
  boost::split(strs, url, boost::is_any_of("/"));
  std::string ent = strs.back();
  ent = ::url_decode(ent);
  ::trim(ent);
  return ent;
}

/*
 * trim the string using boost::regex
 */
std::string & trim(std::string & str){
  // clean up terms in parentheses
  const boost::regex rm_paren_e("\\_\\(\\w+\\)");
  const std::string non_format("");
  str = boost::regex_replace(str, rm_paren_e, non_format, 
      boost::match_default | boost::format_sed);
  
  // clean up non-word characters, here \W is equivalent to [^[:word:]]
  const boost::regex clean_e("[\\W\\_]+");
  const std::string space_format(" ");
  str = boost::regex_replace(str, clean_e, space_format, 
      boost::match_default | boost::format_sed);

  // remove extra-spaces
  const boost::regex extra_space_e("\\s+");
  str = boost::regex_replace(str, clean_e, space_format, 
      boost::match_default | boost::format_sed);

  // remove leading spaces
  const boost::regex lead_space_e("^\\s+");
  str = boost::regex_replace(str, lead_space_e, non_format, 
      boost::match_default | boost::format_sed);

  // remove trailing spaces
  const boost::regex trail_space_e("\\s+$");
  str = boost::regex_replace(str, trail_space_e, non_format, 
      boost::match_default | boost::format_sed);

  // to lowercase, in STL style
  // Thanks to http://stackoverflow.com/a/313990/219617
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);

  // prepend and append a space respectively
  str.insert(0, 1, ' ');
  str.push_back(' ');

  return str;
}

