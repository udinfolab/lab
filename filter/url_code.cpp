/*
 * Use STL and boost to apply URLEncode and URLDecode
 *
 */

#include <boost/function_output_iterator.hpp>
#include <boost/bind.hpp>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <iterator>
#include <iomanip>

/*
 * Thanks to: http://stackoverflow.com/a/7214192/219617
 */
namespace {
  std::string encimpl(std::string::value_type v) {
    if (isalnum(v))
      return std::string()+v;

    std::ostringstream enc;
    enc << '%' << std::setw(2) << std::setfill('0') << std::hex 
      << std::uppercase << int(static_cast<unsigned char>(v));
    return enc.str();
  }
}

std::string url_encode(const std::string& url) {
  // Find the start of the query string
  const std::string::const_iterator start = std::find(url.begin(), url.end(), '?');

  // If there isn't one there's nothing to do!
  if (start == url.end())
    return url;

  // store the modified query string
  std::string qstr;

  std::transform(start+1, url.end(),
                 // Append the transform result to qstr
                 boost::make_function_output_iterator(
                   boost::bind(static_cast<std::string& (std::string::*)(
                       const std::string&)>(&std::string::append),&qstr,_1)),
                 encimpl);
  return std::string(url.begin(), start+1) + qstr;
}

/*
 * Thanks to http://bit.ly/14a6NZC
 */

std::string url_decode(const std::string& in){
  std::string out;
  out.reserve(in.size());

  for (std::size_t i = 0; i < in.size(); ++i)
  {
    if (in[i] == '%')
    {
      if (i + 3 <= in.size())
      {
        int value = 0;
        std::istringstream is(in.substr(i + 1, 2));
        if (is >> std::hex >> value)
        {
          out += static_cast<char>(value);
          i += 2;
        }
        else
        {
          return out;
        }
      }
      else
      {
        return out;
      }
    }
    else if (in[i] == '+')
    {
      out += ' ';
    }
    else
    {
      out += in[i];
    }
  }
  return out;
}

