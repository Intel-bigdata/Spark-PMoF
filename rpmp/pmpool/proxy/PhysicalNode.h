#ifndef PMPOOL_PROXY_PHYSICALNODE_H_
#define PMPOOL_PROXY_PHYSICALNODE_H_

#include <string>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

using std::string;

class PhysicalNode {
  public:
   template <class Archive>
   void serialize(Archive &ar, const unsigned int version) {
     ar &ip;
     ar &port;
   }

   PhysicalNode(string ip, string port) : ip(ip), port(port) {}

   PhysicalNode() = default;

   string getKey() { return ip + ":" + port; }

   string getIp() { return ip; }

   string getPort() { return port; }

   bool operator==(PhysicalNode &node) {
     return (this->ip == node.getIp()) && (this->port == node.getPort());
    }

  private:
    string ip;
    string port;
};

#endif