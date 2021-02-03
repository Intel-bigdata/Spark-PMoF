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

   PhysicalNode(string ip, string port) : ip(ip), port(port) {};

   PhysicalNode() = default;

   string getKey() const { return ip + ":" + port; }

   string getIp() { return ip; }

   string getPort() { return port; }

  private:
    string ip;
    string port;
};

inline bool operator==(const PhysicalNode &node1, const PhysicalNode &node2) {
  return node1.getKey() == node2.getKey();
}

class PhysicalNodeHash {
  public:
    size_t operator() (const PhysicalNode &node) const{
      std::hash<string> hash;
      return hash(node.getKey());
    }
};
#endif