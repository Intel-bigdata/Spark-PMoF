#include <iostream>
#include <libcuckoo/cuckoohash_map.hh>
#include "include/xxhash/xxhash.hpp"
#include <string>
#include "pmpool/proxy/ConsistentHash.h"

using namespace std;

xxh::hash64_t hash2(string key){
  xxh::hash64_t key_i = xxh::xxhash<64>(key); 
  return key_i;
}

void consistent_hash_test(){
  ConsistentHash<PhysicalNode> *consistentHash = new ConsistentHash<PhysicalNode>();
  /**
   *
   * Create two physical nodes
   *
   **/
  PhysicalNode *physicalNode = new PhysicalNode("host1");
  int loadBalanceFactor = 5;
  consistentHash->addNode(*physicalNode, loadBalanceFactor);

  PhysicalNode *physicalNode2 = new PhysicalNode("host2");
  consistentHash->addNode(*physicalNode2, 3);

  /**
   *
   * Get physical node for a shuffle block
   *
   **/
  string shuffle_key = "shuffleID";
  PhysicalNode node = consistentHash->getNode(shuffle_key);
  cout << "getdNode: " << node.getKey() << endl;

  /**
   *
   * Remove one physical node
   *
   **/
  consistentHash->removeNode(*physicalNode);


  /**
   *
   * Get physical node for a shuffle block again
   *
   **/
  PhysicalNode node2 = consistentHash->getNode("shuffleID");
  cout << "getNode: " << node2.getKey() << endl;
}

int main(int argc, char **argv){
  consistent_hash_test();
	return 0;	
}
