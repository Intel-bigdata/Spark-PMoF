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
  ConsistentHash *consistentHash = new ConsistentHash();
  /**
   *
   * Create physical nodes
   *
   **/
  int loadBalanceFactor = 5;
  PhysicalNode *physicalNode1 = new PhysicalNode("host1", "12345");
  consistentHash->addNode(*physicalNode1, loadBalanceFactor);
  PhysicalNode *physicalNode2 = new PhysicalNode("host1", "12346");
  consistentHash->addNode(*physicalNode2, loadBalanceFactor);
  PhysicalNode *physicalNode3 = new PhysicalNode("host2", "12345");
  consistentHash->addNode(*physicalNode3, loadBalanceFactor);
  PhysicalNode *physicalNode4 = new PhysicalNode("host2", "12346");
  consistentHash->addNode(*physicalNode4, loadBalanceFactor);
  PhysicalNode *physicalNode5 = new PhysicalNode("host3", "12345");
  consistentHash->addNode(*physicalNode5, loadBalanceFactor);

  /**
   *
   * Get physical node for a shuffle block
   *
   **/
  string shuffle_key = "shuffleID";
  PhysicalNode node = consistentHash->getNode(shuffle_key);
  cout << "getdNode: " << node.getKey() << endl;

  /**
   * Get physical nodes for a shuffle block and block replication
   * */
  cout << "get nodes for replication" << endl;
  unordered_set<PhysicalNode, PhysicalNodeHash> pNodes = consistentHash->getNodes(shuffle_key, 3);
  for (auto pNode : pNodes) {
    cout << "getNode: " << pNode.getKey() << endl;
  }
  /**
   *
   * Remove one physical node
   *
   **/
  consistentHash->removeNode(*physicalNode4);


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
