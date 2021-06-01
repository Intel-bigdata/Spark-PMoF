#ifndef PMPOOL_PROXY_CONSISTENTHASH_H_
#define PMPOOL_PROXY_CONSISTENTHASH_H_

#include <map>
#include <string>
#include <vector>
#include <unordered_set>

#include "pmpool/proxy/PhysicalNode.h"
#include "pmpool/proxy/VirtualNode.h"
#include "pmpool/proxy/XXHash.h"

using namespace std;

class ConsistentHash {
  public:
    ConsistentHash(){};

    void addNode(PhysicalNode physicalNode, uint32_t loadBalanceFactor){
      nodes.insert(physicalNode.getKey());
      for (int i = 0; i < loadBalanceFactor; i++){
        VirtualNode virtualNode = {physicalNode, i};
        uint64_t hashValue = hashFactory->hash(virtualNode.getKey());
        ring.insert(pair<uint64_t, VirtualNode>(hashValue, virtualNode));
      }

      map<uint64_t, VirtualNode>::iterator itr;
      int counter = 0;
      for (itr = ring.begin(); itr != ring.end(); ++itr){
        counter++;
      }
    };

    void removeNode(PhysicalNode physicalNode){
      nodes.erase(physicalNode.getKey());
      map<uint64_t, VirtualNode>::iterator itr3;
      for (itr3 = ring.begin(); itr3 != ring.end(); ++itr3){
          cout << '\t' << itr3->first << '\t' << itr3->second.getKey() << '\n';
      }

      for (auto itr = ring.begin(); itr != ring.end();){
        if (itr->second.attachedTo(physicalNode)){
          ring.erase(itr++);
        }else{
          ++itr;
        }
      }

      /**
      map<uint64_t, VirtualNode>::iterator itr2;
      for (itr2 = ring.begin(); itr2 != ring.end(); ++itr2){
        cout << '\t' << itr2->first << '\t' << itr2->second.getKey() << '\n';
      }
      **/
    };                                                 

    PhysicalNode getNode(uint64_t hashValue){
        map<uint64_t, VirtualNode>::iterator itr = ring.lower_bound(hashValue);
        if (itr == ring.end()){
            PhysicalNode pTarget = ring.begin()->second.getPhysicalNode();
            uint64_t original_key = ring.begin()->first;
            return pTarget;
        }else{
            VirtualNode vTarget = itr->second;
            PhysicalNode pTarget = vTarget.getPhysicalNode();
            return pTarget;
        }
    }

    PhysicalNode getNode(std::string key){   
      uint64_t hashValue = hashFactory->hash(key);
      return getNode(hashValue);
    };

    unordered_set<PhysicalNode,PhysicalNodeHash> getNodes(uint64_t hashValue, uint32_t num) {
      uint32_t node_num = num < nodes.size() ? num : nodes.size();
      unordered_set<PhysicalNode,PhysicalNodeHash> pNodes;
      if (ring.begin() == ring.end()) {
        return pNodes;
      }
      auto it = ring.lower_bound(hashValue);
      it = it == ring.end() ? ring.begin() : it;
      auto begin = it;
      pNodes.insert(it->second.getPhysicalNode());
      ++it;
      for (int i = 1; i < node_num; i++) {
        it = it == ring.end() ? ring.begin() : it;
        while (true) {
          if (it == ring.end()) {
            it = ring.begin();
          }
          if (it == begin) {
            break;
          }
          if (pNodes.count(it->second.getPhysicalNode())) {
            ++it;
          } else {
            break;
          }
        }
        if (it == begin) {
          break;
        }
        pNodes.insert(it->second.getPhysicalNode());
        ++it;
      }
      return pNodes;
    }

    unordered_set<PhysicalNode, PhysicalNodeHash> getNodes(string key, uint32_t num) {
      uint64_t hashValue = hashFactory->hash(key);
      return getNodes(hashValue, num);
    }

    uint32_t getNodeNum() {
      return nodes.size();
    }

  private:
    map<uint64_t, VirtualNode> ring;
    unordered_set<string> nodes;
    XXHash *hashFactory = new XXHash();      
};

#endif
