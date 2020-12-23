#ifndef PMPOOL_PROXY_CONSISTENTHASH_H_
#define PMPOOL_PROXY_CONSISTENTHASH_H_

#include <map>
#include <string>
#include <vector>
#include <unordered_set>
#include "pmpool/proxy/Node.h"
#include "pmpool/proxy/VirtualNode.h"
#include "pmpool/proxy/XXHash.h"

using namespace std;

class ConsistentHash {
  public:
    ConsistentHash(){};

    void addNode(PhysicalNode physicalNode, int loadBalanceFactor){
      dataNodes.insert(physicalNode.getKey());
      for (int i = 0; i < loadBalanceFactor; i++){
        VirtualNode *virtualNode = new VirtualNode(physicalNode, i);
        uint64_t hashValue = hashFactory->hash(virtualNode->getKey());
        ring.insert(pair<uint64_t, VirtualNode>(hashValue, *virtualNode));
      }

      map<uint64_t, VirtualNode>::iterator itr;
      for (itr = ring.begin(); itr != ring.end(); ++itr){
        cout << '\t' << itr->first << '\t' << itr->second.getKey() << '\n';
      }

    };

    void removeNode(PhysicalNode physicalNode){
      dataNodes.erase(physicalNode.getKey());
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

    vector<string> getNodes(uint64_t hashValue, uint32_t num) {
      uint32_t node_num = num < dataNodes.size() ? num : dataNodes.size();
      unordered_set<string> pNodes;
      map<uint64_t, VirtualNode>::iterator itr = ring.lower_bound(hashValue);
      itr = itr == ring.end() ? ring.begin() : itr;
      map<uint64_t, VirtualNode>::iterator begin = itr;
      for (int i=0; i<node_num; i++) {
        itr = itr == ring.end() ? ring.begin() : itr;
        while (pNodes.count(itr->second.getPhysicalNode().getKey())) {
          ++itr;
        }
        pNodes.insert(itr->second.getPhysicalNode().getKey());
        ++itr;
        if (itr == begin) {
          break;
        }
      }

      return vector<string>(pNodes.begin(), pNodes.end());
    }

    vector<string> getNodes(string key, uint32_t num) {
      uint64_t hashValue = hashFactory->hash(key);
      return getNodes(hashValue, num);
    }

  private:
    map<uint64_t, VirtualNode> ring;
    unordered_set<string> dataNodes;
    IHash *hashFactory = new XXHash();      
};

#endif
