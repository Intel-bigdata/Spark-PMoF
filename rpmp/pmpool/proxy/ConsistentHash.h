#ifndef PMPOOL_PROXY_CONSISTENTHASH_H_
#define PMPOOL_PROXY_CONSISTENTHASH_H_

#include <map>
#include <string>
#include "pmpool/proxy/Node.h"
#include "pmpool/proxy/VirtualNode.h"
#include "pmpool/proxy/XXHash.h"

using namespace std;

template <class T> 
class ConsistentHash {
  public:
    ConsistentHash<T>(){
    };

    void addNode(T physicalNode, int loadBalanceFactor){                         
      Node *node2 = new VirtualNode(physicalNode, 1);
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

    void removeNode(T physicalNode){           

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

  private:
    map<uint64_t, VirtualNode> ring;
    IHash *hashFactory = new XXHash();      
};

#endif
