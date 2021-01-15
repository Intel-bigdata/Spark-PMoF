#ifndef PMPOOL_PROXY_VIRTUALNODE_H_
#define PMPOOL_PROXY_VIRTUALNODE_H_

#include "PhysicalNode.h"

using namespace std;

class VirtualNode {
  public:
    VirtualNode(const PhysicalNode &physicalNode, int index):physicalNode("", ""){
      this->physicalNode = physicalNode;
      this->index = index;
    }

    string getKey(){
      return physicalNode.getKey() + "-" + to_string(index);
    }

    PhysicalNode getPhysicalNode(){
      return physicalNode;
    }

    bool attachedTo(PhysicalNode physicalNode2){
      if(physicalNode2.getKey().compare(physicalNode.getKey()) == 0){
        return true;
      }

      return false;
    }

  private:
    PhysicalNode physicalNode;
    long index;
};

#endif
