#ifndef PMPOOL_PROXY_NODE_H_
#define PMPOOL_PROXY_NODE_H_

#include <iostream>
#include <string>

using namespace std;

class Node{
  private:
    string key;
  public:
    virtual string getKey() = 0;
};

#endif
