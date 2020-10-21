
class PhysicalNode: public Node{
  public:
    PhysicalNode(string key){
      this->key = key;
    }

    string getKey(){
      return key;
    }
  private:
    string key;
};
