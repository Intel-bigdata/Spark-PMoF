
class PhysicalNode: public Node{
  public:
    PhysicalNode(string ip, string port){
      this->key = ip + port;
      this->ip = ip;
      this->port = port;
    }

    string getKey(){
      return key;
    }

    string getIp() {
      return ip;
    }

    string getPort() {
      return port;
    }
    
  private:
    string key;
    string ip;
    string port;
};
