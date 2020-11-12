#ifndef RPMP_CLIENT_H
#define RPMP_CLIENT_H

class MessageSender{
    public:
        void operator()(Connection* connection, ChunkMgr* chunkMgr);
};

#endif //RPMP_CLIENT_H
