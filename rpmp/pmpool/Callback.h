#ifndef RPMOF_CALLBACK_H
#define RPMOF_CALLBACK_H

#include "pmpool/HeartbeatClient.h"
#include "pmpool/DataService/DataServerService.h"

/**
 * A callback to take action when the built connection is shut down.
 */
class ConnectionShutdownCallback : public Callback {
public:
    explicit ConnectionShutdownCallback(std::shared_ptr<HeartbeatClient> heartbeatClient,
                                        std::shared_ptr<DataServerService> dataService);
    ~ConnectionShutdownCallback() override = default;
    void operator()(void* param_1, void* param_2);

private:
    std::shared_ptr<HeartbeatClient> heartbeatClient_;
    std::shared_ptr<DataServerService> dataService_;
};

#endif //RPMOF_CALLBACK_H
