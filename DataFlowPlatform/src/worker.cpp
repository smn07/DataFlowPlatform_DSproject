#include <omnetpp.h>

using namespace omnetpp;

class Worker: public cSimpleModule{
    /*
    Workers are either mappers or reducers.
    There is a static data structure shared across workers and coordinator.

    Messages:
    Execute Task: recieves from the coordinator the message to execute a task, the worker accesses the data in the global structure corresponding to its index and executes the task. The result is also saved to the global structure. After execution a timer is set for a executionTime selfmessage.
    Execution Time: the task is finished and a completion message can be sent out to the coordinator.
    If a ping is recieved it responds with a pong.
    If a stopExecuting message is recieved stop the current task.

    Failure model:
    randomly fail after recieving an execute task message. Set the failed flag to true and set a timer for a Recovery self message.
    after recieving the recovery send a backonline to coordinator

    messages:
        executeTask();
        executionTime();
        completion(workerId);
        Ping();
        Pong(workerId);
        Recovery();
        backOnline(workerId);
        stopExecution()

    data structures:
        GlobalData[] data;
        id of the worker;
        bool to know if the worker has failed

    technically the coordinator cannot schedule on a failed worker so the flag is not needed, this way it is more general
    */
}
