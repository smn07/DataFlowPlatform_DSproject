#include <omnetpp.h>

using namespace omnetpp;

class Worker: public cSimpleModule{
    /*
    getTask: recieves message, if it is a task then execute it. After execution set a timer for a self message to simulate execution time, after recieving the message send the result out.
    randomly fail after recieving a task, set the failed flag to true, set a timer for a self messuge to get back online. when back online send a message to the coordinator.
    if a ping is recieved send a pong in response.
    if the execution takes too long a stopExecution is received, the executionTime message is destroied.

    messages:
        executeTask(task,data);
        completion(workerId,result);
        backOnline(workerId);
        executionTime();
        stopExecution()
        Ping()
        Pong(workerId)

    data structures:
        id of the worker;
        bool to know if the worker has failed

    technically the coordinator cannot schedule on a failed worker so the flag is not needed, this way it is more general
    */
}
