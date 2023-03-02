#include <omnetpp.h>

using namespace omnetpp;

class Coordinator: public cSimpleModule{
    /*Has a view of all workers through an object, containing info about if the worker is working and what task it is working on. also has an array to know if the worker is online or not.
    reads tasks and data, schedules tasks by selecting a non active worker and sending a message containing task and data. When there are no workers free waits for a completion message
    recieves a completion message -> changes the workers object, saves result data, schedules another task on that worker.
    after each task is dispatched set a timeout for a self message, if the message is recieved the corresponding worker is a struggler, thus it is considered failed and a stop execution message is setn to the worker, the task is put in a special queue. when a worker is free first take tasks from the queue only later from disk.
    if a backOnline message is recieved the worker is marked as online and considered free, a new task is scheduled.
    periodically the coordinator sends a ping to a worker and schedules a self message for the timeout, if no pong is recieved before the timeout the worker is considered failed, the array is updated and the task is put in the queue

    messages:
        executeTask(task,data)
        completion(workerId,result)
        strugglerTimeout(workerId)
        ping()
        pingTimeout(workerId)
        pong(workerId)
        backOnline(workerId)
        stopExecution()

    dataStructures:
        taskTracker: contains the id of the worker, the task exeuted on the data it is being executed on
        workerOnline: boolean array to know if a worker is online
        failedTaskQueue: queue of tasks and data
        tasks: order of tasks to be executed, possibly read from disk
        data: dataset, possibly read from disk

    open problems: how to store task order, how to distinguish tasks, workers should write the result on disk instead of returning in the message, coordinator should read from disk.
    */
}
