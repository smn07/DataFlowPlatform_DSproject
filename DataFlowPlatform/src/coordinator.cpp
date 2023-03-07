#include <omnetpp.h>

using namespace omnetpp;

class Coordinator: public cSimpleModule{
    /*
    At Runtime:
    Has a view of all workers through an object, containing info about if the worker is working and what task it is working on. Also has an array to know if the worker is online or not.
    the coordinator first reads the data and partitions it according to the number of mappers, then puts the data for a worker in a global static array of objects representing the data. The coordinator will then send an executeMap to each mapper telling it to execute the map on the data at its index.
    when a Mapcompetion message is recieved the info about that worker is updated, the coordinator will read a keys from the results of that mapper and send an executeReduce(key) to each nonworking reducer that may need to work a key. If needed a failed map is scheduled on that free mapper. 
    when a RecudeCompletion message is recieved the info about that worker is updated.

    Fail tolerance:
    the coordinato periodically sends a ping to the workers and sets a pingTimeout selfmessage. if the timeout is recieved before the pong the worker is considered failed (info updated). if a mapper fails a stopExecution is sent to every reducer that was working on the keys in its current output, its task is put into a queue. if a reducer fails its reduce is put into a queue.
    when a task is dispathed a strugglerTimeout is set, if it is recieved before the worker finisches it is considered failed and a stopExecution is sent.
    when a back online message is recieved the info about the worker is updated and if possible a new task if scheduled.

    messages:
        executeMap();
        executeReduce(key);
        Mapcompletion(workerId);
        ReduceCompletion(workerId);
        ping();
        pingTimeout(workerId);
        pong(workerId);
        strugglerTimeout(workerId);
        backOnline(workerId);
        stopExecution();

    dataStructures:
        GlobalData[] data;
        workersData: contains id, type, task, working or failed;
        failedMapQueue and failedReduceQueue;
        MapTask: operations in a map
        ReduceTask: operations in a reduce
        data: dataset
    */
}
