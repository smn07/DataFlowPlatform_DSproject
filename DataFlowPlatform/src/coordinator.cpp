#include <omnetpp.h>
#include <std.h>

using namespace omnetpp;
using namespace std;

struct WorkersData{
    int id;
    bool type; //0 mapper, 1 reducer
    int taskId;
    bool online;
} WorkersData_t

use Task=pair<int,int>;
use FuncDef=vector<pair<String,int>>;

class Coordinator: public cSimpleModule{
    /*

    //Implement message to change reducers after failure 

    The coordinator has an array workersData, containing informations about the id, type, task executed (array of index of tasks done) and state of each worker.
    Initially the Coordinator read and validates the JSON file with the map and the reduce and the input data. Then divides the input into tasks and populates the GlobalData, Map, Reduce and taskQueue structure.
    
    At Runtime:
    First the coordinator sends a defineMap and defineReduce to all workers, to tell them of the current Map and Reduce. Also sends a setData to mappers with a pointer to the GlobalData.
    The coordinator sends and executeTask message to a mapper, with the index of the GlobalData to read. Then it will update the workersData structure.
    When a taskCompletion message is recieved the info about that worker is updated, another task from the queue is scheduled on that free mapper with the usual executeTask message. 

    The coordinator will also periodically ping all workers by sending a ping and scheduling a self pingTimeout message, if no pong is recieved before a pingTimeout for that worker then it is considered failed and its worker data is updated. 
    If a mapper fails its task is put on top of the queue.
    If a reducer fails all its work is lost, we wait for it to come back online.
    
    When a back online message is recieved the info about the worker is updated. If it is a mapper a new task is scheduled, if it is a reducer all mappers are informed and send the corresponding key.

    when all tasks are completed a message is sent to the reducers telling to output the final result.

    messages:
        defineMap(Map*);
        defineReduce(Reduce*);
        setData(GlobalData*);
        executeTask(int index);
        Mapcompletion(workerId);
        ping(workerId);
        pingTimeout(workerId);
        pong(workerId);
        resendKey(int reducerID);
        backOnline(workerId);
        TasksDone();

        // strugglerTimeout(workerId);
        // stopExecution();
        
        

    dataStructures:
        GlobalData[] data: input data;
        Map: map program;
        Reduce: reduce program;
        workersData: contains id, type, task, working or failed;
        taskQueue: queue of int;
    */
    public:
        void run();

    private:
        void parseInput();
        void setup();
        FuncDef parseMap(JSON);
        FuncDef parseReduce(JSON);
        FuncDef parseTasks(JSON,CSV);

        FuncDef map;
        FuncDef reduce;
        Vector<Task> tasks;
        Vector<WorkersData_t> workersData;
        Stack<int> taskQueue;

        int freeMapper(); //checks that there is a free mapper;
        int getFreeMapper(); //Gets a free mapper by reading workersData;
}

int main(){
    Coordinator coordinator = new Coordinator();
    coordinator.run();
}

void Coordinator::run(){
    parseInput();
    setup();
    while(!stack.empty() && freeWorker()){
        int task = stack.pop();
        int worker = getFreeMapper();
        sendMessage(executeTask(task),mapperChannel(worker));
        workersData[worker].taskId = task;
    }
    for(int i=0; i<WorkersData.size;i++){
        sendMessage(ping(),outChannels[i]);
        scheduleMessage(pingTimeout(i), self);
    }
}

void Coordinator::parseInput(){
    //read JSON and CSV
    map = parseMap(JSON);
    reduce = parseReduce(JSON);
    tasks = parseTasks(JSON,CSV);
}

void Coordinator::setup(){
    sendMessage(defineMap(map), mapperChannels);
    sendMessage(defineReduce(reduce), reduceChannels);
    sendMessage(setData(tasks), mapperChannels);
    for(i=0;i<tasks.size;i++){
        stack.push(i);
    }
}

void Coordinator::handleTaskCompletion(message mapCompletio){
    int mapperId = mapCompletion.workerId;
    if(WorkersData[mapperId].online){
        WorkersData[mapperId].task = 0;
        if(!stack.empty()){
            int task = stack.pop();
            sendMessage(executeTask(task),mapperChannels(mapperId));
            WorkersData[mapperId].task = task;
        }
    }
}

void Coordinator::handlePong(message ping){
    int id=ping.id;
    if(WorkersData[id].online=true){
        deleteMessage(pingTimeout(id));
        sendMessage(ping(id),outChannels[id]);
        scheduleMessage(pingTimeout(id), self);
    }
}

void Coordinator::handlePingTimeout(message pingTimeout){
    int id = pingTimeout.id;
    sendMessage(stopExecution,outChannel);
    WorkersData[id].online = false;
    if (WorkersData[id].type){
        //è un mapper
        stack.push(WorkersData[id].task);
        WorkersData[id].task = 0;
    } else {
        //è un reducer, va modificato il comportamento
    }
}

void Coordinator::handleBackOnline(message backOnline){
    int id = backOnline.id;
    WorkersData[id].online = true;
    if(WorkersData[id].type){
        if(!stack.empty()){
            int task = stack.pop();
            sendMessage(executeTask(task),mapperChannel(id));
            WorkersData[id].task = task;
            sendMessage(ping(id),outChannels[id]);
            scheduleMessage(pingTimeout(id), self);
        }
    } else {
        sendMessage(resendKey(id),mapperChannels);
    }
}
