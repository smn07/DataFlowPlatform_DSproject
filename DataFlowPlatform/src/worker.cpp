#include <omnetpp.h>

using namespace omnetpp;

use Task=vector<pair<int,int>>;
use FuncDef=vector<pair<String,int>>;

class Worker: public cSimpleModule{
    /*
    Workers are either mappers or reducers.
    
    SharedAttributes: int workerID; boolean failed; int failureProbability; boolean executing.
    SharedProcedures: pingPong, if a ping is recieved it responds with a pong.

    messages:
        Ping();
        Pong(workerId);
    */
   private:
        int id;
        bool failed;
        int failProb;
        void handlePing(){
            sendMessage(pong(id),coordinatorOut);
        }
}

class Mapper::Worker{
    //Remove terminator, change reducers

    /*
    recieves the map from the coordinator with a defineMap message and saves the list of instructions, recieves the GlobalData structure. Recieves the executeTask message from the coordinator and fails with an x% chance. If it fails then its failed flag becomes true and set a timer for a Recovery self message, after recieveing the flag becomes false and a backonline message is sent to the coordinator. If it does not fail then the task is executed and an executionTime selfmessage is schedules, in the meanwhile every message recieved is delayed and the flag executing is set.
    After the executionTime is recieved the computation is completed, the mapper saves the output locally, performs the shuffle, then computes using mod the reducers it will have to send keys to and send them a getPairs. The mapper also sends a mapCompletion to the coordinator.
    if a resendKey message is recieved the pairs for a reducer are sent again.

    Messages:
        defineMap(Map*);
        setData(GlobalData*);
        executeTask(int index);
        Recovery();
        backOnline(workerId);
        executionTime();
        getPairs(OutputPairs*, int taskIndex);
        Mapcompletion(workerId);
        resendKey(int reducerID);
        
    Config Data:
        int numberOfReducers;

    data structures:
        Map;
        GlobalData[] data;
        OutputPairs;

    */
   private:
        FuncDef Map;
        Vector<Task> tasks;
        Vector<Task> result;
        Pair<Vector<Task>,int> resultArchive;
        int reducerNumber;
        bool executing;

        void run();
        void handleDefineMap(message defineMap);
        void handleSetData(message setData);
        void handleExecuteTask(message executeTask);
}

void mapper::run(){
    getMessage(){
        while(executing){
            wait();
        }
        switch (expression)
        {
        case //All messages
            break;
        
        default:
            break;
        }
    }
}

void mapper::handleDefineMap(message defineMap){
    map = defineMap.map;
}

void mapper::handleSetData(message setData){
    data = setData.data;
}

void mapper::handleExecuteTask(message executeTask){
    //simulate failure;
    if (simulateFailure){
        failed = true;
        scheduleMessage(recovery, randTime);
    } else {
        int taskId = executeTask.id;
        executeMap(taskId);
        scheduleMessage(executionTime(taskId), randTime);
    }
}

void mapper::handleRecovery(){
    failed = false;
    sendMessage(backOnline(id),coordinatorOut);
}

void mapper::handleExecutionTime(message exectionTime){
    sendMessage(mapCompletion(id),coordinatorOut);
    orderOutput();
    sendOutput();
    resultArchive.add(<result,executionTime.taskId>);
    result.deleteAll();
    executing = false;
}

void mapper::handleResendKey(int reducerId){
    //Da rifare 
    for(Pair task in resultArchive){
        int taskId;
        if (outPair%reducerNumber == reducerId){
            sendMessage(getPairs(*outPair, ),reducerChannel[outpair.key%reducerNumber]);
        }
        
    }
}




class Reducer{
    /*
    recieves from the coordinator a setReduce message.
    recieves from the mapper the getPairs message and starts retrieving the pairs that belong to it, after that the task is set as fully read in tasksRead.
    the reducer initiates the reduce task and randomly fails. If it does not fail then it schedules an executionTime selfmessage and performs the reduce, in the meanwhile the executing flag is set. If it fails then its failed flag becomes true and set a timer for a Recovery self message, after recieveing the flag becomes false and a backonline message is sent to the coordinator.
    if a taskDone is recieved the output is written and the computation ends.
    if a getPairs is recieved for a task already read the reducer skips them.

    messages:
        setReduce(Reduce*);
        getPairs(OutputPairs*, int taskIndex);

    data structures:
        reduce;
        pairs;
        tasksRead;
    */

   private:
        FuncDef reduce;
        Task task;
        Vector<int> tasksRead;

        void handleSetReduce(FuncDef reduce);
        void handleGetPairs();
}
