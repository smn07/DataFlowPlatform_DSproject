#include <omnetpp.h>
#include <std.h>
#include <iostream>
#include <fstream>
#include <json/json.h>



using namespace omnetpp;
using namespace std;

use task=Pair<String,int>;

struct WorkersData{
    int workerId;
    Task task;
    bool online;
    cModule *wo; //questo è il nuovo aggiornamento
} WorkersData_t


class Coordinator: public cSimpleModule{
    public:
        void run();

    private:
        void parseInput();
        void setup();
        void assignTask();

        Vector<WorkersData_t> workersData;
        Vector<Task> taskQueue;
        Vector<List<Pair<int,int>>> globalData;
        Stack<Pair<int,int>> currentTaskQueue;
        Vector<bool> chunkDone;
        Vector<List<Pair<int,int>>> reduceData;
        int chunkNumber;
        int workerNumber;

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
    assignTask();
}

void Coordinator::parseInput(){
    //read JSON and fill taskQueue, also read number of chunks
    Json::Value subroutines;
    std::ifstream program_file("program.json", std::ifstream::binary);
    program_file >> subroutines;

    //lettura array
    //decoding col foreach
    for(auto& subroutine : subroutines){
        if (subroutine.key == "chunks"){
            chunkNumber = subroutine.value;
        } else {
            taskQueue.add(subroutine.value);
        } 
    }

    //fill currentTaskQueue
    for(int i=0;i<chunkNumber;i++){
        currentTaskQueue.add(new Pair(i,0));
        chunkDone[i]=false;
    }

    //read CSV and fill 

    // Open the CSV file for reading
    std::ifstream csv_file("input.csv");

    // Create a vector of vectors to hold the parsed data
    std::vector<std::vector<std::string>> data;

    // Read each line of the CSV file
    std::string line;
    while (std::getline(csv_file, line)) {
        // Parse the line into fields using a stringstream
        std::stringstream ss(line);
        std::vector<std::string> fields;
        std::string field;
        while (std::getline(ss, field, ',')) {
            fields.push_back(field);
        }

        // Add the fields to the data vector
        data.push_back(fields);
    }

    // Print the parsed data to the console
    for (const auto& fields : data) {
        for (const auto& field : fields) {
            //std::cout << field << "\t";
        globalData[i%chunkNumber].add(field);
        i++;

        }
    }

    /*int i = 0;
    for(auto& pair : pair){
        globalData[i%chunkNumber].add(pair);
        i++;
    }*/



}

void Coordinator::setup(){
    /*WorkersData array definition*/
    cSimulation *sim = getSimulation();
    cModule *network = sim->getModuleByPath("DataflowPlatform");
    workerNumber = par("workerNumber").intValue(); //(questo non so se è giusto oppure se è network->par()..non capisco come accedere ai parametri della network)
    cModule *subModule = network->getSubmodule("workers");

    cModule *element = NULL; //attenzione ho fatto una modifica nella struct di WorkersData_t perchè non sapevo
                            //come associare l'elemento di tipo cModulo a quello di WorkerData_t e quindi ho fatto
                            //questa cosa. Non so se ha senso
    for(int i=0; i<workerNumber; i++) {
        element = subModule->getSubmoduleByIndex(i);
        WorkersData_t newElement;
        newElement.wo = element;
        newElement.workerID = i;
        newElement.Task = NULL;  //all'inizio non hanno nessun task assegnato
        newElement.online = true;  //ho assunto che di default siano attivi
        workersData.push_back(newElement); //agiungo il nuovo elemento nell'array workersData
    }

    //send ping for seeing if the worker is active (all'inizio sono tutti attivi)
    for(int i=0; i<workerNumber; i++){
        Ping *pingmsg = new Ping();
        pingmsg->p(i);
        send(pinbgmsg, "out"); //va finito non ho più tempo bella.

        PingTimeout *timeoutmsg = new PingTimeout();
        timeoutmsg->workerId = i;
        send(timeoutmsg, out[i], randomTime);
    }
    
    assignTask();
}

void Coordinator::assignTask(){
    int freeWorker = getFreeWorker();
    while(freeWorker>=0 && !currentTaskQueue.empty()){
        Pair<int,int> currentTask = currentTaskQueue.pop();
        ExecuteTask *msg = new ExecuteTask();
        if (currentTask.value == taskQueue.size()){
            msg->chunk = reduceData[currentTask.key];
        } else {
            msg->chunk = globalData[currentTask.key];
        }
        msg->op = taskQueue[currentTask.value];
        send(msg,out[freeWorker]);
        workersData[freeWorker].task = currentTask;
        freeWorker = getFreeWorker(); 
    }
}

void Coordinator::getFreeWorker(){
    for(workerData worker : workersData){
        if (worker.online && worker.task == NULL){
            return worker.workerId;
        }
        return -1;
    }
}

void Coordinator::handleTaskCompletion(TaskCompleted *msg){
    int workerId = msg->workerId;

    if(workersData[id].online){
        //update worker status
        Task taskCompleted = workersData[workerId].task;
        workersData[workerId].task = NULL;

        chunkDone[taskCompleted.key] = true;

        if(taskCompleted.value == taskQueue.size()-1){
            //change key finished, need to sort the output
            int chunkId = taskCompleted.key;
            for(Pair<int,int> pair : globalData[chunkId]){
                reduceData[pair.key % workerNumber].add(pair);
            }    
        }
        
        if (taskCompleted.value == taskQueue.size() && chunksDone()){
            //program is done
            endProgram();
        } else {
            //program is not done, schedule next op
            if (!currentTaskQueue.empty()){
                //There are tasks to schedule
                assignTask();
            } else if (chunksDone() && taskCompleted.value != taskQueue.size()){
                //we need to fill the queue
                for(int i=0;i<chunkNumber;i++){
                    chunkDone[i]=false;
                    currentTaskQueue.push(new Pair<int,int>(i,taskCompleted.value+1));
                }
            }
            //if the queue is empty but we are still processing some chunks we wait the next completion
        }
    }
}

void Coordinator::chunksDone(){
    for (bool chunk : chunkDone){
        if(!chunk){return false;}
    }
    return true;
}

void Coordinator::handlePong(Pong *msg){
    int id=msg->id;
    if(workersData[id].online){
        deleteMessage(pingTimeout(id)); // non funziona dobbiamo vedere come si fa
        send(new Ping(), out[id]);
        PingTimeout *timeoutmsg = new PingTimeout();
        timeoutmsg->workerId = i;
        send(timeoutmsg, out[i], randomTime);
    }
}

void Coordinator::handlePingTimeout(PingTimeout *msg){
    int id = msg->id;
    workersData[id].online = false;
    Task failedTask = workersData[id].task;
    workersData[id].task = NULL;
    currentTaskQueue.push(failedTask);
}

void Coordinator::handleBackOnline(BackOnline *msg){
    int id = BackOnline->id;
    workersData[id].online = true;
    assignTask();
}
