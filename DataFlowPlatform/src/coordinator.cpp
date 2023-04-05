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
    Pair<int,int> op;
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

        //Vettore con struct che contiene le informazioni su id, stato e operazione in esecuzione di ogni worker
        Vector<WorkersData_t> workersData;
        //Vettore di coppie <string,int> che rappresentano ogni singola operationze da eseguire
        Vector<Task> taskQueue;
        //Vettore con lista di coppie chiave valore divise in chunk
        Vector<Vector<Pair<int,int>>> globalData;
        //Stack contenente le operazioni da schedulare
        Stack<Pair<int,int>> currentTaskQueue;
        //Vettore con le informaizoni su quali chunk sono stati completati
        Vector<bool> chunkDone;
        //Sorted Input per la reduce
        Vector<Vector<Pair<int,int>>> reduceData;
        //Vettore degli id dei pingTimeout message
        Vector<long> timeoutId;
        //numero di chunk in cui dividere l'input
        int chunkNumber;
        //numero di worker
        int workerNumber;
}

Coordinator(){
    timeoutId = new Vector<long>(workerNumber);
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

    for(int i=0; i<workerNumber; i++){
        SetId *setmsg = new SetId();
        pingmsg->id=i;
        send(setmsg, "ports",i); 
    }

    //send ping for seeing if the worker is active (all'inizio sono tutti attivi)
    for(int i=0; i<workerNumber; i++){
        Ping *pingmsg = new Ping();
        pingmsg->p(i);
        send(pinbgmsg, "ports",i); //va finito non ho più tempo bella.

        PingTimeout *timeoutmsg = new PingTimeout();
        timeoutmsg->workerId = i;
        workerNumber[i] = getId(timeoutmsg);
        scheduleAt(simTime()+uniform(250ms,400ms),timeoutmsg);
    }
    
    assignTask();
}

void Coordinator::assignTask(){
    int freeWorker = getFreeWorker();
    while(freeWorker>=0 && !currentTaskQueue.empty()){
        Pair<int,int> currentTask = currentTaskQueue.pop();
        ExecuteTask *msg = new ExecuteTask();
        if (currentTask.second == taskQueue.size()){
            msg->chunk = reduceData[currentTask.first];
        } else {
            msg->chunk = globalData[currentTask.first];
        }
        msg->op = taskQueue[currentTask.second];
        send(msg,"ports",freeWorker);
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

void Coordinator::handleTaskCompleted(TaskCompleted *msg){
    int workerId = msg->workerId;

    if(workersData[id].online){
        //update worker status
        Pair<int,int> taskCompleted = workersData[workerId].task;
        workersData[workerId].task = NULL;
        globalData[taskCompleted.first] = msg->result;

        chunkDone[taskCompleted.first] = true;

        if(taskCompleted.second == taskQueue.size()-1){
            //change key finished, need to sort the output
            int chunkId = taskCompleted.first;
            for(Pair<int,int> pair : globalData[chunkId]){
                reduceData[pair.first % workerNumber].add(pair);
            }    
            for(auto data : reduceData){
                for(int i=0;i<data.size();i++){
                    for(int j=i;j<data.size();j++){
                        if data[i].first>data[j].first{
                            auto tmp = data[j];
                            data[j] = data[i];
                            data[i] = tmp;
                        }
                    }
                }
            }
        }
        
        if (taskCompleted.second == taskQueue.size() && chunksDone()){
            //program is done
            endProgram();
        } else {
            //program is not done, schedule next op
            if (!currentTaskQueue.empty()){
                //There are tasks to schedule
                assignTask();
            } else if (chunksDone() && taskCompleted.second != taskQueue.size()){
                //we need to fill the queue
                for(int i=0;i<chunkNumber;i++){
                    chunkDone[i]=false;
                    currentTaskQueue.push(new Pair<int,int>(i,taskCompleted.second+1));
                }
                assignTask();
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
        deleteMessage(timeoutId[id]); 
        send(new Ping(), "ports",id);
        PingTimeout *timeoutmsg = new PingTimeout();
        timeoutmsg->workerId = id;
        timeoutId[id] = getId(timeoutmsg);
        scheduleAt(simTime()+uniform(250ms,400ms),timeoutmsg);
    }
}

void Coordinator::handlePingTimeout(PingTimeout *msg){
    int id = msg->id;
    timeoutId[id] = 0;
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
