#include <omnetpp.h>
#include <iostream>
#include <fstream>
#include <json/json.h>

//check for op null, va ripensato il json per la parte di reduce

using namespace omnetpp;
using namespace std;

using task=pair<string,int>;

struct workersData{
    int workerId;
    pair<int,int> op;
    bool online;
} workersData_t;


class Coordinator: public cSimpleModule{
    public:
        void run();

    private:
        void parseInput();
        void setup();
        void assignTask();
        int getFreeWorker();
        bool chunksDone();
        void endProgram(){return};
        
        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;

        void handleTaskCompleted(TaskCompleted *msg);
        void handlePong(Pong *msg);
        void handlePingTimeout(PingTimeout *msg);
        void handleBackOnline(BackOnline *msg);

        //Vettore con struct che contiene le informazioni su id, stato e operazione in esecuzione di ogni worker
        vector<workersData> workersData;
        //Vettore di coppie <string,int> che rappresentano ogni singola operationze da eseguire
        vector<task> taskQueue;
        //Vettore con lista di coppie chiave valore divise in chunk
        vector<vector<pair<int,int>>> globalData;
        //Stack contenente le operazioni da schedulare
        stack<pair<int,int>> currentTaskQueue;
        //Vettore con le informaizoni su quali chunk sono stati completati
        vector<bool> chunkDone;
        //Sorted Input per la reduce
        vector<vector<pair<int,int>>> reduceData;
        //Vettore degli id dei pingTimeout message
        vector<cMessage*> timeoutId;
        //numero di chunk in cui dividere l'input
        int chunkNumber;
        //numero di worker
        int workerNumber;
};

void Coordinator::initialize(){
    parseInput();
    setup();
    assignTask();
}

void Coordinator::parseInput(){
    //read JSON and fill taskQueue, also read number of chunks
    Json::Value subroutines;
    ifstream program_file("program.json", ifstream::binary);
    program_file >> subroutines;

    //lettura array
    //decoding col foreach
    for(auto& subroutine : subroutines){
        if (subroutine.key == "chunks"){
            chunkNumber = subroutine.value;
        } else {
            taskQueue.push_back(subroutine.value);
        } 
    }

    //fill currentTaskQueue
    for(int i=0;i<chunkNumber;i++){
        currentTaskQueue.push(pair{i,0});
        chunkDone[i]=false;
    }

    //read CSV and fill 

    // Open the CSV file for reading
    ifstream csv_file("input.csv");

    // Create a vector of vectors to hold the parsed data
    vector<vector<string>> data;

    // Read each line of the CSV file
    string line;
    while (getline(csv_file, line)) {
        // Parse the line into fields using a stringstream
        stringstream ss(line);
        vector<string> fields;
        string field;
        while (getline(ss, field, ',')) {
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
        struct workersData newElement{i,pair<int,int>{-1,0},true};
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
        send(pingmsg, "ports",i); //va finito non ho più tempo bella.

        PingTimeout *timeoutmsg = new PingTimeout();
        timeoutmsg->workerId = i;
        timeoutId[i] = timeoutmsg;
        scheduleAt(simTime()+par("timeout"),timeoutmsg);
    }
    
    assignTask();
}

void Coordinator::assignTask(){
    int freeWorker = getFreeWorker();
    while(freeWorker>=0 && !currentTaskQueue.empty()){
        pair<int,int> currentTask=currentTaskQueue.pop();
        ExecuteTask *msg = new ExecuteTask();
        if (currentTask.second == taskQueue.size()){
            msg->chunk = reduceData[currentTask.first];
        } else {
            msg->chunk = globalData[currentTask.first];
        }
        msg->op = taskQueue[currentTask.second];
        send(msg,"ports",freeWorker);
        workersData[freeWorker].op = currentTask;
        freeWorker = getFreeWorker(); 
    }
}

int Coordinator::getFreeWorker(){
    for(auto worker : workersData){
        if (worker.online && worker.op == pair<int,int>{-1,0}){
            return worker.workerId;
        }
        return -1;
    }
}

void Coordinator::handleTaskCompleted(TaskCompleted *msg){
    int workerId = msg->workerId;

    if(workersData[workerId].online){
        //update worker status
        pair<int,int> taskCompleted = workersData[workerId].op;
        workersData[workerId].op = pair<int,int>{-1,0};
        globalData[taskCompleted.first] = msg->result;

        chunkDone[taskCompleted.first] = true;

        if(taskCompleted.second == taskQueue.size()-1){
            //change key finished, need to sort the output
            int chunkId = taskCompleted.first;
            for(pair<int,int> pair : globalData[chunkId]){
                reduceData[pair.first % workerNumber].push_back(pair);
            }    
            for(auto data : reduceData){
                for(int i=0;i<data.size();i++){
                    for(int j=i;j<data.size();j++){
                        if (data[i].first>data[j].first){
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
                    currentTaskQueue.push(pair<int,int>{i,taskCompleted.second+1});
                }
                assignTask();
            }
            //if the queue is empty but we are still processing some chunks we wait the next completion
        }
    }
}

bool Coordinator::chunksDone(){
    for (bool chunk : chunkDone){
        if(!chunk){return false;}
    }
    return true;
}

void Coordinator::handlePong(Pong *msg){
    int id=msg->id;
    if(workersData[id].online){
        delete timeoutId[id]; 
        send(new Ping(), "ports",id);
        PingTimeout *timeoutmsg = new PingTimeout();
        timeoutmsg->workerId = id;
        timeoutId[id] = timeoutmsg;
        scheduleAt(simTime()+par("timeout"),timeoutmsg);
    }
}

void Coordinator::handlePingTimeout(PingTimeout *msg){
    int id = msg->id;
    timeoutId[id] = 0;
    workersData[id].online = false;
    pair<int,int> failedTask = workersData[id].op;
    workersData[id].op = pair<int,int>{-1,0};
    currentTaskQueue.push(failedTask);
}

void Coordinator::handleBackOnline(BackOnline *msg){
    int id = msg->id;
    workersData[id].online = true;
    assignTask();
}

void Coordinator::handleMessage(cMessage *msg){
    switch (msg->getKind())
    {
    case TaskCompleted:
        handleTaskCompleted(msg);
        break;
    case Pong:
        handlePong(msg);
        break;
    case PingTimeout:
        handlePingTimeout(msg);
        break;
    case BackOnline:
        handleBackOnline(msg);
        break;
}
