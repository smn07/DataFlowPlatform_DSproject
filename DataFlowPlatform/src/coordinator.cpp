#include <omnetpp.h>
#include <iostream>
#include <fstream>
#include <json/json.h>

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
        void endProgram(){return;};
        
        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;

        void handleTaskCompleted(TaskCompleted *msg);
        void handlePong(Pong *msg);
        void handlePingTimeout(PingTimeout *msg);
        void handleBackOnline(BackOnline *msg);

        //Vettore con struct che contiene le informazioni su id, stato e operazione in esecuzione di ogni worker
        vector<workersData> workersData;
        //Vettore di coppie <string,int> che rappresentano ogni singola operationze da eseguire
        vector<task> mapTaskQueue;
        task reduceTask;
        //Vettore con lista di coppie chiave valore divise in chunk
        vector<vector<pair<int,int>>> globalData;
        //Stack contenente l'indice del chunk su cui lavorare, l'indice dell'operazione da schedulare e un bool per sapere se è una map o una reduce
        stack<pair<bool,pair<int,int>>> currentTaskQueue;
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
        } else if (subroutine.key == "Map") {
            mapTaskQueue.push_back(subroutine.value);
        } else {
            reduceTask = subroutine.value;
        }
    }

    //fill currentTaskQueue
    for(int i=0;i<chunkNumber;i++){
        currentTaskQueue.push(pair{false,pair{i,0}});
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
    for(int i=0; i<workerNumber; i++) {
        struct workersData newElement{i,pair<int,int>{-1,0},true};
        workersData.push_back(newElement); //agiungo il nuovo elemento nell'array workersData
    }
    /*Setta l'id di ogni worker*/
    for(int i=0; i<workerNumber; i++){
        SetId *setmsg = new SetId();
        pingmsg->id=i;
        send(setmsg, "ports",i); 
    }

    /*Manda il primo ping ad ogni worker e schedula il timeout*/
    for(int i=0; i<workerNumber; i++){
        Ping *pingmsg = new Ping();
        pingmsg->p(i);
        send(pingmsg, "ports",i); 

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
        //Se vi sono worker liberi e task da schedulare
        pair<bool,pair<int,int>> currentTask=currentTaskQueue.pop();
        ExecuteTask *msg = new ExecuteTask();
        if (currentTask.first){
            //Se stiamo schedulando una reduce leggiamo da reduce data altrimenti leggiamo da globalData
            msg->chunk = reduceData[currentTask.first];
            msg->op = reduceTask;
            workersData[freeWorker].op = pair<int,int>{currentTask.second.first,-1};//il -1 indica che è una reduce
        } else {
            msg->chunk = globalData[currentTask.second.first];
            msg->op = mapTaskQueue[currentTask.second.second];
            workersData[freeWorker].op = currentTask.second;
        }
        
        send(msg,"ports",freeWorker);
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

        //check for next task to schedule
        if(taskCompleted.second == mapTaskQueue.size()){
            //reduce is next, need to sort the chunk
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
        
        if (taskCompleted.second == -1 && chunksDone()){
            //program is done, il -1 indica che abbiamo terminato una reduce
            endProgram();
        } else {
            //program is not done, schedule next op
            if (!currentTaskQueue.empty()){
                //There are tasks to schedule
                assignTask();
            } else if (chunksDone()){
                //we need to fill the queue
                for(int i=0;i<chunkNumber;i++){
                    chunkDone[i]=false;
                    if (taskCompleted.second == mapTaskQueue.size()){
                        //dobbiamo riempire con la reduce
                        currentTaskQueue.push(pair<bool,pair<int,int>>{true,pair<int,int>{i,-1}}); 
                    }
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
    currentTaskQueue.push(pair<bool,pair<int,int>>{failedTask.second==-1?true:false,pair<int,int>{failedTask.first,failedTask.second}});
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
