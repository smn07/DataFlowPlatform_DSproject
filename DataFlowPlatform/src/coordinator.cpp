#include <omnetpp.h>
#include <iostream>
#include <fstream>
#include "json/json.h"
#include "messages.cpp"

using namespace omnetpp;
using namespace std;

using task=pair<string,int>;

struct workersData{
    int workerId;
    pair<int,int> op;
    bool online;
} workersData_t;

//Togliere il bool in currentTaskQueue


class Coordinator: public cSimpleModule{
    private:
        void parseInput();
        void setup();
        void assignTask();
        int getFreeWorker();
        bool chunksDone();
        void endProgram();
        static bool comparePairs(const pair<int,int>& a, const pair<int,int>& b);

        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;

        void handleTaskCompleted(TaskCompleted *msg);
        void handlePong(Pong *msg);
        void handlePingTimeout(PingTimeout *msg);
        void handleBackOnline(BackOnline *msg);
        void handleSendPing(SendPing *msg);


        //Vettore con struct che contiene le informazioni su id, stato e operazione in esecuzione di ogni worker
        vector<workersData> workersData;
        //Vettore di coppie <string,int> che rappresentano ogni singola operationze da eseguire
        vector<task> mapTaskQueue;
        task reduceTask;
        //Vettore con lista di coppie chiave valore divise in chunk
        vector<vector<pair<int,int>>> globalData;
        //Stack contenente l'indice del chunk su cui lavorare, l'indice dell'operazione da schedulare e un bool per sapere se è una map o una reduce
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

        int messageNumber;
        simtime_t startTime;
};

Define_Module(Coordinator);

void Coordinator::initialize(){
    messageNumber=0;
    startTime = simTime();
    parseInput();
    setup();
}

void Coordinator::parseInput(){

    // Read the JSON file
    ifstream file("program.json");
    if (!file.is_open()) {
        cout << "Failed to open json file" << endl;
        return;
    }

    // Read file content into a string
    string jsonData((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
    file.close();

    // Parse the JSON data
    Json::Value root;
    Json::Reader reader;
    if (!reader.parse(jsonData, root)) {
        cout << "Failed to parse JSON data: " << reader.getFormattedErrorMessages() << endl;
        return;
    }

    if (!root.isObject()) {
        cout << "Invalid JSON format. Root element must be an object." << endl;
        return;
    }

    chunkNumber = root["Chunks"].asInt();
    Json::Value mapArray = root["Map"];
    for (const Json::Value& map : mapArray) {
        for (Json::Value::const_iterator itr = map.begin(); itr != map.end(); ++itr) {
            const string keytmp = itr.key().asString();
            const int value = stoi((*itr).asString());
            mapTaskQueue.push_back(pair<string,int>{keytmp,value});
        }
    }
    auto reduce = root["Reduce"].asString();
    reduceTask = pair<string,int>{reduce,-1};

    //fill currentTaskQueue
    for(int i=0;i<chunkNumber;i++){
        currentTaskQueue.push(pair<int,int>{i,0});
        chunkDone.push_back(false);
    }

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

    // fill globaldata
    int i = 0;
    for (const auto& fields : data) {
        if(i<chunkNumber){
            vector<pair<int,int>> tmp;
            tmp.push_back(pair<int,int>{stoi(fields[0]),stoi(fields[1])});
            globalData.push_back(tmp);
        } else {
            globalData[i%chunkNumber].push_back(pair<int,int>{stoi(fields[0]),stoi(fields[1])});
        }
        i++;
   }
}

void Coordinator::setup(){

    workerNumber = par("workerNumber");
    for(int i=0; i<workerNumber; i++){
        timeoutId.push_back(nullptr);
    }

    /*WorkersData array definition*/
    for(int i=0; i<workerNumber; i++) {
        struct workersData newElement{i,pair<int,int>{-1,0},true};
        workersData.push_back(newElement); //agiungo il nuovo elemento nell'array workersData
    }
    /*Setta l'id di ogni worker*/
    for(int i=0; i<workerNumber; i++){
        SetId *setmsg = new SetId();
        setmsg->setWorkerId(i);
        send(setmsg, "ports$o",i);
        messageNumber++;
    }

    /*Manda il primo ping ad ogni worker e schedula il timeout*/
    for(int i=0; i<workerNumber; i++){
        Ping *pingmsg = new Ping();
        pingmsg->setWorkerId(i);
        send(pingmsg, "ports$o",i);
        messageNumber++;

        PingTimeout *timeoutmsg = new PingTimeout();
        timeoutmsg->setWorkerId(i);
        timeoutId[i] = timeoutmsg;
        scheduleAfter(par("timeout"),timeoutmsg);
    }

    for(int i=0; i<chunkNumber; i++){
        reduceData.push_back(vector<pair<int,int>>());
    }


    assignTask();
}

void Coordinator::assignTask(){
    int freeWorker = getFreeWorker();
    while(freeWorker>=0 && !currentTaskQueue.empty()){
        //Se vi sono worker liberi e task da schedulare
        pair<int,int> currentTask = currentTaskQueue.top();
        currentTaskQueue.pop();
        ExecuteTask *msg = new ExecuteTask();
        if (currentTask.second==-1){
            //Se stiamo schedulando una reduce leggiamo da reduce data altrimenti leggiamo da globalData
            msg->setChunk(reduceData[currentTask.first]);
            msg->setOp(reduceTask);
            workersData[freeWorker].op = pair<int,int>{currentTask.first,-1};//il -1 indica che è una reduce
        } else {
            msg->setChunk(globalData[currentTask.first]);
            msg->setOp(mapTaskQueue[currentTask.second]);
            workersData[freeWorker].op = currentTask;
        }
        send(msg,"ports$o",freeWorker);
        messageNumber++;
        freeWorker = getFreeWorker(); 
    }
}

int Coordinator::getFreeWorker(){
    for(auto worker : workersData){
        if (worker.online && worker.op.first==-1){
            return worker.workerId;
        }
    }
    return -1;
}

bool Coordinator::comparePairs(const pair<int,int>& a, const pair<int,int>& b) {
    return a.first < b.first;
}

void Coordinator::handleTaskCompleted(TaskCompleted *msg){
    messageNumber++;
    int workerId = msg->getWorkerId();
    if(workersData[workerId].online){
        //update worker status
        pair<int,int> taskCompleted = workersData[workerId].op;
        workersData[workerId].op = pair<int,int>{-1,0};
        if(taskCompleted.second == -1){
            //sovrascriviamo la reduce
            reduceData[taskCompleted.first] = msg->getResult();
        } else {
            //sovrascriviamo globalData
            auto res = msg->getResult();
            globalData[taskCompleted.first].clear();
            for(auto respair : res){
                globalData[taskCompleted.first].push_back(pair<int,int>{respair.first,respair.second});
            }
        }
        chunkDone[taskCompleted.first] = true;

        //if the next task to schedule is a reduce we sort the input
        if(taskCompleted.second == mapTaskQueue.size()-1){
            int chunkId = taskCompleted.first;
            for(pair<int,int> pair : globalData[chunkId]){
                reduceData[pair.first % chunkNumber].push_back(pair);
            }    
            for(auto& data : reduceData){
                sort(data.begin(), data.end());
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
                    if (taskCompleted.second == mapTaskQueue.size()-1){
                        //dobbiamo riempire con la reduce
                        if(reduceData[i].size() != 0){
                            currentTaskQueue.push(pair<int,int>{i,-1});
                        } else {
                            chunkDone[i]=true;
                        }
                    } else {
                        currentTaskQueue.push(pair<int,int>{i,taskCompleted.second+1});
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
    int id=msg->getWorkerId();
    if(workersData[id].online){
        try{
            cancelAndDelete(timeoutId[id]);
        } catch (const cRuntimeError e){
            drop(timeoutId[id]);
            delete timeoutId[id];
        }
        SendPing *sendPingmsg = new SendPing();
        sendPingmsg->setWorkerId(id);
        scheduleAfter(par("pingInterval"),sendPingmsg);
    }
}

void Coordinator::handleSendPing(SendPing *msg){
    int id=msg->getWorkerId();
    send(new Ping(), "ports$o",id);
    messageNumber++;
    PingTimeout *timeoutmsg = new PingTimeout();
    timeoutmsg->setWorkerId(id);
    timeoutId[id] = timeoutmsg;
    scheduleAfter(par("timeout"),timeoutmsg);
}

void Coordinator::handlePingTimeout(PingTimeout *msg){
    int id = msg->getWorkerId();
    timeoutId[id] = 0;
    workersData[id].online = false;
    if(workersData[id].op.first != -1){
        pair<int,int> failedTask = pair<int,int>{workersData[id].op.first,workersData[id].op.second};
        workersData[id].op = pair<int,int>{-1,0};
        currentTaskQueue.push(pair<int,int>{failedTask});
        assignTask();
    }
}

void Coordinator::handleBackOnline(BackOnline *msg){
    messageNumber++;
    int id = msg->getWorkerId();
    workersData[id].online = true;
    workersData[id].op = pair<int,int>{-1,0};
    send(new Ping(), "ports$o",id);
    messageNumber++;
    PingTimeout *timeoutmsg = new PingTimeout();
    timeoutmsg->setWorkerId(id);
    timeoutId[id] = timeoutmsg;
    scheduleAfter(par("timeout"),timeoutmsg);
    assignTask();
}

void Coordinator::handleMessage(cMessage *msg){
    TaskCompleted* taskmsg = dynamic_cast<TaskCompleted*>(msg);
    if(taskmsg!=nullptr){
        handleTaskCompleted(taskmsg);
    } else {
        Pong* pongmsg = dynamic_cast<Pong*>(msg);
        if (pongmsg!=nullptr){
            handlePong(pongmsg);
        } else {
            PingTimeout* pingtimeoutmsg = dynamic_cast<PingTimeout*>(msg);
            if(pingtimeoutmsg!=nullptr){
                handlePingTimeout(pingtimeoutmsg);
            } else {
                BackOnline* backonlinemsg = dynamic_cast<BackOnline*>(msg);
                if(backonlinemsg!=nullptr){
                    handleBackOnline(backonlinemsg);
                } else {
                    SendPing* sendPingmsg = dynamic_cast<SendPing*>(msg);
                    if(sendPingmsg!=nullptr){
                        handleSendPing(sendPingmsg);
                    } else {
                        throw invalid_argument("coordinator recieved invalid message type");
                    }
                }
            }
        }
    }
}

void Coordinator::endProgram(){

    cout << "writing on file" << endl;

    std::ofstream outFile("output.txt");

    if (!outFile) {
        std::cerr << "Failed to open the file for writing." << std::endl;
        return;
    }

    // Write the pairs to the file
    for (const auto& pairs : reduceData){
        for (const auto& pair : pairs) {
            outFile << pair.first << ", " << pair.second << std::endl;
        }
    }

    // Close the file
    outFile.close();

    std::cout << "Pairs have been written to the file." << std::endl;
    simtime_t endTime = simTime();
    recordScalar("simulation time",(endTime-startTime).dbl());
    recordScalar("messageNumber",messageNumber);

    endSimulation();
    return;
}

