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

        Vector<WorkersData_t> workersData;
        Vector<Task> taskQueue;
        Vector<List<Pair<int,int>>> globalData;
        Stack<Pair<int,int>> currentTaskQueue;

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
}

void Coordinator::parseInput(){
    //read JSON and fill taskQueue, also read number of chunks
    int chunkNumber;
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
    int workerNumber = par("workerNumber").intValue(); //(questo non so se è giusto oppure se è network->par()..non capisco come accedere ai parametri della network)
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
    Ping *msg = new Ping();
    msg->p(1);
    send(msg, "out"); //va finito non ho più tempo bella.
    
    
    
    //send tasks
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
