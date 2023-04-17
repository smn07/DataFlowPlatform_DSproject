#include <omnetpp.h>

using namespace omnetpp;
using namespace std;

//VA fatto l'handle message

using task=vector<pair<int,int>>;
using funcDef=vector<pair<string,int>>;

class Worker: public cSimpleModule{
   private:
        int id;
        bool failed;
        int failProb;
        vector<pair<int,int>> result;

        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;
        void handlePing();
        void handleSetId(SetId *msg);
        void handleExecuteTask(ExecuteTask *msg);
        void handleExecutionTime();
        void handleRecovery();
        void executeOperation(vector<pair<int,int>> chunk, pair<string,int> op);

};

void Worker::initialize(){
    failed = false;
    failProb = par("failure");
}

void Worker::handleSetId(SetId *msg){
    id = msg->id;
}

void Worker::handlePing(){
    if(!failed){
        Pong *msg = new Pong();
        msg->workerId = id;
        send(msg,"port");
    }
}

void Worker::handleExecuteTask(ExecuteTask *msg){
    if(!failed){
        if(randomFail){
            failed = true;
            scheduleAt(simTime()+par("recovery"),new Recovery());
            return;
        }
        vector<pair<int,int>> chunk = msg->chunk;
        pair<string,int> op = msg->op;

        executeOperation(chunk,op);
        scheduleAt(simTime()+par("exec"),new ExecutionTime());
    }
}

void Worker::handleExecutionTime(){
    TaskCompleted *msg = new TaskCompleted;
    msg->workerId = id;
    msg->result = result;
    send(msg,"port");
}

void Worker::handleRecovery(){
    BackOnline *msg;
    msg->workerId = id;
    send(msg,"port");
}

void Worker::executeOperation(vector<pair<int,int>> chunk, pair<string,int> op){
    pair<int,int> res;
    if(op.first=="ADD"){
        for(pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second + op.second;
            result.push_back(res);
        }
    } else if (op.first=="SUB"){
        for(pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second - op.second;
            result.push_back(res);
        }
    } else if (op.first=="DIV"){
        for(pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second / op.second;
            result.push_back(res);
        }
    } else if (op.first=="CHANGEKEY"){
        for(pair<int,int> pair : chunk){
            res.first = pair.second;
            res.second = pair.first;
            result.push_back(res);
        }
    } else if (op.first=="REDUCE"){
        if(op.second==1){
            //ADD
            res.first = chunk.front().first;
            res.second = 0;
            for(pair<int,int> pair : chunk){
                if(pair.first == res.first){
                    res.second += pair.second;
                } else {
                    result.push_back(res);
                    res.first = pair.first;
                    res.second = pair.second;
                }
            }
        } else if (op.second==2){
            //SUB
            res.first = chunk.front().first;
            res.second = 0;
            for(pair<int,int> pair : chunk){
                if(pair.first == res.first){
                    res.second -= pair.second;
                } else {
                    result.push_back(res);
                    res.first = pair.first;
                    res.second = pair.second;
                }
            }
        } else if (op.second==3){
            //MUL
            res.first = chunk.front().first;
            res.second = 1;
            for(pair<int,int> pair : chunk){
                if(pair.first == res.first){
                    res.second *= pair.second;
                } else {
                    result.push_back(res);
                    res.first = pair.first;
                    res.second = pair.second;
                }
            }
        }
    }
}