#include <omnetpp.h>
#include "messages.cpp"

using namespace omnetpp;
using namespace std;

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

Define_Module(Worker);

void Worker::initialize(){
    failed = false;
    failProb = par("failure").intValue();
}

void Worker::handleSetId(SetId *msg){
    id = msg->getWorkerId();
    EV << id << endl;
}

void Worker::handlePing(){
    if(!failed){
        Pong *msg = new Pong();
        msg->setWorkerId(id);
        send(msg,"port$o");
    }
}

void Worker::handleExecuteTask(ExecuteTask *msg){
    if(!failed){
        if(rand()%(10-failProb) == 0){
            cout << "worker " << id << " failed" << endl;
            failed = true;
            scheduleAt(simTime()+par("recovery"),new Recovery());
            return;
        }
        cout << "worker " << id << " executing task" << endl;
        vector<pair<int,int>> chunk = msg->getChunk();
        pair<string,int> op = msg->getOp();

        executeOperation(chunk,op);
        cout << "worker " << id << " scheduling execution time" << endl;
        scheduleAt(simTime()+par("exec"),new ExecutionTime());
    }
}

void Worker::handleExecutionTime(){
    TaskCompleted *msg = new TaskCompleted();
    msg->setWorkerId(id);
    msg->setResult(result);
    cout << "worker " << id << " sending task completed" << endl;
    send(msg,"port$o");
}

void Worker::handleRecovery(){
    BackOnline *msg = new BackOnline();
    msg->setWorkerId(id);
    send(msg,"port$o");
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
    } else if (op.first=="REDUCEADD"){
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
    } else if (op.first=="REDUCESUB"){
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
    } else if (op.first=="REDUCEMUL"){
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
    cout << "worker " << id << " exeuction done" << endl;
}

void Worker::handleMessage(cMessage *msg){
    SetId* setidmsg = dynamic_cast<SetId*>(msg);
    if(setidmsg!=nullptr){
        handleSetId(setidmsg);
    } else {
        Ping* pingmsg = dynamic_cast<Ping*>(msg);
        if (pingmsg!=nullptr){
            handlePing();
        } else {
            ExecuteTask* executetaskmsg = dynamic_cast<ExecuteTask*>(msg);
            if(executetaskmsg!=nullptr){
                handleExecuteTask(executetaskmsg);
            } else {
                Recovery* recoverymsg = dynamic_cast<Recovery*>(msg);
                if(recoverymsg!=nullptr){
                    handleRecovery();
                } else {
                    ExecutionTime* executiontimemsg = dynamic_cast<ExecutionTime*>(msg);
                    if(executiontimemsg!=nullptr){
                        handleExecutionTime();
                    } else {
                        throw invalid_argument("coordinator recieved invalid message type");
                    }
                }
            }
        }
    }
}
