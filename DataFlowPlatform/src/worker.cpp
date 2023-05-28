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
        double failProb;
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
    failProb = par("failure").doubleValue();
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
        if(uniform(0,100) < failProb){
            failed = true;
            scheduleAfter(par("recovery"),new Recovery());
            bubble("Node Failed");
            return;
        }
        vector<pair<int,int>> chunk = msg->getChunk();
        pair<string,int> op = msg->getOp();

        executeOperation(chunk,op);
        ExecutionTime *endmsg = new ExecutionTime();
        double execTime = par("exec");
        scheduleAfter(execTime*chunk.size(),endmsg);
    } else {
        scheduleAfter(par("recovery"),new Recovery());
    }
}

void Worker::handleExecutionTime(){
    TaskCompleted *msg = new TaskCompleted();
    msg->setWorkerId(id);
    msg->setResult(result);
    send(msg,"port$o");
}

void Worker::handleRecovery(){
    failed = false;
    BackOnline *msg = new BackOnline();
    msg->setWorkerId(id);
    send(msg,"port$o");
    bubble("Node Back Online");
}

void Worker::executeOperation(vector<pair<int,int>> chunk, pair<string,int> op){
    pair<int,int> res = pair<int,int>{0,0};
    result.clear();
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
        res = pair<int,int>{0,1};
        for(pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second / op.second;
            result.push_back(res);
        }
    } else if (op.first=="MUL"){
        res = pair<int,int>{0,1};
        for(pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second * op.second;
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
                result.push_back(::pair<int,int>{res.first,res.second});
                res.first = pair.first;
                res.second = pair.second;
            }
        }
        result.push_back(::pair<int,int>{res.first,res.second});
    } else if (op.first=="REDUCESUB"){
        //SUB
        res.first = chunk.front().first;
        res.second = 0;
        for(pair<int,int> pair : chunk){
            if(pair.first == res.first){
                res.second -= pair.second;
            } else {
                result.push_back(::pair<int,int>{res.first,res.second});
                res.first = pair.first;
                res.second = pair.second;
            }
        }
        result.push_back(::pair<int,int>{res.first,res.second});
    } else if (op.first=="REDUCEMUL"){
        //MUL
        res.first = chunk.front().first;
        res.second = 1;
        for(pair<int,int> pair : chunk){
            if(pair.first == res.first){
                res.second *= pair.second;
            } else {
                result.push_back(::pair<int,int>{res.first,res.second});
                res.first = pair.first;
                res.second = pair.second;
            }
        }
        result.push_back(::pair<int,int>{res.first,res.second});
    }
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
