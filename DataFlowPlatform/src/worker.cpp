#include <omnetpp.h>

using namespace omnetpp;
using namespace std;

use Task=vector<pair<int,int>>;
use FuncDef=vector<pair<String,int>>;

class Worker: public cSimpleModule{
   private:
        int id;
        bool failed;
        int failProb;
        void handlePing(){};
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
            scheduleAt(simTime()+uniform(500ms,700ms),new Recovery());
            return;
        }
        Vector<Pair<int,int>> chunk = msg->chunk;
        Pair<String,int> op = msg->op;

        Vector<Pair<int,int>> result = new Vector<Pair<int,int>>();
        executeOperation(chunk,op,reult);
        scheduleAt(simTime()+uniform(150ms,300ms),new ExecutionTime());
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

void Worker::executeOperation(Vector<Pair<int,int>> chunk, Pair<String,int> op, Vector<Pair<int,int>> result){
    Pair<int,int> res = new Pair<int,int>();
    switch (op.first)
    {
    case "ADD":
        for(Pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second + op.second;
            result.add(res);
        }
        break;
    
    case "SUB":
        for(Pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second - op.second;
            result.add(res);
        }
        break;
    
    case "MUL":
        for(Pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second * op.second;
            result.add(res);
        }
        break;

    case "DIV":
        for(Pair<int,int> pair : chunk){
            res.first = pair.first;
            res.second = pair.second / op.second;
            result.add(res);
        }
        break;

    case "CHANGEKEY":
        for(Pair<int,int> pair : chunk){
            res.first = pair.second;
            res.second = pair.first;
            result.add(res);
        }
        break;
    }

    case "REDUCE":
        switch (op.second)
        {
        case "ADD":
            res.first = chunk.getFirst().first;
            res.second = 0;
            for(Pair<int,int> pair : chunk){
                if(pair.first == res.first){
                    res.second += pair.second;
                } else {
                    result.add(res);
                    res.first = pair.first;
                    res.second = pair.second;
                }
            }
            break;

        case "SUB":
            res.first = chunk.getFirst().first;
            res.second = 0;
            for(Pair<int,int> pair : chunk){
                if(pair.first == res.first){
                    res.second -= pair.second;
                } else {
                    result.add(res);
                    res.first = pair.first;
                    res.second = pair.second;
                }
            }
            break;

        case "MUL":
            res.first = chunk.getFirst().first;
            res.second = 0;
            for(Pair<int,int> pair : chunk){
                if(pair.first == res.first){
                    res.second *= pair.second;
                } else {
                    result.add(res);
                    res.first = pair.first;
                    res.second = pair.second;
                }
            }
            break;
        }
    return;
}