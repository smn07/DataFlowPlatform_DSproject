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
        send(msg,"out");
    }
}

void Worker::handleExecuteTask(ExecuteTask *msg){
    if(!failed){
        if(randomFail){
            failed = true;
            send(new Recovery(),"in",randomTime);
            return;
        }
        List<Pair<int,int>> chunk = msg->chunk;
        Pair<String,int> op = msg->op;

        List<Pair<int,int>> result = new List<Pair<int,int>>();
        executeOperation(chunk,op,reult);
        send(new ExecutionTime(),"in",randomTime);
    }
}

void Worker::handleExecutionTime(){
    TaskCompleted *msg = new TaskCompleted;
    msg->workerId = id;
    msg->result = result;
    send(msg,"out");
}

void Worker::handleRecovery(){
    BackOnline *msg;
    msg->workerId = id;
    send(msg,"out");
}

void Worker::executeOperation(List<Pair<int,int>> chunk, Pair<String,int> op, List<Pair<int,int>> result){
    Pair<int,int> res = new Pair<int,int>();
    switch (op.key)
    {
    case "ADD":
        for(Pair<int,int> pair : chunk){
            res.key = pair.key;
            res.value = pair.value + op.value;
            result.add(res);
        }
        break;
    
    case "SUB":
        for(Pair<int,int> pair : chunk){
            res.key = pair.key;
            res.value = pair.value - op.value;
            result.add(res);
        }
        break;
    
    case "MUL":
        for(Pair<int,int> pair : chunk){
            res.key = pair.key;
            res.value = pair.value * op.value;
            result.add(res);
        }
        break;

    case "DIV":
        for(Pair<int,int> pair : chunk){
            res.key = pair.key;
            res.value = pair.value / op.value;
            result.add(res);
        }
        break;

    case "CHANGEKEY":
        for(Pair<int,int> pair : chunk){
            res.key = pair.value;
            res.value = pair.key;
            result.add(res);
        }
        break;
    }

    case "REDUCE":
        switch (op.value)
        {
        case "ADD":
            res.key = chunk.getFirst().key;
            res.value = 0;
            for(Pair<int,int> pair : chunk){
                if(pair.key == res.key){
                    res.value += pair.value;
                } else {
                    result.add(res);
                    res.key = pair.key;
                    res.value = value.value;
                }
            }
            break;

        case "SUB":
            res.key = chunk.getFirst().key;
            res.value = 0;
            for(Pair<int,int> pair : chunk){
                if(pair.key == res.key){
                    res.value -= pair.value;
                } else {
                    result.add(res);
                    res.key = pair.key;
                    res.value = value.value;
                }
            }
            break;

        case "MUL":
            res.key = chunk.getFirst().key;
            res.value = 0;
            for(Pair<int,int> pair : chunk){
                if(pair.key == res.key){
                    res.value *= pair.value;
                } else {
                    result.add(res);
                    res.key = pair.key;
                    res.value = value.value;
                }
            }
            break;
        }
    return;
}