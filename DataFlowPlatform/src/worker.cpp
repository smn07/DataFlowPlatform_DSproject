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

        executeOperation(chunk,op);
        send(new ExecutionTime(),"in",randomTime);
    }
}

void Worker::handleExecutionTime(){
    TaskCompleted *msg = new TaskCompleted;
    msg->workerId = id;
    send(msg,"out");
}

void Worker::handleRecovery(){
    BackOnline *msg;
    msg->workerId = id;
    send(msg,"out");
}

void Worker::executeOperation(List<Pair<int,int>> chunk, Pair<String,int> op){
    switch (op.key)
    {
    case "ADD":
        for(Pair<int,int> pair : chunk){
            pair.value = pair.value + op.value;
        }
        break;
    
    case "SUB":
        for(Pair<int,int> pair : chunk){
            pair.value = pair.value - op.value;
        }
        break;
    
    case "MUL":
        for(Pair<int,int> pair : chunk){
            pair.value = pair.value * op.value;
        }
        break;

    case "DIV":
        for(Pair<int,int> pair : chunk){
            pair.value = pair.value / op.value;
        }
        break;

    case "CHANGEKEY":
        for(Pair<int,int> pair : chunk){
            int tmp = pair.value;
            pair.value = pair.key;
            pair.key = pair.value;
        }
        break;
    }

    case "REDUCE":
        switch (op.value)
        {
        case "ADD":
            Pair<int,int> accum = new Pair<int,int>(chunk.getFirst().key,0);
            List<Pair<int,int>> res = new List<Pair<int,int>>;
            for(int i=0;i<chunk.size();i++){
                Pair<int,int> pair = chunk.removeFirst();
                if (pair.key == accum.key){
                    accum.value = accum.value + pair.value;
                } else {
                    res.push(accum);
                    accum = new Pair<int,int>(pair.key,pair.value);
                }
            }
            chunk = res;
            break;

        case "SUB":
            Pair<int,int> accum = new Pair<int,int>(chunk.getFirst().key,0);
            List<Pair<int,int>> res = new List<Pair<int,int>>;
            for(int i=0;i<chunk.size();i++){
                Pair<int,int> pair = chunk.removeFirst();
                if (pair.key == accum.key){
                    accum.value = accum.value - pair.value;
                } else {
                    res.push(accum);
                    accum = new Pair<int,int>(pair.key,pair.value);
                }
            }
            chunk = res;
            break;

        case "MUL":
            Pair<int,int> accum = new Pair<int,int>(chunk.getFirst().key,1);
            List<Pair<int,int>> res = new List<Pair<int,int>>;
            for(int i=0;i<chunk.size();i++){
                Pair<int,int> pair = chunk.removeFirst();
                if (pair.key == accum.key){
                    accum.value = accum.value * pair.value;
                } else {
                    res.push(accum);
                    accum = new Pair<int,int>(pair.key,pair.value);
                }
            }
            chunk = res;
            break;
        }
    return;
}