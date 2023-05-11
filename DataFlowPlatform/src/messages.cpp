#include <omnetpp.h>

using namespace omnetpp;
using namespace std;

class Ping: public cMessage{
    private:
        int workerId;
    public:
        void setWorkerId(int par){
            workerId = par;
        }
        int getWorkerId(){
            return workerId;
        }
};

class ExecuteTask: public cMessage{
    private:
        vector<pair<int,int>> chunk;
        pair<string,int> op;
    public:
        void setChunk(vector<pair<int,int>> par){
            chunk = par;
        }
        vector<pair<int,int>> getChunk(){
            return chunk;
        }
        void setOp(pair<string,int> par){
            op = par;
        }
        pair<string,int> getOp(){
            return op;
        }
};

class TaskCompleted: public cMessage{
    private:
        int workerId;
        vector<pair<int,int>> result;
    public:
        void setWorkerId(int par){
            workerId = par;
        }
        int getWorkerId(){
            return workerId;
        }
        void setResult(vector<pair<int,int>> par){
            result = par;
        }
        vector<pair<int,int>> getResult(){
            return result;
        }
};

class PingTimeout: public cMessage{
    private:
        int workerId;
    public:
        void setWorkerId(int par){
            workerId = par;
        }
        int getWorkerId(){
            return workerId;
        }
        /*~PingTimeout(){
            drop();
        }*/
};

class Pong: public cMessage{
    private:
        int workerId;
    public:
        void setWorkerId(int par){
            workerId = par;
        }
        int getWorkerId(){
            return workerId;
        }
};

class BackOnline: public cMessage{
    private:
        int workerId;
    public:
        void setWorkerId(int par){
            workerId = par;
        }
        int getWorkerId(){
            return workerId;
        }
};

class ExecutionTime: public cMessage{};

class SetId: public cMessage{
    private:
        int workerId;
    public:
        void setWorkerId(int par){
            workerId = par;
        }
        int getWorkerId(){
            return workerId;
        }
};

class Recovery: public cMessage{
    private:
        int workerId;
    public:
        void setWorkerId(int par){
            workerId = par;
        }
        int getWorkerId(){
            return workerId;
        }
};
