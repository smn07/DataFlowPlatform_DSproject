message Ping {
    int p;
}

message executeTask{
    List<Pair<int,int>> chunk;
    Pair<String,int> op;
}

message taskCompleted{
    int workerId;
}

message pingTimeout{
    int workerId;
}

message Pong{
    int workerId;
}

message BackOnline{
    int workerId;
}

message executionTime(){}