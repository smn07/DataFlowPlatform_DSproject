message Ping {
    int p;
}

message executeTask{
    List<Pair<int,int>> chunk;
    Pair<String,int> op;
}

message taskCompleted{
    int workerId;
    List<Pair<int,int>> result;
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

message executionTime{}

message SetId{
    int id;
}