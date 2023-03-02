package dataflowplatform;

@license(LGPL);


simple worker{
    parameters:
        int id;
        bool running;
    gates:
        input in;
        output out;
}

simple coordinator{
    parameters: 
        int workers;
    gates:
        input in;
        output out;
}

channel TCPChannel extends ned.DatarateChannel{
    delay=uniform(100ms,150ms);
}

network dataflowPlatform{
    parameters:
        int workerNumber = default(10);
    submodules:
        workers[workerNumber]: worker {}
        coordinator: coordinator{}
        // i=0..n-1{workers[i].id=i;}
    connections:
        for i=0..n-1{
            workers[i].out --> TCPChannel --> coordinator.in;
            coordinator.out --> TCPChannel --> workers[i].in;
        }
}