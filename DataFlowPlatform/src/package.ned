package dataflowplatform;

@license(LGPL);


simple Worker{
    gates:
        inout port;
}

simple coordinator{
    gates:
        inout ports[];
}

channel TCPChannel extends ned.DelayChannel{
    delay=uniform(100ms,150ms);
}

network dataflowPlatform{
    parameters:
        int workerNumber = default(10);
    submodules:
        workers[workerNumber]: Worker;
        coordinator: Coordinator;
    connections:
        for i=0..workerNumber-1{
            workers[i].port <--> TCPChannel <--> coordinator.ports++;
        }
}