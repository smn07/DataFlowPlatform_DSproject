package dataflowplatform;

@license(LGPL);


simple Worker{
    parameters:
        int failure = uniform(1,9);
        int recovery = uniform(500ms,750ms);
        int exec = uniform(100ms,250ms);
    gates:
        inout port;
}

simple coordinator{
    parameters:
        int timeout = uniform(250ms,400ms);
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