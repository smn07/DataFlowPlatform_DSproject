package dataflowplatform;

@license(LGPL);


simple Worker
{
    parameters:
        double recovery @unit(ms) = uniform(500ms,750ms);
        double exec @unit(ms) = uniform(100ms,250ms);
        int failure =  intuniform(1,9);
    gates:
        inout port;
}

simple Coordinator
{
    parameters:
        double timeout @unit(ms)= uniform(250ms,400ms);
    gates:
        inout ports[];
}

channel TCPChannel extends ned.DelayChannel
{
    delay = uniform(100ms,150ms);
}

network dataflowPlatform
{
    parameters:
        int workerNumber = default(10);
        @display("bgb=628,249");
    submodules:
        workers[workerNumber]: Worker;
        coordinator: Coordinator {
            @display("p=284,50");
        }
    connections:
        for i=0..workerNumber-1 {
            workers[i].port <--> TCPChannel <--> coordinator.ports++;
        }
}
