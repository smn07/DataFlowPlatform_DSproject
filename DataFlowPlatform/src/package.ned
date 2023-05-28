package dataflowplatform;

@license(LGPL);


simple Worker
{
    parameters:
        double recovery @unit(ms) = default(uniform(500ms,750ms));
        double exec @unit(ms) = default(uniform(10ms,25ms));
        double failure = default(20);
    gates:
        inout port;
}

simple Coordinator
{
    parameters:
        int workerNumber = default(10);
        double timeout @unit(ms) = default(300ms);
        double pingInterval @unit(ms) = default(125ms);
    gates:
        inout ports[];
}

channel TCPChannel extends ned.DelayChannel
{
    delay = uniform(10ms,15ms);
}

network dataflowPlatform
{
    parameters:
        int workerNumber = default(10);
        @display("bgb=628,249");
    submodules:
        workers[workerNumber]: Worker {
            @display("b=30,30,rect,#00FF00,,4");
        }
        coordinator: Coordinator {
            @display("p=287,58;b=30,30,rect,#FF8800,,4");
        }
    connections:
        for i=0..workerNumber-1 {
            workers[i].port <--> TCPChannel <--> coordinator.ports++;
        }
}
