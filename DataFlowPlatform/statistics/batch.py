import os

with open('statistics.txt','a') as stats:

    # reference data
    workerNumberReference = 10
    failureReference = 2
    with open('reference.json','r') as jsonRef:
        jsonReference = jsonRef.readlines()
    with open('reference.csv','r') as csvRef:
        csvReference = csvRef.readlines()

    # set reference
    with open('../src/program.json','w') as f:
        f.writelines(jsonReference)

    with open('../src/input.csv','w') as f:
        f.writelines(csvReference)

    with open('../src/omnetpp.ini', 'r') as f:
        lines = f.readlines()
    param_line = None
    for i, line in enumerate(lines):
        if line.startswith('**.workerNumber'):
            param_line = i
            break
    if param_line is not None:
        lines[param_line] = '**.workerNumber = {}\n'.format(workerNumberReference)
    with open('omnetpp.ini', 'w') as f:
        f.writelines(lines)

    with open('../src/omnetpp.ini', 'r') as f:
        lines = f.readlines()
    param_line = None
    for i, line in enumerate(lines):
        if line.startswith('**.workers*.failure'):
            param_line = i
            break
    if param_line is not None:
        lines[param_line] = '**.workers*.failure = {}\n'.format(workerNumberReference)
    with open('../src/omnetpp.ini', 'w') as f:
        f.writelines(lines)

    # test parameters
    workerNumber = [1,2,5,10,20]
    numberOfOperations = [1,2,3,5,10]
    numberOfPairs = [50,100,200,400,1000]
    failProb = [1,2,5,10,20]

    # running tests
    for value in workerNumber:
        # modify the ini file
        with open('../src/omnetpp.ini', 'r') as f:
            lines = f.readlines()
        param_line = None
        for i, line in enumerate(lines):
            if line.startswith('**.workerNumber'):
                param_line = i
                break
        if param_line is not None:
            lines[param_line] = '**.workerNumber = {}\n'.format(value)
        with open('../src/omnetpp.ini', 'w') as f:
            f.writelines(lines)

        # run the simulation
        os.system('cd ../src && ./DataFlowPlatform_dbg')

        # Collect and process the simulation results
        with open('../src/results/General-#0.sca') as f:
            lines = f.readlines()
            res = (lines[-2].split(" "))[-2] + " " + (lines[-2].split(" "))[-1]
            stats.write(res)
            for i, line in enumerate(lines):
                if line.startswith('scalar dataflowPlatform.coordinator "simulation time"'):
                    param_line = i
                    break
            if param_line is not None:
                res = "simulation time: " + (lines[param_line].split(" "))[-1]
            stats.write(res)
            if line.startswith('scalar dataflowPlatform.coordinator messageNumber'):
                param_line = i
                break
            if param_line is not None:
                res = "messageNumber: " + (lines[param_line].split(" "))[-1]
            stats.write(res+'\n')