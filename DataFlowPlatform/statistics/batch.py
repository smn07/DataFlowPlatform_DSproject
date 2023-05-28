import json
import os

# reference data
workerNumberReference = 10
failureReference = 2
with open('reference.json','r') as jsonRef:
    jsonReference = jsonRef.readlines()
with open('reference.csv','r') as csvRef:
    csvReference = csvRef.readlines()

def setReference():
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
    with open('../src/omnetpp.ini', 'w') as f:
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

with open('statistics.txt','a') as stats:
    #number of workers
    setReference()
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
            sim_line=None
            mess_line=None
            lines = f.readlines()
            res = (lines[-2].split(" "))[-2] + " " + (lines[-2].split(" "))[-1]
            stats.write(res)
            for i, line in enumerate(lines):
                if line.startswith('scalar dataflowPlatform.coordinator "simulation time"'):
                    sim_line = i
                if line.startswith('scalar dataflowPlatform.coordinator messageNumbe'):
                    mess_line = i
            if sim_line is not None:
                res = "simulation time: " + (lines[sim_line].split(" "))[-1]
            stats.write(res)
            res = ''
            if mess_line is not None:
                res = "messageNumber: " + (lines[mess_line].split(" "))[-1]
            stats.write(res+'\n')
            res = ''

    #program lenght
    setReference()
    for value in numberOfOperations:
        # modify the json file
        with open('reference.json', 'r') as f:
            program=json.load(f)
        if(value < len(program['Map'])):
            program['Map'] = program['Map'][:value]
        elif(value > len(program['Map'])):
            program['Map'] += program['Map']
        with open('../src/program.json', 'w') as f:
            json.dump(program,f)

        # run the simulation
        os.system('cd ../src && ./DataFlowPlatform_dbg')

        # Collect and process the simulation results
        with open('../src/results/General-#0.sca') as f:
            sim_line=None
            mess_line=None
            lines = f.readlines()
            res = 'Number of map operations: {}\n'.format(value)
            stats.write(res)
            for i, line in enumerate(lines):
                if line.startswith('scalar dataflowPlatform.coordinator "simulation time"'):
                    sim_line = i
                if line.startswith('scalar dataflowPlatform.coordinator messageNumbe'):
                    mess_line = i
            if sim_line is not None:
                res = "simulation time: " + (lines[sim_line].split(" "))[-1]
            stats.write(res)
            res = ''
            if mess_line is not None:
                res = "messageNumber: " + (lines[mess_line].split(" "))[-1]
            stats.write(res+'\n')
            res = ''

    #input size
    setReference()
    for value in numberOfPairs:
        # modify the csv file
        with open('reference.csv', 'r') as f:
            lines = f.readlines()
        if(value < len(lines)):
            lines = lines[:value]
        elif(value > len(lines)):
            while(value > len(lines)):
                lines += lines
            if(value < len(lines)):
                lines = lines[:value]
        with open('../src/input.csv', 'w') as f:
            f.writelines(lines)

        # run the simulation
        os.system('cd ../src && ./DataFlowPlatform_dbg')

        # Collect and process the simulation results
        with open('../src/results/General-#0.sca') as f:
            sim_line=None
            mess_line=None
            lines = f.readlines()
            res = 'Number of input pairs: {}\n'.format(value)
            stats.write(res)
            for i, line in enumerate(lines):
                if line.startswith('scalar dataflowPlatform.coordinator "simulation time"'):
                    sim_line = i
                if line.startswith('scalar dataflowPlatform.coordinator messageNumbe'):
                    mess_line = i
            if sim_line is not None:
                res = "simulation time: " + (lines[sim_line].split(" "))[-1]
            stats.write(res)
            res = ''
            if mess_line is not None:
                res = "messageNumber: " + (lines[mess_line].split(" "))[-1]
            stats.write(res+'\n')

    #failure probability
    setReference()
    for value in failProb:
        # modify the ini file
        with open('../src/omnetpp.ini', 'r') as f:
            lines = f.readlines()
        param_line = None
        for i, line in enumerate(lines):
            if line.startswith('**.workers*.failure'):
                param_line = i
                break
        if param_line is not None:
            lines[param_line] = '**.workers*.failure = {}\n'.format(value)
        with open('../src/omnetpp.ini', 'w') as f:
            f.writelines(lines)

        # run the simulation
        os.system('cd ../src && ./DataFlowPlatform_dbg')

        # Collect and process the simulation results
        with open('../src/results/General-#0.sca') as f:
            sim_line=None
            mess_line=None
            lines = f.readlines()
            res = (lines[-10].split(" "))[-2] + " " + (lines[-10].split(" "))[-1]
            stats.write(res)
            for i, line in enumerate(lines):
                if line.startswith('scalar dataflowPlatform.coordinator "simulation time"'):
                    sim_line = i
                if line.startswith('scalar dataflowPlatform.coordinator messageNumbe'):
                    mess_line = i
            if sim_line is not None:
                res = "simulation time: " + (lines[sim_line].split(" "))[-1]
            stats.write(res)
            res = ''
            if mess_line is not None:
                res = "messageNumber: " + (lines[mess_line].split(" "))[-1]
            stats.write(res+'\n')
            res = ''