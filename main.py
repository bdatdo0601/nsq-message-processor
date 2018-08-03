import json
import argparse
import os
from subprocess import Popen, PIPE, STDOUT
from processor.nsqprocessor import NsqProcessor

"""
MAIN.PY

This will instantiate a nsq cluster along with a processor
that will listen to incoming messages
"""

parser = argparse.ArgumentParser("nsq processor")
parser.add_argument(
    "nsqdAmt", help="amount of nsqd instances", type=int)
parser.add_argument(
    "nsqlookupdAmt", help="amount of nsqlookupd instances", type=int)
argList = parser.parse_args()

if (argList.nsqdAmt < argList.nsqlookupdAmt):
    print("Nsqlookupd amount should be smaller than nsqd amount")
    exit()

# instatiate nsq cluster instance
nsqClusterInstance = Popen(["ruby", "cluster/start.rb", "-n", str(argList.nsqdAmt), "-l", str(argList.nsqlookupdAmt)], stdin=PIPE, stdout=PIPE, stderr=STDOUT)

# get nsqClusterInstace
nsqClusterData = {}
# read output line by line
while True:
    # send a command to get cluster data
    nsqClusterInstance.stdin.write("\n")
    # check if instance has terminated:
    if nsqClusterInstance.poll() is not None:
        print("Cluster instance has terminated.")
        exit()
    # if not, get the next line response
    line = nsqClusterInstance.stdout.readline().rstrip()
    if (line == "[end]"):
        break
    # attempt to parse the line as list of json data
    try:
        nsqClusterData = json.loads(line)
        if (len(nsqClusterData) != 0):
            break
    except ValueError:
        pass

if (len(nsqClusterData) == 0):
    print("cannot instaniate the cluster")
    exit()


try:
    # distribute nsqd into 4 roughly equal-size list
    nsqdList = [ [] for i in range(0, 4) ]
    index = 0
    for nsqd in nsqClusterData["nsqd"]:
        nsqdList[index].append(nsqd)
        index = (index + 1) if (index < len(nsqdList) - 1) else 0
    # create a file for message output
    outputFilePath = os.path.dirname(os.path.abspath(__file__)) + "/output.txt"
    # instatiate processor
    processorInstance = NsqProcessor(requestProducerAddrList=nsqdList[0],
                            fastLaneAddrList=nsqdList[1], slowLaneAddrList=nsqdList[2],
                            requestConsumerAddrList=nsqdList[3], nsqlookupdList=nsqClusterData["nsqlookupd"],
                            http_input=nsqClusterData["nsqd_http"],
                            outputfiledir=outputFilePath)
    try:
        processorInstance.start_running()
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        processorInstance.stop_running()
        # exit the cluster
        nsqClusterInstance.stdin.write("exit\n")
except Exception as e:
    print("Addresses Unavailable! Check Cluster")






