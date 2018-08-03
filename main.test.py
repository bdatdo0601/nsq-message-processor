import subprocess
import os
import signal
import requests
import json
import time
import thread
from threading import Timer

mockData = {
    "id1": 99,
    "id2": 12
}

mainProcess = subprocess.Popen(["python", "-u","main.py", "4", "3"], stdout=subprocess.PIPE)
    
try:
    time.sleep(1) # wait for one second so the processor fully initiated
    input_http = json.loads(mainProcess.stdout.readline().rstrip())
    outputFilePath = mainProcess.stdout.readline().rstrip()
    url = "http://{address}/pub?topic=REQUEST".format(address=input_http["address"][0])
    result = {}
    for key, value in mockData.iteritems():
        for i in range(0, value):
            data=json.dumps({ "guid": key })
            res = requests.post(url, data=json.dumps({ "guid": key })).text
    time.sleep(5)
    with open(outputFilePath, "r") as output:
        lines = output.readlines()
    for line in lines:
        result.update(json.loads(line))
    print result
except KeyboardInterrupt:
    print "Test Terminated"
finally:
    # Send the signal to all the process groups
    mainProcess.terminate()
