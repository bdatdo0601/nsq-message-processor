import subprocess
import os
import signal
import requests
import json
import time
import thread
import unittest
from threading import Timer


# instantiate a processor
mainProcess = subprocess.Popen(["python", "-u","main.py", "4", "3"], stdout=subprocess.PIPE)

class ProcessorUnitTesting(unittest.TestCase):
    def test_processor(self):
        try:
            ACTUAL_RESULT = {
                "id1": 200,
                "id2": 10
            }
            time.sleep(1) # wait for one second so the processor fully initiated
            input_http = json.loads(mainProcess.stdout.readline().rstrip())
            outputFilePath = mainProcess.stdout.readline().rstrip()
            url = "http://{address}/pub?topic=REQUEST".format(address=input_http["address"][0])
            result = {}
            for key, value in ACTUAL_RESULT.iteritems():
                for i in range(0, value):
                    data=json.dumps({ "guid": key })
                    res = requests.post(url, data=json.dumps({ "guid": key })).text
            time.sleep(70) # wait for more than 60 seconds to assert for low priority response 
            with open(outputFilePath, "r") as output:
                lines = output.readlines()
            print "Response: "
            for line in lines:
                print line
                result.update(json.loads(line))
            self.assertDictEqual(result, ACTUAL_RESULT)
            if os.path.exists(outputFilePath):
                    os.remove(outputFilePath)
        except KeyboardInterrupt:
            print "Test Terminated"
        finally:
            # Send the signal to all the process groups
            mainProcess.terminate()

if __name__ == "__main__":
    print("Begin Testing, Approximate Time Required: ~70secs")
    unittest.main()
