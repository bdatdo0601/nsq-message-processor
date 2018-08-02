import json
from processor.nsqprocessor import NsqProcessor

try:
    processorInstance = NsqProcessor(requestProducerAddrList=["127.0.0.1:4250"],
                            fastLaneAddrList=["127.0.0.1:4252"], slowLaneAddrList=["127.0.0.1:4254"],
                            requestConsumerAddrList=["127.0.0.1:4256"], nsqlookupdList=["http://127.0.0.1:4361", "http://127.0.0.1:4363"],
                            http_input=["127.0.0.1:4251", "127.0.0.1:4253", "127.0.0.1:4255", "127.0.0.1:4257"])
    try:
        processorInstance.start_running()
    except KeyboardInterrupt:
        print "Interrupted"
    finally:
        processorInstance.stop_running()
except Exception as e:
    print e.message
    print "Addresses Unavailable! Check Cluster"






