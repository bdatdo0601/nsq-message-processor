import nsq
import tornado.ioloop
import datetime
import json
import functools
import threading
import requests
from repeatedTimer import RepeatedTimer

# INPUT MESSAGE FORMAT 
# { guid: <vid_id> } --> { guid: <vid_id>, count: <count> }

# LANE MESSAGE FORMAT
# { guid: <vid_id>, count: <count> } -> { <vid_id>: <count>, <vid_id>: <count>}

# OUTPUT: MESSAGE FORMAT
# { <vid_id>: <count> }

TOPIC = {
    "FAST_LANE": "FAST_LANE",
    "SLOW_LANE": "SLOW_LANE",
    "PUBLISH": "PUBLISH",
    "REQUEST": "REQUEST"
}

def onFinish(conn, data):
    pass
    # print conn
    # print data


class NsqProcessor(object):
    __videoListCount = {}
    __slowLaneCache = {}
    def __init__(self, requestProducerAddrList,
                    fastLaneAddrList, slowLaneAddrList, 
                    requestConsumerAddrList, http_input,
                    nsqlookupdList=[],
                    slowLaneCacheLimit=20,
                    playCountToCacheThreshold=100,
                    timeDifferenceLimit=60):
        self.requestProducerAddrList = requestProducerAddrList
        self.fastLaneAddrList = fastLaneAddrList
        self.slowLaneAddrList = slowLaneAddrList
        self.requestConsumerAddrList = requestConsumerAddrList
        self.nsqlookupdList = nsqlookupdList
        self.__slowLaneCacheLimit = slowLaneCacheLimit
        self.__playCountToCacheThreshold = playCountToCacheThreshold
        self.__initializeNsqlookupd()
        self.__inititalizeNsqdWriters()
        self.__initializeNsqdReader()
        self.__http_input = http_input
        self.__timeDifferenceLimit=timeDifferenceLimit
        self.__intervalFunction = RepeatedTimer(timeDifferenceLimit, self.__tick)

    def __initializeNsqlookupd(self):
        for nsqlookupd in self.nsqlookupdList:
            for topic in TOPIC:
                requests.post(
                    "{nsqlookupdAddr}/topic/create?topic={topic}".format(nsqlookupdAddr=nsqlookupd, topic=topic))

    def __inititalizeNsqdWriters(self):
        # write locally since topic will be polled to nsqlookupd
        self.__requestProducerWriter = nsq.Writer(self.requestProducerAddrList)
        self.__fastLaneWriter = nsq.Writer(self.fastLaneAddrList)
        self.__slowLaneWriter = nsq.Writer(self.slowLaneAddrList)

    def __initializeNsqdReader(self):
        nsq.Reader(message_handler=self.__onFastLaneHandler,
                        nsqd_tcp_addresses=self.fastLaneAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["FAST_LANE"], channel="propagate",
                        max_in_flight=self.__slowLaneCacheLimit,
                        lookupd_poll_interval=1)
        nsq.Reader(message_handler=self.__onSlowLaneHandler,
                        nsqd_tcp_addresses=self.slowLaneAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["SLOW_LANE"], channel="cache",
                        max_in_flight=self.__slowLaneCacheLimit,
                        lookupd_poll_interval=1)
        nsq.Reader(message_handler=self.__onPublishHandler,
                        nsqd_tcp_addresses=self.requestConsumerAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["PUBLISH"], channel="publish",
                        max_in_flight=self.__slowLaneCacheLimit,
                        lookupd_poll_interval=1)
        nsq.Reader(message_handler=self.__onRequestHandler,
                        nsqd_tcp_addresses=self.requestProducerAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["REQUEST"], channel="receive",
                        lookupd_poll_interval=1)

    def __onFastLaneHandler(self, message):
        data = json.loads(message.body)
        publishingData = { data["guid"]: data["count"]  }
        self.__fastLaneWriter.pub(TOPIC["PUBLISH"], json.dumps(publishingData), onFinish)
        return True
                
    def __onSlowLaneHandler(self, message):
        data = json.loads(message.body)
        self.__slowLaneCache[data["guid"]] = data["count"]
        if (len(self.__slowLaneCache) >= self.__slowLaneCacheLimit):
            self.__publishSlowLaneCache()
        return True
        
    def __onPublishHandler(self, message):
        message.enable_async()
        # change method of output here
        print(message.body)
        message.finish()
    
    def __tick(self):
        if (len(self.__slowLaneCache) > 0): 
            self.__publishSlowLaneCache() # publish slow lane cache

    def __publishSlowLaneCache(self):
        publishingData = self.__slowLaneCache.copy()
        self.__slowLaneCache.clear()
        self.__slowLaneWriter.pub(TOPIC["PUBLISH"], json.dumps(
            publishingData), onFinish)

    def __onRequestHandler(self, message):
        self.pushMessage(message.body)
        return True

    def pushMessage(self, rawData):
        data = json.loads(rawData)
        guid = data["guid"]
        if (guid in self.__videoListCount):
            self.__videoListCount[guid] += 1
        else: 
            self.__videoListCount[guid] = 1
        count = self.__videoListCount[guid]
        publishingData = { "guid": guid, "count": count  }
        topic = TOPIC["FAST_LANE"] if count < self.__playCountToCacheThreshold else TOPIC["SLOW_LANE"]
        self.__requestProducerWriter.pub(topic, json.dumps(publishingData), onFinish)

    def start_running(self):
        print "running"
        print "listening to request at: "
        for s in self.__http_input:
            print(s)
        self.__intervalFunction.start()
        nsq.run()

    def stop_running(self):
        print "stopping" 
        self.__intervalFunction.stop()
