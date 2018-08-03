import nsq
import tornado.ioloop
import datetime
import json
import functools
import threading
import requests
import os
from .repeatedTimer import RepeatedTimer

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

# placeholder for on finish data
def onFinish(conn, data):
    pass

class NsqProcessor(object):
    # This will store guid along with its playcount,
    # Whenever a property from here change, it will pass forward
    # to publish accordingly
    __videoListCount = {}
    # This is a cache for slow lane to buffer message up
    # allowing lower priorty guid to bundle before publish
    # (reducing the amount of response to clients)
    __slowLaneCache = {}
    # Initialize processor
    def __init__(self, requestProducerAddrList,
                    fastLaneAddrList, slowLaneAddrList, 
                    requestConsumerAddrList, http_input,
                    nsqlookupdList=[],
                    slowLaneCacheLimit=20,
                    playCountToCacheThreshold=100,
                    timeDifferenceLimit=60,
                    outputfiledir=""):
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
        self.__outputfiledir=outputfiledir
        if os.path.exists(self.__outputfiledir):
            os.remove(self.__outputfiledir)

    # initialize nsqlookupd address with topics
    def __initializeNsqlookupd(self):
        for nsqlookupd in self.nsqlookupdList:
            for topic in TOPIC:
                requests.post(
                    "{nsqlookupdAddr}/topic/create?topic={topic}".format(nsqlookupdAddr=nsqlookupd, topic=topic))

    # initialize nsqd writer
    # although every nsqd can search through any nodes for a specific topic
    # This processor establish a rule of write locally but read globally
    def __inititalizeNsqdWriters(self):
        # write locally since topic will be polled to nsqlookupd
        self.__requestProducerWriter = nsq.Writer(self.requestProducerAddrList)
        self.__fastLaneWriter = nsq.Writer(self.fastLaneAddrList)
        self.__slowLaneWriter = nsq.Writer(self.slowLaneAddrList)

    # initialize nsq reader
    # each cluster of nsqd will handle one of 4 task
    # before propagate data or output data
    def __initializeNsqdReader(self):
        nsq.Reader(message_handler=self.__onFastLaneHandler,
                        nsqd_tcp_addresses=self.fastLaneAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["FAST_LANE"], channel="processor",
                        max_in_flight=self.__slowLaneCacheLimit,
                        lookupd_poll_interval=1)
        nsq.Reader(message_handler=self.__onSlowLaneHandler,
                        nsqd_tcp_addresses=self.slowLaneAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["SLOW_LANE"], channel="processor",
                        max_in_flight=self.__slowLaneCacheLimit,
                        lookupd_poll_interval=1)
        nsq.Reader(message_handler=self.__onPublishHandler,
                        nsqd_tcp_addresses=self.requestConsumerAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["PUBLISH"], channel="processor",
                        max_in_flight=self.__slowLaneCacheLimit,
                        lookupd_poll_interval=1)
        nsq.Reader(message_handler=self.__onRequestHandler,
                        nsqd_tcp_addresses=self.requestProducerAddrList,
                        lookupd_http_addresses=self.nsqlookupdList,
                        topic=TOPIC["REQUEST"], channel="processor",
                        lookupd_poll_interval=1)

    # when a message pass through here
    # it will immediate convert them into a appropriate format
    # before sending it to publish node
    def __onFastLaneHandler(self, message):
        message.enable_async()
        data = json.loads(message.body)
        message.finish()
        publishingData = { data["guid"]: data["count"]  }
        self.__fastLaneWriter.pub(TOPIC["PUBLISH"], json.dumps(publishingData), onFinish)
        return True

    # when a message pass through here
    # it will immediate be cached in a dictionary
    # whenever the cache reach the length of 20 id
    # it will perform send all 20 of them to publish node              
    def __onSlowLaneHandler(self, message):
        message.enable_async()
        data = json.loads(message.body)
        message.finish()
        self.__slowLaneCache[data["guid"]] = data["count"]
        if (len(self.__slowLaneCache) >= self.__slowLaneCacheLimit):
            self.__publishSlowLaneCache()
        
        
    # when a message pass through here
    # it will be considered as published
    def __onPublishHandler(self, message):
        message.enable_async()
        # change method of output here
        print(message.body)
        with open(self.__outputfiledir, "a+") as outfile:
            outfile.write(message.body + "\n")
        message.finish()
    
    # this will trigger after every timeDifference interval
    # it will check the cache and publish all data in it
    # This will ensure published message will always updated
    # at most an interval away from its actual value
    def __tick(self):
        if (len(self.__slowLaneCache) > 0): 
            self.__publishSlowLaneCache() # publish slow lane cache

    # helper function to publish message from cache
    def __publishSlowLaneCache(self):
        publishingData = self.__slowLaneCache.copy()
        self.__slowLaneCache.clear()
        self.__slowLaneWriter.pub(TOPIC["PUBLISH"], json.dumps(
            publishingData), onFinish)

    # message will be received here
    def __onRequestHandler(self, message):
        message.enable_async()
        self.pushMessage(message.body)
        message.finish()

    # helper function to help propagate message to appropriate lane
    def pushMessage(self, rawData):
        try:
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
        except Exception as e:
            pass

    # start the processor
    def start_running(self):
        print(json.dumps({ "address": self.__http_input }))
        print(self.__outputfiledir)
        self.__intervalFunction.start()
        nsq.run()

    # stop the processor
    def stop_running(self):
        self.__intervalFunction.stop()
