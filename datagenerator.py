import random
import json
import asyncio
import argparse
from aiohttp import ClientSession

"""
DATAGENERATOR.PY

This is a script to generate 100000 messages from 100 video_id with 10 low count
video_id. The distribution is random and it is guarantee to have low count video
"""

parser = argparse.ArgumentParser("nsq data generator")
parser.add_argument("address", help="address to send data to", type=str)
argList = parser.parse_args()

VIDEO_AMOUNT = 100
MESSAGE_AMOUNT = 100000

LOW_COUNT_VIDEO_AMOUNT = 10

LOW_COUNT_VIDEO_LIST = random.sample(range(0, VIDEO_AMOUNT - 1), LOW_COUNT_VIDEO_AMOUNT)
URL = "http://{address}/pub?topic=REQUEST".format(address=str(argList.address))

messageList = []
videoCount = {}

for i in range(0, VIDEO_AMOUNT):
    guid = "video_id_{index}".format(index=i)
    videoCount[guid] = 0

for i in range(0, MESSAGE_AMOUNT):
    randGUIDIndex = random.randint(0, VIDEO_AMOUNT - 1)
    guid = "video_id_{index}".format(index=randGUIDIndex)
    while (randGUIDIndex in LOW_COUNT_VIDEO_LIST and videoCount[guid] > 100):
        randGUIDIndex = random.randint(0, VIDEO_AMOUNT - 1)
        guid = "video_id_{index}".format(index=randGUIDIndex)
    data = { "guid": guid }
    messageList.append(json.dumps(data))
    videoCount[guid] +=1

async def fetch(url, data, session):
    async with session.post(url, data=data) as response:
        return await response.read()

async def makeRequests():
    tasks = []
    async with ClientSession() as session:
        for message in messageList:
            task = asyncio.ensure_future(fetch(URL, message, session))
            tasks.append(task)
        print("sending")
        responses = await asyncio.gather(*tasks)
        print("finish sending")

loop = asyncio.get_event_loop()
print("start making requests")
loop.run_until_complete(makeRequests())
print("done")


