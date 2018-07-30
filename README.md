# Wistia Optimal Queuing Developer Challenge

## Overview

This project is about synchronizing large amounts of data between different services, something we do daily at Wistia. Imagine a system where messages are placed onto a queue; a processor takes messages off the queue, performs some operations on them, and publishes the results to the client. We're interested in developing a scalable and efficient processor.

## Language Selection

Please write this application in Ruby, Python, or Elixir. If you don't know any of these languages, give us a shout and we'll work with you to find a language that you know and we can evaluate.

## System Description

* Each message in the queue is a JSON string with a GUID video_id attribute, which is used by the client to identify a video.
* Each message in the queue implies a "play" event for the video_id it references.
* The processor is responsible for reading each message off the queue, updating the play count of the respective video, and publishing the result to the client.

## System Constraints

* There are 100,000 messages on the queue combined for 100 videos. Some videos have more plays than other videos. You can decide on the distribution of 100,000 plays across the 100 videos, but ensure there are at least 15 videos with fewer than 100 plays each.
* For videos with fewer than 100 plays, the processor needs to the publish the result to the client as soon as a play is received for one because with such a small sample size, each play is of the utmost "realtime" importance.
* For videos with 100 or more plays, each play is a little less significant, and so their stats do not need to be as "realtime." Rather, the client just needs to have the total play count for these videos up to date for the last 1 minute, i.e. each message that arrives on the queue must manifest as a change in the total play count for the associated video_id within 1 minute of arriving on the queue.
* Each time the processor publishes (i.e. makes a call to) to the client, it can provide updated play counts for at most 20 videos in a single message.
* An updated play count for a video should only be published to the client if the play count for that video has changed since it was last published.
* The goal is to minimize the number of times the processor publishes its results to the client. Imagine that the cost of each request made is high.

## Your Task

Based on the aforementioned constraints, write a script that produces messages onto a queue, and create the processor that consumes from that queue and publishes its results to the client.

## Notes

* We recommend using [NSQ](http://nsq.io/), a simple distributed messaging platform. We use this at Wistia for a variety of data ingestion problems. Client libraries are available for [Ruby](https://github.com/wistia/nsq-ruby), [Python](https://github.com/nsqio/pynsq), and [Elixir](https://github.com/wistia/elixir_nsq).
* We recommend [nsq-cluster](https://github.com/wistia/nsq-cluster) to set up your NSQ cluster.
* Publishing to the client can be as trivial as logging to STDOUT or as complex as a webpage that updates with the total play count of each video. This is totally up to you!
* Please reach out to robby@wistia.com and ryan@wistia.com if you have any questions or would like further clarification.

## Python Users
We recommend using the [pynsq client library](https://github.com/nsqio/pynsq).

You can use the following script to confirm that NSQ is set up correctly on your system.

```python
import nsq
import tornado.ioloop
import time

def pub_message():
    writer.pub('test', time.strftime('%H:%M:%S'), finish_pub)

def finish_pub(conn, data):
    print(data)


buf = []

def process_message(message):
    global buf
    message.enable_async()
    # cache the message for later processing
    buf.append(message)
    if len(buf) >= 3:
        for msg in buf:
            print msg.body
            msg.finish()
        buf = []
    else:
        print 'deferring processing'

r = nsq.Reader(message_handler=process_message,
        nsqd_tcp_addresses=['127.0.0.1:4150'],
        topic='test', channel='async', max_in_flight=9)


writer = nsq.Writer(['127.0.0.1:4150'])
tornado.ioloop.PeriodicCallback(pub_message, 1000).start()
nsq.run()
```

Your output should look something like this
```shell
OK
deferring processing
OK
deferring processing
OK
19:54:09
19:54:10
19:54:11
OK
deferring processing
OK
deferring processing
OK
19:54:12
19:54:13
19:54:14
OK
deferring processing
OK
deferring processing
OK
19:54:15
19:54:16
19:54:17
...
```

Best of luck!
