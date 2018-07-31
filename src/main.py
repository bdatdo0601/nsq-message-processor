import nsq
import tornado.ioloop
import time

def pub_message():
    # write.pub(topic, message, callback)
    writer.pub('test', time.strftime('%H:%M:%S'), finish_pub)


def finish_pub(conn, data):
    print(conn) # connection
    print(data)

writer = nsq.Writer(['127.0.0.1:4150'])
tornado.ioloop.PeriodicCallback(pub_message, 1000).start()
nsq.run()
