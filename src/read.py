import nsq

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

nsq.Reader(message_handler=process_message,
               nsqd_tcp_addresses=['127.0.0.1:4150'],
               topic='test', channel='async', max_in_flight=9)
nsq.Reader(message_handler=process_message,
            nsqd_tcp_addresses=['127.0.0.1:4150'],
            topic='test2', channel='async', max_in_flight=9)

nsq.run()
