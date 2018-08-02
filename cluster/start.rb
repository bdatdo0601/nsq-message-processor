require "nsq-cluster"
require "json"

# Start a cluster of 3 nsqd's and 2 nsqlookupd's.
# This will block execution until all components are fully up and running.
@cluster = NsqCluster.new(nsqd_count: 4, nsqlookupd_count: 2)

# p "nsqlookupds"
# cluster.nsqlookupd.each_with_index do |nslookupdElement, index|
#     p "#{index}"
#     p "http://#{nsqlookupdElement.host}:#{nsqlookupdElement.http_port}"
#     p "tcp://#{nsqlookupdElement.host}:#{nsqlookupdElement.tcp_port}"
# end

# p "nsqds"
# cluster.nsqd.each_with_index do |nsqdElement, index|
#     p "#{index}"
#     p "http://#{nsqdElement.host}:#{nsqdElement.tcp_port}"
# end

at_exit do 
    # Tear down the whole cluster.
    @cluster.destroy()
end

# exit
while cmd = STDIN.gets
    # remove whitespaces:
  cmd.chop!
  # if command is "exit", terminate:
  if cmd == "exit"
    print "exiting"
    break
  else
    # print current status
    if !@cluster.nil?
        print "still running\n"
        portsData = {}
        portsData[:nsqd] = @cluster.nsqd.map { |nsqdInstance| "#{nsqdInstance.host}:#{nsqdInstance.tcp_port}"}
        portsData[:nsqd_http] = @cluster.nsqd.map { |nsqdInstance| "#{nsqdInstance.host}:#{nsqdInstance.http_port}" }
        portsData[:nsqlookupd] = @cluster.nsqlookupd.map { |nsqlookupdInstance| "#{nsqlookupdInstance.host}:#{nsqlookupdInstance.http_port}" }
        portsData.each do |key, value|
            p "#{key}:#{value}"
        end
    else
        print "cluster not found"
        break
    end
    # and append [end] so that master knows it's the last line:
    print "[end]\n"
    # flush stdout to avoid buffering issues:
    STDOUT.flush
  end
end