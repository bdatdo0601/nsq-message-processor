require "nsq-cluster"
require "optparse"
require "json"

options = {}
OptionParser.new do |opts|
    opts.banner = "Usage: example.rb [options]"

    opts.on("-nNUM", "--nsqd_count=NUM", "nsqd count") do |n|
        options[:nsqd] = n
    end

    opts.on("-lNUM", "--nsqlookupd_count=NUM", "nsqlookupd count") do |l|
        options[:nsqlookupd] = l
    end

end.parse!

if (!options.key?(:nsqd) || !options.key?(:nsqlookupd))
    options[:nsqd] = 4
    options[:nsqlookupd] = 2
end


# This will block execution until all components are fully up and running.
@cluster = NsqCluster.new(nsqd_count: options[:nsqd].to_i, nsqlookupd_count: options[:nsqlookupd].to_i)

at_exit do 
    print "[end]\n"
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
    print "[end]\n"
    break
  else
    # print cluster information
    if !@cluster.nil?
        portsData = {}
        portsData[:nsqd] = @cluster.nsqd.map { |nsqdInstance| "#{nsqdInstance.host}:#{nsqdInstance.tcp_port}"}
        portsData[:nsqd_http] = @cluster.nsqd.map { |nsqdInstance| "#{nsqdInstance.host}:#{nsqdInstance.http_port}" }
        portsData[:nsqlookupd] = @cluster.nsqlookupd.map { |nsqlookupdInstance| "http://#{nsqlookupdInstance.host}:#{nsqlookupdInstance.http_port}" }
        puts portsData.to_json
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