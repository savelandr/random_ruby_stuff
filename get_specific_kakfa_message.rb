require 'jruby/kafka'
require 'optparse'
require_relative 'zookeeper_utils'

OptionParser.new do |parser|
  parser.banner = "Usage: jruby #{$0} <params>"
  parser.on("-z", "--zookeeper ZK_URL", "Zookeeper and chroot for kafka cluster (REQUIRED)") do |z|
    $zookeeper = z
  end
  parser.on("-t", "--topic TOPIC_NAME", "Topic name to fetch message from (REQUIRED)") do |t|
    $topic = t
  end
  parser.on("-p", "--partiton PARTITION_ID", "Partition ID to fetch message from (REQUIRED)") do |p|
    $partition = p.to_i
  end
  parser.on("-o", "--offset OFFSET_ID", "Offset ID of message to fetch (REQUIRED)") do |o|
    $offset = o.to_i
  end
end.parse!
raise ArgumentError, "No zookeeper host specified" unless $zookeeper
raise ArgumentError, "No topic specified" unless $topic
raise ArgumentError, "No partition specified" unless $partition
raise ArgumentError, "No message offset specified" unless $offset

# Figure out broker to connect to
ZookeeperUtils.connect $zookeeper
brokers = ZookeeperUtils.brokers
partitions = ZookeeperUtils.partitions $topic
partition = partitions.find {|p| p[:partition] == $partition.to_s}
broker = brokers.find {|b| b[:id] == partition[:leader].to_s}
raise RuntimeError, "Couldn't find broker that is leader of partition #{$partition}" unless broker

# Connect and get the message
consumer = Java::KafkaConsumer::SimpleConsumer.new(broker[:host], broker[:port],100000, 64 * 1024, "bob-test-in-prod")
begin
  request = Java::KafkaApi::FetchRequestBuilder.new.client_id("Client_#{$topic}_#{rand(1000)}").add_fetch($topic, $partition, $offset, 100000).build
  response = consumer.fetch(request)
  possible_message = response.message_set($topic, $partition).to_list.find {|m| m.offset == $offset}
  raise_not_found = proc {raise RuntimeError, "Message with offset #{$offset} not found on broker"}
  message = possible_message.get_or_else(raise_not_found).message
  size = message.payload_size
  bytes = Java::byte[size].new
  message.payload.get(bytes)
  puts "Message: \n#{bytes.inspect}" #just shows the byte array
ensure
  consumer.close
end
