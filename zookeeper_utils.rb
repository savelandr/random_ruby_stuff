if $CLASSPATH.find {|f| f.include? "zookeeper"}.nil?
  # zookeeper libs aren't already loaded, so load them
  require 'jruby/zookeeper'
end

begin #Try to shut Log4j up if gem is installed
  require 'jruby/log4j_shutter_upper'
rescue
end

require 'multi_json'

module ZookeeperUtils

  def self.connect(zk_url)
    if @zk && self.connected?
      self.close
    end
    @zk = Java::OrgApacheZookeeper::ZooKeeper.new(zk_url, 3000, SomeWatcher.new)
    @connected = true
  end

  def self.close
    @zk.close
    @connected = false
  end

  def self.connected?
    !!@connected
  end

  def self.zk
    @zk
  end

  def self.exists?(path)
    self.check_connection
    !!@zk.exists(path, false)
  end

  def self.partitions(topic_name)
    self.check_connection
    partitions = []
    stat = Java::OrgApacheZookeeperData::Stat.new
    if self.exists?("/brokers/topics/#{topic_name}/partitions")
      ids = @zk.get_children("/brokers/topics/#{topic_name}/partitions", false)
      ids.each do |partition|
        data = @zk.get_data("/brokers/topics/#{topic_name}/partitions/#{partition}/state", false, stat).to_s
        data = MultiJson.load data
        partitions << {partition: partition, leader: data["leader"], isr: data["isr"]}
      end #each
    end #if
    return partitions
  end

  def self.brokers
    self.check_connection
    brokers = []
    stat = Java::OrgApacheZookeeperData::Stat.new
    if self.exists?("/brokers/ids")
      ids = @zk.get_children("/brokers/ids", false)
      ids.each do |broker_id|
        data = @zk.get_data("/brokers/ids/#{broker_id}", false, stat).to_s
        data = MultiJson.load data
        brokers << {host: data['host'], port: data['port'], id: broker_id}
      end #each
    end #if
    return brokers
  end

  def self.broker_url
    brokers.map {|b| "#{b[:host]}:#{b[:port]}"}.join(",")
  end

  private
  def self.check_connection
    raise RuntimeError, "Not connected to a Zookeeper" unless self.connected?
  end

  class SomeWatcher
    include Java::OrgApacheZookeeper::Watcher

    def process(event)
      puts "Watcher: #{event.to_s}" if $DEBUG
    end
  end
end
