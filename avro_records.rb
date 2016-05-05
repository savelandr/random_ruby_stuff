require 'delegate'
require 'jruby/avro'
require 'multi_json'

# Helper to read avro records out of HDFS.  Note that this puts all records into a collection, so it
# would not be suitable for production sized files.
class AvroRecords < Delegator

  # Get records from a distributed file
  # ==Example:
  #   path = "hdfs://hadoop.somewhere.com:8020/projects/us/expanded/base/2014/06/01/00/"
  #   name = "02f3e6ea-bea3-4576-967c-9cc3cd0d4b8a_2014060100.avro"
  #   file = path + name
  #   AvroRecords.from_dfile file        -> [record1, record2, ...]
  def self.from_dfile(distributed_filename)
    require 'jruby/cdh/hdfs'
    require 'jruby/avro/mapred'
    reader = Java::OrgApacheAvroGeneric::GenericDatumReader.new
    path = Java::OrgApacheHadoopFs::Path.new distributed_filename
    conf = Java::OrgApacheHadoopConf::Configuration.new
    input = Java::OrgApacheAvroMapred::FsInput.new path, conf
    dist_reader = Java::OrgApacheAvroFile::DataFileReader.new input, reader
    return populate_records(self.new, dist_reader)
  end

  # Get records from an S3 file
  # ==Example:
  #   s3_url = "s3://some-bucket/topic/2015/10/1/some_file.avro"
  #   AvroRecords.from_s3_file s3_url        -> [record1, record2, ...]
  def self.from_s3_file(s3_url, region="us-east-1")
    require 'tempfile'
    require 'aws-sdk'
    tempfile = Tempfile.new(["from_s3", ".avro"])
    populate_temp_file(tempfile.path, s3_url, region)
    final_records = from_file(tempfile.path)
    tempfile.close!
    return final_records
  end

  # Get records from an S3 file
  # ==Example:
  #   s3_url = "s3://some-bucket/topic/2015/10/1/some_file.avro"
  #   AvroRecords.from_s3_file s3_url        -> [record1, record2, ...]
  def self.each_from_s3_file(s3_url, region="us-east-1", &block)
    require 'tempfile'
    require 'aws-sdk'
    tempfile = Tempfile.new(["from_s3", ".avro"])
    populate_temp_file(tempfile.path, s3_url, region)
    each_from_file(tempfile.path, &block)
    tempfile.close!
  end

  # Get schema from an S3 file
  # ==Example:
  #   s3_url = "s3://some-bucket/topic/2015/10/1/some_file.avro"
  #   AvroRecords.schema_from_s3_file s3_url        -> {"type"=>"record", "name"=>"UnifiedRecord", ...}
  def self.schema_from_s3_file(s3_url, region="us-east-1")
    require 'tempfile'
    require 'aws-sdk'
    tempfile = Tempfile.new(["from_s3", ".avro"])
    populate_temp_file(tempfile.path, s3_url, region)
    schema = schema_from_file tempfile.path
    tempfile.close!
    return schema
  end

  # Get schema from a local file
  # ==Example:
  #   AvroRecords.schema_from_file "/tmp/file.avro"        -> {"type"=>"record", "name"=>"UnifiedRecord", ...}
  def self.schema_from_file(file_path)
    reader = Java::OrgApacheAvroGeneric::GenericDatumReader.new
    file = Java::JavaIo::File.new file_path
    file_reader = Java::OrgApacheAvroFile::DataFileReader.new file, reader
    schema = file_reader.schema
    file_reader.close
    return MultiJson.load schema.to_s
  end

  # Get records from a local file
  # ==Example:
  #   AvroRecords.from_file "/tmp/file.avro"        -> [record1, record2, ...]
  def self.from_file(file_path)
    reader = Java::OrgApacheAvroGeneric::GenericDatumReader.new
    file = Java::JavaIo::File.new file_path
    file_reader = Java::OrgApacheAvroFile::DataFileReader.new file, reader
    records = populate_records(self.new, file_reader)
    return records
  end

  # Get records from a local file
  # ==Example:
  #   AvroRecords.from_file "/tmp/file.avro"        -> [record1, record2, ...]
  def self.each_from_file(file_path, &block)
    reader = Java::OrgApacheAvroGeneric::GenericDatumReader.new
    file = Java::JavaIo::File.new file_path
    file_reader = Java::OrgApacheAvroFile::DataFileReader.new file, reader
    yield_records(file_reader, &block)
  end

  # Write avro records to a local avro file.
  # The records all have to have the same schema, or a compatible schema, or it
  # is unlikely to work.
  # This also won't work with any AvroRecord objects read from a file as they 
  # have been turned into Hashes already and have no schema/etc.
  def to_file(filename)
    raise RuntimeError, "No records to write" unless @records.length > 0
    raise RuntimeError, "Can't serialize Hashes" if @records.any? {|r| r.is_a? Hash}
    schema = @records.first.schema
    datum = Java::OrgApacheAvroGeneric::GenericDatumWriter.new schema
    writer = Java::OrgApacheAvroFile::DataFileWriter.new datum
    writer.create(schema, Java::JavaIo::File.new(filename))
    @records.each {|r| writer.append r}
    writer.close
  end

  def initialize
    @records=[]
  end

  def __getobj__
    @records
  end

private

  def self.populate_records(records, reader)
    while reader.has_next?
      avro_record = reader.next
      records << MultiJson.load(avro_record.to_s)
    end
    reader.close
    return records
  end

  def self.yield_records(reader)
    while reader.has_next?
      avro_record = reader.next
      yield MultiJson.load(avro_record.to_s)
    end
    reader.close
  end

  def self.populate_temp_file(tempfile, s3_url, region)
    require 'uri'
    s3 = Aws::S3::Client.new region: region, credentials: Aws::SharedCredentials.new(region: region)
    s3_uri = URI.parse s3_url
    bucket = s3_uri.host
    object = s3_uri.path[1..-1] #omit leading slash
    s3.get_object bucket: bucket, key: object, response_target: tempfile
  end
end
