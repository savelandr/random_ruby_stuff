require_relative 'file_list'
require 'aws-sdk'

# A helper for dealing with finding new S3 files, waiting for S3 files to be written, etc
class S3FileList < FileList

  def initialize(bucket, prefix, region="us-east-1")
    @bucket = bucket
    @prefix = prefix
    @region = region
    @credentials = Aws::SharedCredentials.new region: @region
    @s3 = Aws::S3::Client.new region: @region, credentials: @credentials
    refresh_files
  end

  def refresh_files
    @files = []
    more_remaining = true
    marker = nil
    while more_remaining
      objects = @s3.list_objects bucket: @bucket, prefix: @prefix, marker: marker
      objects.contents.each {|o| @files << "s3://#{@bucket}/#{o.key}" unless o.key.match /_\$folder\$$/}
      marker = objects.contents.last.key if objects.is_truncated
      more_remaining = objects.is_truncated
    end
    self
  end

end
