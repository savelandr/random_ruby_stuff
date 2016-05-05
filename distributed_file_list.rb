require_relative 'file_list'
require 'jruby/cdh/hdfs'

# A helper for dealing with finding new distributed files, waiting for distributed files to be written, etc
class DistributedFileList < FileList

  def initialize(directory)
    @directory = directory
    @path = Java::OrgApacheHadoopFs::Path.new @directory
    conf = Java::OrgApacheHadoopConf::Configuration.new
    @file_system = @path.get_file_system conf
    refresh_files
  end

  def refresh_files
    @files = []
    begin
      file_locators = @file_system.list_files(@path, true)
      while file_locators.has_next?
        @files << file_locators.next.path.to_s
      end
      @files.delete_if {|file| file.include?(".processing") || file.include?(".creating")} #The DF process includes temporary processing files we want to ignore
    rescue Java::JavaIo::FileNotFoundException
    end
    self
  end

end
