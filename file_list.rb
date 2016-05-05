require 'delegate'

# A helper for dealing with finding new files, waiting for files to be written, etc.
class FileList < Delegator
  attr_reader :directory

  def self.get_files(directory)
    self.new directory
  end

  def initialize(directory)
    @directory = directory
    refresh_files
  end

  def refresh_files
    @files = Dir[@directory + "/**/*"].find_all {|f| File.file? f}
  end

  def __getobj__
    @files
  end

  def new_files
    old_files = @files
    refresh_files - old_files
  end

  def wait_for_new_files(wait_secs=30)
    start = Time.now
    files = []
    while (Time.now < start + wait_secs) && files.empty?
      files = self.new_files
      sleep 2
    end
    files
  end
end
