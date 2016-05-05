require 'aws-sdk'

def check_args
  if ARGV.length == 2
    serial_number = ARGV[0]
    if serial_number.include? "/" #full serial number
      $serial_number = serial_number
    else #just user name
      $serial_number = "arn:aws:iam::1234567890:mfa/#{serial_number}"
    end
    $token_code = ARGV[1]
  else
    puts "Syntax: ruby #{$0} serial_num_or_name token_code"
    puts "Must have 'original' profile defined in .aws/credentials with your long term access key"
    puts "E.g."
    puts "ruby #{$0} arn:aws:iam::1234567890:mfa/my_user_name 123456"
    puts "or"
    puts "ruby #{$0} my_user_name 123456"
    exit
  end
end

def get_temp_credentials
  sts = Aws::STS::Client.new region: "us-east-1", profile: "original"
  $sess_token = sts.get_session_token serial_number: $serial_number, token_code: $token_code
end

def print_credentials
  raise RuntimeError, "Failed to get token" unless $sess_token
  puts "[default]"
  puts "aws_access_key_id = #{$sess_token.credentials.access_key_id}"
  puts "aws_secret_access_key = #{$sess_token.credentials.secret_access_key}"
  puts "aws_session_token = #{$sess_token.credentials.session_token}"
end

check_args
get_temp_credentials
print_credentials
