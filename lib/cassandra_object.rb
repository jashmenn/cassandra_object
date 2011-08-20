require 'rubygems'
require 'i18n'
require 'active_support'
require 'active_support/version'
require 'active_support/all'
require 'active_model'
require 'time'
require 'date'

module CassandraObject
  class << self
  end
  VERSION = "0.5.0"
end

if defined?(JRUBY_VERSION)
  require 'java'
  # jars_dir = File.dirname(__FILE__) + "/../vendor/jars"
  # $LOAD_PATH << jars_dir
  # Dir.entries(jars_dir).sort.each do |entry|
  #   if entry =~ /.jar$/
  #     puts entry
  #     require entry
  #   end
  # end
end

require 'cassandra_object/helpers'
require 'cassandra_object/base'


