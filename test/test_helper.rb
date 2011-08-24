$:.unshift(File.dirname(__FILE__))
# require 'monkey_watch'
require 'rubygems'
require 'bundler'

if defined?(JRUBY_VERSION)
require 'java'
gem 'hector.rb'
require 'hector'
end

Bundler.setup

require 'cassandra_object'
require 'connection'

require 'test/unit'
require 'active_support/test_case'
require 'shoulda'
require 'running_man'

require 'fixture_models'
require 'pp'

require 'test_case'

