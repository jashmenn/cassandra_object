require 'rubygems'
require 'bundler'

if defined?(JRUBY_VERSION)
gem 'hector.rb'
require 'hector'
end

Bundler.setup

require 'cassandra_object'
require 'connection'

require 'test/unit'
require 'active_support/test_case'
require 'shoulda'

require 'fixture_models'
require 'pp'

require 'test_case'
