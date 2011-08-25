Gem::Specification.new do |s|
  s.name    = 'hector_object'
  s.version = '0.7.1.pre'
  s.email   = "nate@natemurray.com"
  s.author  = "hector fork by Nate Murray, originally by Michael Koziarski"

  s.description = %q{Gives you most of the familiarity of ActiveRecord, but with the scalability of cassandra.}
  s.summary     = %q{Maps your objects into cassandra.}
  s.homepage    = %q{http://github.com/jashmenn/cassandra_object}
  s.add_dependency('activesupport', '>= 3.0.9')
  s.add_dependency('activemodel',   '>= 3.0.9')
  s.add_dependency('hector')

  s.files = Dir['lib/**/*'] + Dir["vendor/**/*"]
  s.require_path = 'lib'
end
