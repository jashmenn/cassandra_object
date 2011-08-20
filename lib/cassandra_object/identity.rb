require 'cassandra_object/identity/abstract_key_factory'
require 'cassandra_object/identity/key'
require 'cassandra_object/identity/uuid_key_factory'
require 'cassandra_object/identity/natural_key_factory'

module CassandraObject
  # Some docs will be needed here but the gist of this is simple.  Instead of returning a string, Base#key  now returns a key object.
  # There are corresponding key factories which generate them
  module Identity
    extend ActiveSupport::Concern
    module ClassMethods
      # Indicate what kind of key the model will have: uuid or natural
      #
      # @param [:uuid, :natural] the type of key
      # @param the options you want to pass along to the key factory (like :attributes => :name, for a natural key).
      # 
      def key(name_or_factory = :uuid, *options)
        @key_factory = case name_or_factory
        when :uuid
          UUIDKeyFactory.new
        when :natural
          NaturalKeyFactory.new(*options)
        else
          name_or_factory
        end
      end
    
      def next_key(object = nil)
        returning(@key_factory.next_key(object)) do |key|
          raise "Keys may not be nil" if key.nil?
        end
      end
      
      def parse_key(thing)
        # TODO decide what we want to do here, now that we are using real UUIDs
        # for now do nothing
        # thing
        @key_factory.parse(thing)
      end

      def convert_key_to_java(key)
        case key
        when String then parse_key(key).to_java
        else key.to_java
        end
      end

      def convert_keys_to_java(keys)
        keys.collect{|k| convert_key_to_java(k)}
      end

      def stringify_hkey(key)
        case key
        when String then key
        when CassandraObject::Identity::Key then key.to_s
        else key
        end
      end

      def stringify_hkeys(keys)
        keys.collect{|k| stringify_hkey(k)}
      end


    end
    
    module InstanceMethods

      def ==(comparison_object)
        comparison_object.equal?(self) ||
          (comparison_object.instance_of?(self.class) &&
            comparison_object.key == key &&
            !comparison_object.new_record?)
      end

      def eql?(comparison_object)
        self == (comparison_object)
      end

      def hash
        key.to_s.hash
      end
      
      def to_param
        key.to_param
      end
    end
  end
end
