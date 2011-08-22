
module CassandraObject
  module Identity
    # Key factories need to support 3 operations
    class TimeUUIDKeyFactory < UUIDKeyFactory
      class UUID < UUIDKeyFactory::UUID
        include Key

        def initialize(uuid=nil)
          @uuid = case uuid
                  when String then raise "can't create a time uuid from a string"
                  when Fixnum then Java::MePrettyprintCassandraUtils::TimeUUIDUtils.getTimeUUID(uuid)
                  else Java::MePrettyprintCassandraUtils::TimeUUIDUtils.getUniqueTimeUUIDinMillis()
                  end
          @_uuidstr = @uuid.toString # for pp debug purposes only
        end

      end
      
      # Next key takes an object and returns the key object it should use.
      # object will be ignored with synthetic keys but could be useful with natural ones
      def next_key(object); UUID.new; end
      
      # Parse should create a new key object from the 'to_param' format
      def parse(string); UUID.new(string); end
      
      # create should create a new key object from the cassandra format.
      def create(string); UUID.new(string); end
    end
  end
end

