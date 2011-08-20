require 'cassandra_object/associations/one_to_many'
require 'cassandra_object/associations/one_to_one'

module CassandraObject
  module Associations
    extend ActiveSupport::Concern
    
    included do
      class_inheritable_hash :associations
    end

    module ClassMethods
      def column_family_configuration
        super << {:Name=>"#{name}Relationships", :CompareWith=>"UTF8Type", :CompareSubcolumnsWith=>"TimeUUIDType", :ColumnType=>"Super"}
      end
      
      def association(association_name, options= {})
        if options[:unique]
          write_inheritable_hash(:associations, {association_name => OneToOneAssociation.new(association_name, self, options)})
        else
          write_inheritable_hash(:associations, {association_name => OneToManyAssociation.new(association_name, self, options)})
        end
      end
      
      def remove(key)
        # todo should be able to check from the keyspace description
        # TODO shouldn't go straight to the connection, should it?
        #begin
        #  connection.remove("#{name}Relationships", key.to_s)
        #rescue AccessError => e # todo
        #  raise e unless e.message =~ /Invalid column family/
        #end
        super
      end
    end
  end
end
