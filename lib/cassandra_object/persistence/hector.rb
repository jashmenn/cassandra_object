require 'hector'
require 'hector/ordered_hash'

module CassandraObject
  module Persistence
    module Hector
      extend ActiveSupport::Concern
      included do
        class_inheritable_writer :write_consistency
        class_inheritable_writer :read_consistency
      end

      VALID_READ_CONSISTENCY_LEVELS = [:one, :quorum, :all]
      VALID_WRITE_CONSISTENCY_LEVELS = VALID_READ_CONSISTENCY_LEVELS + [:zero]


      module ClassMethods

        def get(key, options = {})
          multi_get([key], options).values.first
        end

        def multi_get(keys, options = {})
          # options = {:consistency => self.read_consistency, :limit => 100}.merge(options)
          # unless valid_read_consistency_level?(options[:consistency])
          #   raise ArgumentError, "Invalid read consistency level: '#{options[:consistency]}'. Valid options are [:quorum, :one]"
          # end

          o = reading_persistence_attribute_options.merge(options)
          keystrings = stringify_hkeys(keys)
          attribute_results = connection.get_rows(column_family, keystrings, o)

          # restore order by keys
          ordered_results = returning(::Hector::OrderedHash.new) do |oh|
            keystrings.each { |key| oh[key] = attribute_results[key] }
          end

          instantiate_results(ordered_results)
        end

        def remove(key)
          connection.delete_rows(column_family, [key.to_s])
        end

        def all(keyrange = ''..'', options = {})
          attribute_results = connection.get_range(column_family, keyrange.first, keyrange.last, 
                                                   reading_persistence_attribute_options.merge(options))
          instantiate_results(attribute_results).values
        end

        def first(keyrange = ''..'', options = {})
          # all(keyrange, options.merge(:limit=>1)).first
        end

        def create(attributes)
          returning new(attributes) do |object|
            object.save
          end
        end

        def write(key, attributes, schema_version)
          returning(key) do |key|
            # todo, key shouldn't be cast to a string here
            #pp [:write, column_family, key.to_s, attributes, schema_version, #   encode_columns_hash(attributes, schema_version)]

            #pp [:write,connection]
            #pp connection.keyspace
            #pp self.connection_class
            #pp connection.keyspace.getKeyspaceName

            # @opts = {:n_serializer => :string, :v_serializer => :string, :s_serializer => :string}

            #key = key.to_java # TODO - use the key serializer!
            key = key.to_s # string keys
            ech = encode_columns_hash(attributes, schema_version)
            pao = persistence_attribute_options(attributes, schema_version)
            # pp [:write, column_family, key, ech, pao]

            connection.put_row(column_family, key, ech, pao)
            # connection.insert(column_family, key.to_s, encode_columns_hash(attributes, schema_version), :consistency => write_consistency_for_thrift)
          end
        end

        def instantiate(key, attributes)
          # remove any attributes we don't know about. we would do this earlier, but we want to make such
          #  attributes available to migrations
          # pp [:model_attribute_keys, model_attributes.keys, attributes]
          schema_version = attributes.delete('schema_version').to_i

          attributes.delete_if{|k,_| !model_attributes.keys.include?(k)}
          returning allocate do |object|
            object.instance_variable_set("@schema_version", schema_version)
            object.instance_variable_set("@key", parse_key(key))
            object.instance_variable_set("@attributes", decode_columns_hash(attributes).with_indifferent_access)
          end
        end

        def encode_columns_hash(attributes, schema_version)
          attributes.inject(Hash.new) do |memo, (column_name, value)|
            memo[column_name.to_s] = model_attributes[column_name].converter.encode(value)
            memo
          end.merge({"schema_version" => schema_version.to_s})
        end

        def persistence_attribute_options(attributes, schema_version)
          {}
        end

        def reading_persistence_attribute_options
          {:n_serializer => :string, :v_serializer => :bytes, :s_serializer => :string}
        end

        def decode_columns_hash(attributes)
          # pp [:decode_columns_hash, attributes]
          attributes.inject(Hash.new) do |memo, (column_name, value)|
            # memo[column_name.to_s] = model_attributes[column_name].converter.decode(value)
            # pp [:decode, column_name, value]
            # pp [model_attributes[column_name].converter, model_attributes[column_name].serializer]
            deserialized = model_attributes[column_name].serializer.fromBytes(value)
            converted    = model_attributes[column_name].converter.decode(deserialized)
            memo[column_name.to_s] = converted
            memo
          end
        end

        def consistency_levels(levels)
          if levels.has_key?(:write)
            unless valid_write_consistency_level?(levels[:write])
              raise ArgumentError, "Invalid write consistency level. Valid levels are: #{VALID_WRITE_CONSISTENCY_LEVELS.inspect}. You gave me #{levels[:write].inspect}"
            end
            self.write_consistency = levels[:write]
          end

          if levels.has_key?(:read)
            unless valid_read_consistency_level?(levels[:read])
              raise ArgumentError, "Invalid read consistency level. Valid levels are #{VALID_READ_CONSISTENCY_LEVELS.inspect}. You gave me #{levels[:write].inspect}"
            end
            self.read_consistency = levels[:read]
          end
        end

        def write_consistency
          read_inheritable_attribute(:write_consistency) || :quorum
        end

        def read_consistency
          read_inheritable_attribute(:read_consistency) || :quorum
        end

        protected

        def instantiate_results(results)
          results.inject(ActiveSupport::OrderedHash.new) do |memo, (key, attributes)|
            if attributes.empty?
              memo[key] = nil # could be a garbage key
            else
              memo[parse_key(key)] = instantiate(key, attributes)
            end
            memo
          end
        end

        def valid_read_consistency_level?(level)
          !!VALID_READ_CONSISTENCY_LEVELS.include?(level)
        end

        def valid_write_consistency_level?(level)
          !!VALID_WRITE_CONSISTENCY_LEVELS.include?(level)
        end

        def column_family_configuration
          # [{:Name=>column_family, :CompareWith=>"UTF8Type"}]
        end

        def write_consistency_for_thrift
          consistency_for_thrift(write_consistency)
        end

        def read_consistency_for_thrift
          consistency_for_thrift(read_consistency)
        end

        # TODO
        def consistency_for_thrift(consistency)
          {
            :zero   => nil, #Cassandra::Consistency::ZERO,
            :one    => nil, #Cassandra::Consistency::ONE, 
            :quorum => nil, #Cassandra::Consistency::QUORUM,
            :all    => nil, #Cassandra::Consistency::ALL
          }[consistency]
        end



      end
      
    end
  end
end
  
