module CassandraObject
  module Persistence
    module Hector
      extend ActiveSupport::Concern
      included do
      end

      module ClassMethods

        def get(key, options = {})
          # multi_get([key], options).values.first
        end

        def multi_get(keys, options = {})
          # options = {:consistency => self.read_consistency, :limit => 100}.merge(options)
          # unless valid_read_consistency_level?(options[:consistency])
          #   raise ArgumentError, "Invalid read consistency level: '#{options[:consistency]}'. Valid options are [:quorum, :one]"
          # end

          # attribute_results = connection.multi_get(column_family, keys.map(&:to_s), :count=>options[:limit], :consistency=>consistency_for_thrift(options[:consistency]))

          # attribute_results.inject(ActiveSupport::OrderedHash.new) do |memo, (key, attributes)|
          #   if attributes.empty?
          #     memo[key] = nil
          #   else
          #     memo[parse_key(key)] = instantiate(key, attributes)
          #   end
          #   memo
          # end
        end

        def remove(key)
          # connection.remove(column_family, key.to_s, :consistency => write_consistency_for_thrift)
        end

        def all(keyrange = ''..'', options = {})
          # results = connection.get_range(column_family, :start => keyrange.first, :finish => keyrange.last, :count=>(options[:limit] || 100))
          # keys = results.map(&:key)
          # keys.map {|key| get(key) }
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
            pp [:write, column_family, key.to_s, attributes, schema_version,
               encode_columns_hash(attributes, schema_version)]
            # @opts = {:n_serializer => :string, :v_serializer => :string, :s_serializer => :string}
            # @client.put_row(@cf, "row-key", {"k" => "v"})
            # there are two types of serialization going on here 1) converters from the ruby side 2) serialization in the hector side
            # maybe we need to add a serializer to the attributes, probably a good idea
            # also we need to figure out supercolumns

            # connection.put_row()
            # connection.insert(column_family, key.to_s, encode_columns_hash(attributes, schema_version), :consistency => write_consistency_for_thrift)
          end
        end

        def instantiate(key, attributes)
          # remove any attributes we don't know about. we would do this earlier, but we want to make such
          #  attributes available to migrations
          # attributes.delete_if{|k,_| !model_attributes.keys.include?(k)}
          # returning allocate do |object|
          #   object.instance_variable_set("@schema_version", attributes.delete('schema_version'))
          #   object.instance_variable_set("@key", parse_key(key))
          #   object.instance_variable_set("@attributes", decode_columns_hash(attributes).with_indifferent_access)
          # end
        end

        def encode_columns_hash(attributes, schema_version)
          # attributes.inject(Hash.new) do |memo, (column_name, value)|
          #   memo[column_name.to_s] = model_attributes[column_name].converter.encode(value)
          #   memo
          # end.merge({"schema_version" => schema_version.to_s})
        end

        def decode_columns_hash(attributes)
          # attributes.inject(Hash.new) do |memo, (column_name, value)|
          #   memo[column_name.to_s] = model_attributes[column_name].converter.decode(value)
          #   memo
          # end
        end
        
        def column_family_configuration
          # [{:Name=>column_family, :CompareWith=>"UTF8Type"}]
        end

        protected
      end
      
    end
  end
end
  
