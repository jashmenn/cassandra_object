module CassandraObject
  class Cursor
    def initialize(target_class, column_family, key, super_column, options={})
      @target_class  = target_class
      @column_family = column_family
      @key           = key.to_s
      @super_column  = super_column
      @options       = options
      @validators    = []
    end
    
    # TODO right now this cursor only works on supercolumn families, make work for standard
    def find(number_to_find, options = {})
      limit       = number_to_find
      objects     = CassandraObject::Collection.new
      out_of_keys = false
      options = {:n_serializer => :uuid, :v_serializer => :string, :s_serializer => :string }.merge(options)
      serializer_options = options.pluck(:s_serializer, :n_serializer, :v_serializer)

      if start_with = @options[:start_after]
        limit += 1
      else
        start_with = nil
      end
      
      while objects.size < number_to_find && !out_of_keys
        o = {:count=>limit, :start=>start_with, :reversed=>@options[:reversed]}.merge(options)
        index_results = connection.get_sub_range(@column_family, @key, @key, @super_column, o)
        index_results[@key] ||= {} # hmmm
        index_results = index_results[@key]

        out_of_keys  = index_results.size < limit

        if !start_with.blank?
          index_results.delete(start_with)
        end

        keys = index_results.keys
        values = index_results.values
        
        missing_keys = []

        results = values.empty? ? {} : @target_class.multi_get(values)
        results.each do |(key, result)|
          if result.nil?
            missing_keys << key
          end
        end
    
        unless missing_keys.empty?
          @target_class.multi_get(missing_keys, :quorum=>true).each do |(key, result)|
            index_key = index_results.index(key)
            if result.nil?
              remove(index_key, serializer_options)
              results.delete(key)
            else
              results[key] = result
            end
          end
        end

        results.values.each do |o|
          if @validators.all? {|v| v.call(o) }
            objects << o
          else
            remove(index_results.index(o.key.to_s), options)
          end
        end
        
        start_with = objects.last_column_name = keys.last
        limit = (number_to_find - results.size) + 1
        
      end
      
      return objects
    end
    
    def connection
      @target_class.connection
    end
    
    def remove(index_key, options = {})
      options = options.merge({:v_serializer => options[:n_serializer]}) # TODO - not sure why
      connection.delete_super_columns(@column_family, {@key => {@super_column => [index_key]}}, options)
    end
    
    def validator(&validator)
      @validators << validator
    end
  end
end
