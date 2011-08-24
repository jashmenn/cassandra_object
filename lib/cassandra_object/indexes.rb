module CassandraObject
  module Indexes
    extend ActiveSupport::Concern
    
    included do
      class_inheritable_accessor :indexes
    end
    
    class UniqueIndex
      def initialize(attribute_name, model_class, options)
        @attribute_name = attribute_name
        @model_class    = model_class
      end

      def self.serialization_options
        {:n_serializer => :string, :v_serializer => :string, :s_serializer => :string}
      end
      
      def find(attribute_value)
        opts = self.class.serialization_options
        # first find the key value
        key = @model_class.connection.get_column(column_family, attribute_value.to_s, 'key', opts)
        # then pass to get
        if key
          model = @model_class.get(key.to_s)
          if model 
            # get worked
            return model
          else
            # bad index delete it
            @model_class.connection.delete_rows(column_family, [attribute_value.to_s])
            nil
          end
        else
          # bad index, delete it
          @model_class.connection.delete_rows(column_family, [attribute_value.to_s])
          nil
        end
      end
      
      def write(record)
        opts = self.class.serialization_options
        # pp [:uniq_write, column_family, record.send(@attribute_name).to_s, {'key'=>record.key.to_s}]
        @model_class.connection.put_row(column_family, record.send(@attribute_name).to_s, {'key'=>record.key.to_s}, opts)
      end
      
      def remove(record)
        @model_class.connection.delete_rows(column_family, [record.send(@attribute_name).to_s])
      end
      
      def column_family
        @model_class.column_family + "By" + @attribute_name.to_s.camelize 
      end
      
      def column_family_configuration
        {:Name=>column_family, :CompareWith=>"UTF8Type"}
      end
    end
    
    class Index
      def initialize(attribute_name, model_class, options)
        @attribute_name = attribute_name
        @model_class    = model_class
        @reversed       = options[:reversed]
      end

      def self.serialization_options
         # {:s_serializer => :string, :n_serializer => :uuid, :v_serializer => :string}
        {}
      end
      
      def find(attribute_value, options = {})
        cursor = CassandraObject::Cursor.new(@model_class, column_family, attribute_value.to_s, @attribute_name.to_s, 
                                             :start_after=>options[:start_after], :reversed=>@reversed)
        cursor.validator do |object|
          object.send(@attribute_name) == attribute_value
        end
        cursor.find(options[:limit] || 100)
      end
      
      def write(record)
        opts = self.class.serialization_options
        k = new_key
        # pp "writing for #{record} %s %s %s %s %s" % [column_family, record.send(@attribute_name).to_s, @attribute_name.to_s, k.uuid.to_s, record.key.to_s]
        # pp [:write_index, column_family, record.send(@attribute_name).to_s, {@attribute_name.to_s=>{new_key=>record.key.to_s}}, k, k.uuid]
        @model_class.connection.put_row(column_family, record.send(@attribute_name).to_s, {@attribute_name.to_s=>{k.uuid=>record.key.to_s}}, opts)

        # if we are changing a value that was previously indexed we need to delete the old index
        # this is why you don't put these junk uuid's before the record key
        # the record key should be the column name and a single bytes should be the value
        # this would allow us to get a specific column and then delete it
        if record.changes.has_key?(@attribute_name.to_s) && !record.changes[@attribute_name.to_s].first.nil?
          # TODO right here we need to delete the instance of this record being in the index
          #pp record.changes[@attribute_name.to_s]
          #old_key = record.changes[@attribute_name.to_s].first
          # index_results = @model_class.connection.get_sub_range(column_family, old_key, old_key, @attribute_name.to_s, opts)
          # pp [:change_the_key, index_results]

          # index_results = index_results[@key]
          # @model_class.connection.put_row(column_family, record.send(@attribute_name).to_s, {@attribute_name.to_s=>{k.uuid=>record.key.to_s}}, opts)
        end
      end
      
      def remove(record)
        # pp "calling remove #{record}"
      end
      
      def column_family
        @model_class.column_family + "By" + @attribute_name.to_s.camelize 
      end
      
      def new_key
        CassandraObject::Identity::TimeUUIDKeyFactory::UUID.new
      end
      
      def column_family_configuration
        {:Name=>column_family, :CompareWith=>"UTF8Type", :ColumnType=>"Super", :CompareSubcolumnsWith=>"TimeUUIDType"}
      end
      
    end
    
    module ClassMethods
      def column_family_configuration
        if indexes
          super + indexes.values.map(&:column_family_configuration)
        else
          super
        end
      end
      
      def index(attribute_name, options = {})
        self.indexes ||= {}.with_indifferent_access
        if options.delete(:unique)
          self.indexes[attribute_name] = UniqueIndex.new(attribute_name, self, options)
          class_eval <<-eom
            def self.find_by_#{attribute_name}(value)
              indexes[:#{attribute_name}].find(value)
            end
            
            after_save do |record|
              self.indexes[:#{attribute_name}].write(record)
              true
            end
              
            after_destroy do |record|
              record.class.indexes[:#{attribute_name}].remove(record)
              true
            end
          eom
        else
          self.indexes[attribute_name] = Index.new(attribute_name, self, options)
          class_eval <<-eom
            def self.find_all_by_#{attribute_name}(value, options = {})
              self.indexes[:#{attribute_name}].find(value, options)
            end
            
            after_save do |record|
              record.class.indexes[:#{attribute_name}].write(record)
              true
            end
              
            after_destroy do |record|
              record.class.indexes[:#{attribute_name}].remove(record)
              true
            end
          eom
        end
      end
    end
  end
end
