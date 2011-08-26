module CassandraObject
  class OneToOneAssociation
    def initialize(association_name, owner_class, options)
      @association_name  = association_name.to_s
      @owner_class       = owner_class
      @target_class_name = options[:class_name] || association_name.to_s.camelize 
      @options           = options

      define_methods!
    end

    def use_intermediate_key?
      @options[:intermediate_key]
    end
    
    def define_methods!
      @owner_class.class_eval <<-eos
        def #{@association_name}
          @_#{@association_name} ||= self.class.associations[:#{@association_name}].find(self)
        end
        
        def #{@association_name}=(record)
          @_#{@association_name} = record
          self.class.associations[:#{@association_name}].set(self, record)
        end
      eos
    end
    
    def clear(owner)
      connection.delete_columns(column_family, owner.key.to_s, [@association_name])
    end

    def self.default_association_serializers
      {:s_serializer => :string, :n_serializer => :uuid, :v_serializer => :string}
    end

    def serializers
      @options[:serializers] ||= {}
      self.class.default_association_serializers.merge(@options[:serializers])
    end
    
    def find(owner)
      opts = serializers.merge({:count => 1})
      sc = connection.get_super_rows(column_family, owner.key.to_s, @association_name.to_s, opts)

    # if key = connection.get_super_columns(column_family, owner.key.to_s, @association_name.to_s, :count=>1).values.first
      if key = sc.values.first[@association_name.to_s].values.first.to_s
        target_class.get(key)
      else
        nil
      end
    end  
    
    def set(owner, record, set_inverse = true)
      clear(owner)
      connection.put_row(column_family, owner.key.to_s, {@association_name=>{new_key.uuid => record.key.to_s}})
      if has_inverse? && set_inverse
        inverse.set_inverse(record, owner)
      end
    end
    
    def new_key
      CassandraObject::Identity::UUIDKeyFactory::UUID.new # TODO
    end
    
    def set_inverse(owner, record)
      set(owner, record, false)
    end
    
    def has_inverse?
      @options[:inverse_of]
    end
    
    def inverse
      has_inverse? && target_class.associations[@options[:inverse_of]]
    end

    def column_family
      @owner_class.to_s + "Relationships"
    end
    
    def connection
      @owner_class.connection
    end
    
    def target_class
      @target_class ||= @target_class_name.constantize
    end
    
    def new_proxy(owner)
      # OneToManyAssociationProxy.new(self, owner)
    end
  end
end
