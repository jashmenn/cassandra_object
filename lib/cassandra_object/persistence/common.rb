module CassandraObject
  module Persistence
    module Common
      extend ActiveSupport::Concern

      module InstanceMethods
        def save
          run_callbacks :save do
            create_or_update
          end
        end
        
        def create_or_update
          if new_record?
            create
          else
            update
          end
          true
        end
        
        def create
          run_callbacks :create do
            @key ||= self.class.next_key(self)
            _write
            @new_record = false
            true
          end
        end
        
        def update
          run_callbacks :update do
            _write
          end
        end
        
        def _write
          changed_attributes = changed.inject({}) { |h, n| h[n] = read_attribute(n); h }
          self.class.write(key, changed_attributes, schema_version)
        end

        def new_record?
          @new_record || false
        end

        def destroy
          run_callbacks :destroy do 
            self.class.remove(key)
          end
        end
        
        def reload
          self.class.get(self.key)
        end
        
      end
    end
  end
end
