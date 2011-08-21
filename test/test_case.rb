if defined?(ActiveSupport::TestCase)
  module ActiveSupport
    class TestCase
      def self.final_teardowns
        @final_teardowns ||= []
      end
    end
  end
end
class CassandraObjectTestCase < ActiveSupport::TestCase
end
#RunningMan.setup_on ActiveSupport::TestCase
RunningMan.setup_on CassandraObjectTestCase

class CassandraObjectTestCase < ActiveSupport::TestCase
  attr_accessor :column_families
  attr_accessor :ks_name

  def add_ks
    #@ks_name = java.util.UUID.randomUUID.to_s.gsub("-","")
    @ks_name = "cassandra_object_test_case_space"
    self.connection.add_keyspace({:name => @ks_name, :strategy => :local, :replication => 1, 
                                   :column_families => [{:name => "Customers"}, 
                                                        {:name => "Invoices"},
                                                        {:name => "Appointments"},
                                                        {:name => "Payments"},
                                                        {:name => "CustomerRelationships", :type => :super},
                                                        {:name => "InvoiceRelationships", :type => :super}]}) 
    connection.keyspace = @ks_name
  end

  def drop_ks
    connection.drop_keyspace(@ks_name)
  end

  setup_once do
    CassandraObject::Base.establish_connection nil

    begin
      add_ks
    rescue Java::MePrettyprintHectorApiExceptions::HInvalidRequestException => e
      drop_ks
      add_ks
    end

    Customer.connection = self.connection # ew but thats how class_inheritable_accessor works
    Invoice.connection  = self.connection # ew but thats how class_inheritable_accessor works
    Appointment.connection  = self.connection # ew but thats how class_inheritable_accessor works
    Payment.connection  = self.connection # ew but thats how class_inheritable_accessor works
  end

  teardown_once do
    puts "teardown once"
    connection.disconnect
  end

  def teardown
    connection.clear_keyspace!(@ks_name)
  end

  def mock_invoice
    Invoice.create :number=>Time.now.to_i*(rand(5)), :total=>Time.now.to_f
  end

  def connection
    CassandraObject::Base.connection
  end
  
  def assert_ordered(expected_object_order, actual_order, to_s_before_comparing = true)
    # don't use map! so we don't go changing user's arguments
    if to_s_before_comparing
      expected_object_order = expected_object_order.map(&:to_s) 
      actual_order = actual_order.map(&:to_s)
    end
    
    assert_equal Set.new(expected_object_order), Set.new(actual_order), "Collections weren't equal"
    actual_indexes = actual_order.map do |e|
      expected_object_order.index(e)
    end
    assert_equal expected_object_order, actual_order, "Collection was ordered incorrectly: #{actual_indexes.inspect}"
  end

  # e.g.
  # setup do
  #   self.column_families = [{:name => "Customers"}]
  #   establish_connection
  # end
  # teardown do
  #   break_connection
  # end
  def establish_connection
    CassandraObject::Base.establish_connection nil
    @ks_name = java.util.UUID.randomUUID.to_s.gsub("-","")
    self.connection.add_keyspace({:name => @ks_name, :strategy => :local, 
                                   :replication => 1, :column_families => self.column_families}) 
    connection.keyspace = @ks_name
    @ks_name
  end

  def break_connection
    connection.drop_keyspace(@ks_name)
    connection.disconnect
  end
end
