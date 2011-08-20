if defined?(JRUBY_VERSION)
  module TypeRegistration
  include_package 'me.prettyprint.cassandra.serializers'
  S = Java::MePrettyprintCassandraSerializers;
  CassandraObject::Base.register_attribute_type(:integer, Integer, CassandraObject::JavaIntegerType, S::IntegerSerializer.get)
  CassandraObject::Base.register_attribute_type(:float, Float, CassandraObject::ToJavaType, S::FloatSerializer.get)
  CassandraObject::Base.register_attribute_type(:long, Fixnum, CassandraObject::JavaLongType, S::LongSerializer.get)
  CassandraObject::Base.register_attribute_type(:string, String, CassandraObject::ToJavaType, S::StringSerializer.get)
  CassandraObject::Base.register_attribute_type(:boolean, Object, CassandraObject::ToJavaType, S::BooleanSerializer.get)
  CassandraObject::Base.register_attribute_type(:byte_buffer, Object, CassandraObject::ToJavaType, S::ByteBufferSerializer.get)
  CassandraObject::Base.register_attribute_type(:byte_array, Object, CassandraObject::ToJavaType, S::BytesArraySerializer.get)
  CassandraObject::Base.register_attribute_type(:date, Date, CassandraObject::JavaDateType, S::DateSerializer.get)
  #CassandraObject::Base.register_attribute_type(:time, Time, CassandraObject::ToJavaType, S::DateSerializer.get)
  CassandraObject::Base.register_attribute_type(:time, Time, CassandraObject::TimeType, S::StringSerializer.get)
  CassandraObject::Base.register_attribute_type(:time_with_zone, ActiveSupport::TimeWithZone, CassandraObject::TimeWithZoneType, S::StringSerializer.get)
  CassandraObject::Base.register_attribute_type(:object, Object, CassandraObject::ToJavaType, S::ObjectSerializer.get)
  CassandraObject::Base.register_attribute_type(:infer, Object, CassandraObject::ToJavaType, S::TypeInferringSerializer.get)
  CassandraObject::Base.register_attribute_type(:uuid, Object, CassandraObject::ToJavaType, S::UUIDSerializer.get)
  CassandraObject::Base.register_attribute_type(:hash, Hash, CassandraObject::HashType, S::StringSerializer.get) # TODO - json? really?
  end
else
CassandraObject::Base.register_attribute_type(:integer, Integer, CassandraObject::IntegerType)
CassandraObject::Base.register_attribute_type(:float, Float, CassandraObject::FloatType)
CassandraObject::Base.register_attribute_type(:date, Date, CassandraObject::DateType)
CassandraObject::Base.register_attribute_type(:time, Time, CassandraObject::TimeType)
CassandraObject::Base.register_attribute_type(:time_with_zone, ActiveSupport::TimeWithZone, CassandraObject::TimeWithZoneType)
CassandraObject::Base.register_attribute_type(:string, String, CassandraObject::StringType)
CassandraObject::Base.register_attribute_type(:hash, Hash, CassandraObject::HashType)
end
