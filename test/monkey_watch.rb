class Class
  @@method_history = {}

  def self.method_history
    return @@method_history
  end

  def method_added(method_name)
    puts "#{method_name} added to #{self}"
    @@method_history[self] ||= {}
    @@method_history[self][method_name] = caller
  end

  def method_defined_in(method_name)
    return @@method_history[self][method_name]
  end
end
