require 'test_helper'

class HABTMTest < CassandraObjectTestCase
  UU = CassandraObject::Identity::UUIDKeyFactory::UUID
 
  def setup
    super
    @b1 = Bookmark.create :name => "Dwight's Place", :url=>"http://www.schrutefarms.com"
    @b2 = Bookmark.create :name => "The Office",     :url=>"http://www.theoffice.com"
    @beet   = Tag.create :name => "beets"
    @bear   = Tag.create :name => "bears"
    @battle = Tag.create :name => "battlestar-gallactica"
  end

  def teardown
    super
  end

  context "if a tag is added to a post" do
    setup do
      @b1.tags << @beet
      @b1.tags << @bear
      
      @b2.tags << @beet
      @sopts = Bookmark.associations[:tags].serializers
    end

    should "the post should know its tags" do
      assert_set_equal ["beets", "bears"], @b1.tags.all.map(&:name)
    end

    should "the tag should know its bookmarks" do
      assert_set_equal [@b2.name, @b1.name], @beet.bookmarks.all.map(&:name)
    end

    should "be idempotent in adding relationships" do
      assert_set_equal ["beets", "bears"], 
        Bookmark.connection.get_super_row(Bookmark.associations[:tags].column_family, @b1.key.to_s, "tags", @sopts).keys
      @b1.tags << @bear
      assert_set_equal ["beets", "bears"], 
        Bookmark.connection.get_super_row(Bookmark.associations[:tags].column_family, @b1.key.to_s, "tags", @sopts).keys
    end

    should "be able to delete tags" do
      @b1.tags << @battle

      assert_set_equal ["beets", "bears", "battlestar-gallactica"], 
        Bookmark.connection.get_super_row(Bookmark.associations[:tags].column_family, @b1.key.to_s, "tags", @sopts).keys

      @b1.tags.delete(@battle)

      assert_set_equal ["beets", "bears"], 
        Bookmark.connection.get_super_row(Bookmark.associations[:tags].column_family, @b1.key.to_s, "tags", @sopts).keys
    end
  end

  context "if a post is added to a tag" do
    setup do
      @beet.bookmarks << @b1
      @beet.bookmarks << @b2
    end

    should "the post should know its tags" do
      assert_set_equal ["beets"], @b1.tags.all.map(&:name)
      assert_set_equal ["beets"], @b2.tags.all.map(&:name)
    end

    should "the tag should know its bookmarks" do
      assert_set_equal [@b2.name, @b1.name], @beet.bookmarks.all.map(&:name)
    end
  end


  #test "it should be able to find posts by tag and cleanup"
  #test "it should be able to find tags by post and cleanup"

  # def association_keys_in_cassandra
  #   a = Customer.connection.get_super_row(Customer.associations[:invoices].column_family, 
  #                                         @customer.key.to_s, "invoices", 
  #                                         :n_serializer => :uuid, :v_serializer => :string, :s_serializer => :string)
  #   a.values
  # end
 
end

