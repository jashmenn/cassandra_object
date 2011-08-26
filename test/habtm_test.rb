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
      assert_set_equal ["beets", "bears"], get_raw_tag_names_for(@b1)
      @b1.tags << @bear
      assert_set_equal ["beets", "bears"], get_raw_tag_names_for(@b1)
    end

    should "be able to delete tags" do
      @b1.tags << @battle

      assert_set_equal ["beets", "bears", "battlestar-gallactica"], get_raw_tag_names_for(@b1)

      @b1.tags.delete(@battle)

      assert_set_equal ["beets", "bears"], get_raw_tag_names_for(@b1)
    end

    should "be able to ignore and cleanup junk relationships" do
      # verify both raw and relational names are the same
      assert_set_equal ["beets", "bears"], get_raw_tag_names_for(@b1)
      assert_set_equal ["beets", "bears"], @b1.tags.all.map(&:name)

      # add some junk key
      connection.put_row(Bookmark.associations[:tags].column_family, @b1.key.to_s, {"tags"=>{"roger"=>true}}, @sopts)
      assert_set_equal ["beets", "bears", "roger"], get_raw_tag_names_for(@b1)
      assert_set_equal ["beets", "bears"], @b1.tags.all.map(&:name)   # ignore the junk
      assert_set_equal ["beets", "bears"], get_raw_tag_names_for(@b1) # junk should have been removed
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

  def get_raw_tag_names_for(bookmark)
    Bookmark.connection.get_super_row(Bookmark.associations[:tags].column_family, bookmark.key.to_s, "tags", @sopts).keys
  end
  
end

