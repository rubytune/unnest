require 'active_record'

test_framework = if ActiveRecord::VERSION::STRING >= "4.1"
  require 'minitest/autorun'
  MiniTest::Test
else
  require 'test/unit'
  Test::Unit::TestCase
end

require File.expand_path(File.dirname(__FILE__) + "/../lib/unnest")
require File.expand_path(File.dirname(__FILE__) + "/../lib/unnest/relation")

def connect!
  begin
    ActiveRecord::Base.establish_connection :adapter => 'postgresql', database: 'unnest_gem_test'
    ActiveRecord::Base.connection.execute 'CREATE TABLE IF NOT EXISTS parent_models (id SERIAL NOT NULL PRIMARY KEY)'
    ActiveRecord::Base.connection.execute 'CREATE TABLE IF NOT EXISTS child_models (id SERIAL NOT NULL PRIMARY KEY, parent_model_id INTEGER NOT NULL)'
    ActiveRecord::Base.connection.execute 'DELETE FROM parent_models'
    ActiveRecord::Base.connection.execute 'DELETE FROM child_models'
  rescue ActiveRecord::NoDatabaseError
    system('createdb -E UTF8 unnest_gem_test') && retry
  end
end

class ParentModel < ActiveRecord::Base
  has_many :child_models
end

class ChildModel < ActiveRecord::Base
  belongs_to :parent_model
end

class UnnestTest < test_framework
  def setup
    ActiveRecord::Base.connection.tables.each do |table|
      ActiveRecord::Base.connection.execute "DELETE FROM #{table}"
    end

    Unnest.limit = 1

    @parent_1 = ParentModel.create
    @parent_2 = ParentModel.create
    @parent_3 = ParentModel.create
    @parent_1_child_1 = @parent_1.child_models.create
    @parent_1_child_2 = @parent_1.child_models.create
    @parent_2_child_1 = @parent_2.child_models.create
    @parent_2_child_2 = @parent_2.child_models.create
    @parent_ids = ParentModel.pluck(:id)
    @child_ids = ChildModel.pluck(:id)
  end

  def with_unnest_limit(lim)
    old_limit = Unnest.limit
    Unnest.limit = lim
    yield
    Unnest.limit = old_limit
  end

  def assert_uses_unnest(query, ids)
    assert_match(/INNER JOIN unnest\(array\[#{ids.join(',')}\]\)/i, query)
  end

  def assert_uses_in(query, ids)
    assert_match(/IN \(#{ids.join(', ')}\)/i, query)
  end

  def test_finds_correct_records
    desired = [@parent_1, @parent_2]
    assert_equal ParentModel.where(:id => desired.map(&:id)).to_a, desired
  end

  def test_where_in_replaced_with_unnested_array_join
    assert_uses_unnest(ParentModel.where(:id => @parent_ids).to_sql, @parent_ids)
  end

  def test_small_queries_are_not_altered
    with_unnest_limit(@parent_ids.size+1) do
      assert_uses_in(ParentModel.where(:id => @parent_ids).to_sql, @parent_ids)
    end
  end

  def test_preloads_use_unnested_array_join
    nth_query = 1
    listener = ActiveSupport::Notifications.subscribe('sql.active_record') do |*args|
      # 2nd query is the preload select
      if nth_query == 2
        sql = args[-1][:sql]
        assert_uses_unnest(sql, @parent_ids)
      end
      nth_query += 1;
    end

    ParentModel.preload(:child_models).to_a

    ActiveSupport::Notifications.unsubscribe(listener)
  end

  def test_preloads_find_correct_records
    records = ParentModel.preload(:child_models).to_a
    records.each do |parent|
      child_models = parent.child_models.to_a.sort_by(&:id)
      case parent
      when @parent_1
        assert_equal child_models, [@parent_1_child_1, @parent_1_child_2]
      when @parent_2
        assert_equal child_models, [@parent_2_child_1, @parent_2_child_2]
      when @parent_3
        assert_equal child_models, []
      end
    end
  end
end


connect!
