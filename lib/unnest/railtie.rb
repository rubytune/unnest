module Unnest
  class Railtie < Rails::Railtie
    initializer('unnest.patches') do
      ActiveRecord::Relation.send(:define_method, :where) do |opts, *rest|
        return self if opts.blank?

        unnests = {}
        if opts.is_a?(Hash)
          opts = opts.clone
          opts.keys.each do |col|
            if "#_{col}"[-3..-1] == 'id' && opts[col].is_a?(Array) && opts[col].size > 1
              unnests[col] = opts.delete(col)
            end
          end
        elsif opts.is_a?(Arel::Nodes::In) && opts.right.is_a?(Array) && opts.right.size > 1 &&
            opts.left.is_a?(Arel::Attributes::Attribute)
          unnests[opts.left.name] = opts.right
          opts = nil
        end

        relation = clone
        relation.where_values += build_where(opts, rest) if opts

        i = 0
        unless unnests.empty?
          unnests.each do |col, values|
            unnest = "unnest(array[#{values.map(&:to_i).join(',')}])"
            tmp = "_unnest_vals#{i}"
            relation = relation.joins("INNER JOIN #{unnest} #{tmp} ON #{table_name}.#{col} = #{tmp}")
            i = i + 1
          end
        end
        relation
      end
    end
  end
end
