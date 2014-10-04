module Unnest
  class Railtie < Rails::Railtie
    initializer('unnest.patches') do
      ActiveRecord::Relation.send(:alias_method, :original_where, :where)

      ActiveRecord::Relation.send(:define_method, :where) do |opts, *rest|
        return self if opts.blank?

        opts, unnests = separate_unnest_options(opts)
        unnests = construct_unnest_join_clauses(unnests)

        if unnests.blank?
          original_where(opts, rest)
        else
          original_where(opts, rest).joins(unnests)
        end
      end

      ActiveRecord::Relation.send(:define_method, :separate_unnest_options) do |opts|
        unnests = {}
        if opts.is_a?(Hash)
          opts = opts.clone
          opts.keys.each do |col|
            if "_#{col}"[-3..-1] == '_id' && opts[col].is_a?(Array) && opts[col].size > 1
              unnests[col] = opts.delete(col)
            end
          end
        elsif opts.is_a?(Arel::Nodes::In) && opts.right.is_a?(Array) && opts.right.size > 1 &&
            opts.left.is_a?(Arel::Attributes::Attribute)
          unnests[opts.left.name] = opts.right
          opts = nil
        end
        [opts, unnests]
      end

      ActiveRecord::Relation.send(:define_method, :construct_unnest_join_clauses) do |unnests|
        joins = []
        unless unnests.empty?
          unnests.each do |col, values|
            unnest = "unnest(array[#{values.map(&:to_i).join(',')}])"
            tmp = "_unnest_vals#{joins.size}"
            joins << "INNER JOIN #{unnest} #{tmp} ON #{table_name}.#{col} = #{tmp}"
          end
        end
        joins.join(' ')
      end
    end
  end
end
