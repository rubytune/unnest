module ActiveRecord
  class Relation

    alias_method :original_where, :where

    def where(opts, *rest)
      return self if opts.blank?

      opts, unnests = separate_unnest_options(opts)
      unnests = construct_unnest_join_clauses(unnests)

      if unnests.blank?
        original_where(opts, rest)
      else
        original_where(opts, rest).joins(unnests)
      end
    end


    private

    def separate_unnest_options(opts)
      unnests = {}
      if opts.is_a?(Hash)
        opts = opts.clone
        opts.keys.each do |col|
          # Only apply to pkey/fkey columns
          # Only apply if argument size is sufficiently large
          next unless "_#{col}"[-3..-1] == '_id'
          next unless opts[col].is_a?(Array) && opts[col].size > Unnest.limit
          unnests[col] = opts.delete(col)
        end
      elsif opts.is_a?(Arel::Nodes::In) && opts.right.is_a?(Array) && opts.right.size > Unnest.limit
        if defined?(Arel::Nodes::Casted)
          unnests[opts.left.name] = opts.right.map do |id|
            id.is_a?(Arel::Nodes::Casted) ? id.val : id
          end
        else
          unnests[opts.left.name] = opts.right
        end
        opts = nil
      end
      [opts, unnests]
    end

    def construct_unnest_join_clauses(unnests)
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
