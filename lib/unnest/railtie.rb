module Unnest
  class Railtie < Rails::Railtie
    initializer('unnest.patches') do
      ActiveRecord::Relation.send(:define_method, :where) do |opts, *rest|
        return self if opts.blank?

        unnests = {}
        if opts.is_a?(Hash)
          opts = opts.clone
          opts.keys.each do |col|
            if opts[col].is_a?(Array) && opts[col].size > 1
              unnests[col] = opts.delete(col)
            end
          end
        end

        relation = clone
        relation.where_values += build_where(opts, rest)

        i = 0
        unless unnests.empty?
          unnests.each do |col, values|
            vals = values.map{ |v| connection.quote v }
            unnest = "unnest(array[#{vals.join(',')}])"
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
