module Unnest
  class Railtie < Rails::Railtie
    initializer('unnest') do
      ActiveSupport.on_load :active_record do
        require 'unnest/relation'
      end
    end
  end
end
