
module Unnest
  @@limit = 300

  def self.limit=(x)
    @@limit = x
  end

  def self.limit
    @@limit
  end
end

require 'unnest/railtie'
