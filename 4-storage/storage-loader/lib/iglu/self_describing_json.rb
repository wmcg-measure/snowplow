# Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Copyright:: Copyright (c) 2016 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require "json-schema"

module Iglu

  # Class holding SchemaVer data
  class SelfDescribingJson
    attr_accessor :schema, :data

    # Constructor. To initalize from string - use static parse_schemaver
    def initialize(schema, data)
      @schema = schema
      @data = data
      @valid = false
    end

    def to_json 
      {
        :schema => @schema.as_uri,
        :data => @data
      }
    end
  
    def validate
      result = Client.validate(to_json)
      @valid = true
      result
    end
  
    def valid?
      begin
        @valid or validate
      rescue JSON::Schema::ValidationError => _
        false
      end
    end

    # Parse Hash (either with string or symbols) into self-describing JSON instance
    def self.parse_json(json)
      schema = json[:schema] || json['schema']
      data = json[:data] || json['data']
      if schema.nil? or data.nil?
        raise IgluError.new "Not a self-describing JSON"
      end
      schema_key = SchemaKey.parse_key(schema)
      SelfDescribingJson.new(schema_key, data)
    end
  end
end
