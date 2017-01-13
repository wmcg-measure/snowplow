# Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Copyright:: Copyright (c) 2017 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require "httparty"
require "json-schema"

module Iglu
  # Iglu Cliend. Able to fetch schemas only from Iglu Central
  class Client
    # Primary method. Validate self-describing JSON instance
    def self.validate(json)
      
      begin
        schema_uri = json[:schema] || json["schema"]
        data = json[:data] || json["data"]
      rescue Exception => _
        raise IgluError.new "JSON instance is not self-describing:\n #{json.to_json}"
      end
        
      schema_key = SchemaKey.parse_key schema_uri
      schema = fetch schema_key
      JSON::Validator.validate!(schema, data)
    end

    def self.fetch(schema)
      uri = "http://iglucentral.com/schemas/#{schema.as_path}"
      begin
        response = HTTParty.get(uri)
      rescue SocketError => _
        raise IgluError.new "Iglu Central is not available"
      end
      if response.code == 200
        JSON::parse(response.body)
      else
        raise IgluError.new "Schema [#{schema.as_uri}] not found on [#{uri}]"
      end
    end
  end
end
