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

require 'json'
require 'json-schema'
require 'spec_helper'

describe Iglu do
  
  it 'correctly parse and validate self-describing JSON' do
    instance = '{"schema": "iglu:com.parrable/decrypted_payload/jsonschema/1-0-0", ' \
               ' "data": {' \
               '   "browserId": "9b5cfd54-3b90-455c-9455-9d215ec1c414",' \
               '   "deviceId": "asdfasdfasdfasdfcwer234fa$#ds±f324jo"' \
               ' }' \
               '}'
    Iglu::SelfDescribingJson.parse_json(JSON.parse(instance)).valid?.should == true
  end

  it 'correctly parse and invalidate invalid self-describing JSON' do
    instance = '{"schema": "iglu:com.parrable/decrypted_payload/jsonschema/1-0-0", ' \
               ' "data": {' \
               '   "browserId": "9b5cfd54-3b90-455c-9455-9d215ec1c414",' \
               '   "deviceId": "asdfasdfasdfasdfcwer234fa$#ds±f324joa"' \
               ' }' \
               '}'
    Iglu::SelfDescribingJson.parse_json(JSON.parse(instance)).valid?.should == false
  end

  it 'correctly parse Iglu URI into object' do
    Iglu::SchemaKey.parse_key("iglu:com.snowplowanalytics.snowplow/event/jsonschema/1-0-1").should == Iglu::SchemaKey.new("com.snowplowanalytics.snowplow", "event", "jsonschema", Iglu::SchemaVer.new(1, 0, 1))
  end

  it 'correctly parse SchemaVer into object' do
    Iglu::SchemaVer.parse_schemaver("2-0-3").should == Iglu::SchemaVer.new(2, 0, 3)
  end

  it 'throw exception on incorrect SchemaKey (without iglu protocol)' do
    expect { Iglu::SchemaKey.parse_key("com.snowplowanalytics.snowplow/event/jsonschema/1-0-1") }.to raise_error(Iglu::IgluError)
  end

  it 'throw exception on incorrect SchemaKey (with incorrect SchemaVer)' do
    expect { Iglu::SchemaKey.parse_key("iglu:com.snowplowanalytics.snowplow/event/jsonschema/1-a-1") }.to raise_error(Iglu::IgluError)
  end

  it 'correctly parse and validate self-describing JSON' do
    instance = '{"schema": "iglu:com.parrable/decrypted_payload/jsonschema/1-0-0", ' \
               ' "data": {' \
               '   "browserId": "9b5cfd54-3b90-455c-9455-9d215ec1c414",' \
               '   "deviceId": "asdfasdfasdfasdfcwer234fa$#ds±f324jo"' \
               ' }' \
               '}'
    json = JSON::parse(instance)
    Iglu::Client.validate(json).should == true
  end

  it 'correctly invalidate self-describing JSON (wrong string length) by returning exception' do
    instance = '{"schema": "iglu:com.parrable/decrypted_payload/jsonschema/1-0-0", ' \
               ' "data": {' \
               '   "browserId": "9b5cfd54-3b90-455c-9455-9d215ec1c414",' \
               '   "deviceId": "asdfasdfasdfasdfcwer234fa$#ds±f324joa"' \
               ' }' \
               '}'
    json = JSON::parse(instance)
    expect { Iglu::Client.validate(json) }.to raise_error(JSON::Schema::ValidationError)
  end
end
