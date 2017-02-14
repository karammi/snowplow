# Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'
require 'open-uri'
require 'avro'
require 'json'

# Abstract class defining a generator behavior.
# See implementing classes: EmrClusterGenerator and PlaybookGenerator.
module Snowplow
  module EmrEtlRunner
    module Generator

      include Contracts

      # Print out the Avro record given by create_record to a file using the schema provided by
      # get_schema
      Contract ConfigHash, String, String, Bool, ArrayOf[String], String, ArrayOf[String] => nil
      def generate(config, version, filename, debug=false, skip=[], resolver='', enrichments=[])
        raw_schema = get_schema(version)
        avro_schema = Avro::Schema.parse(raw_schema)
        datum = create_datum(config, debug, skip, resolver, enrichments)
        if Avro::Schema.validate(avro_schema, datum)
          json = {
            "schema" => "iglu:" + get_schema_name_from_version(version),
            "data" => datum
          }.to_json
          File.open(filename, 'w+') { |file|
            file.write(json)
          }
        else
          raise ConfigError, "Config could not be validated against the schema"
        end
        nil
      end

      # Get the associated Avro schema
      Contract String => String
      def get_schema(version)
        url = "https://raw.githubusercontent.com/snowplow/iglu-central/master/schemas/" +
          get_schema_name_from_version(version)
        download_as_string(url)
      end

      # Get the associated Avro schema name
      Contract String => String
      def get_schema_name_from_version(version)
        raise RuntimeError, '#get_schema_name_from_version needs to be defined in all generators.'
      end

      # Get the associated schema name from name, format and version
      Contract String, String, String => String
      def get_schema_name(name, fmt, version)
        "com.snowplowanalytics.dataflowrunner/#{name}/#{fmt}/#{version}"
      end

      # Create a valid Avro datum
      Contract ConfigHash, Bool, ArrayOf[String], String, ArrayOf[String] => Hash
      def create_datum(config, debug=false, skip=[], resolver='', enrichments=[])
        raise RuntimeError, '#create_datum needs to be defined in all generators.'
      end

      # Download a file as a string.
      Contract String => String
      def download_as_string(url)
        open(url) { |io| data = io.read }
      end

    end
  end
end
