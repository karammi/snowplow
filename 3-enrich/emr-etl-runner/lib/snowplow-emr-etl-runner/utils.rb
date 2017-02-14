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

# Module with diverse utilities dealing with a few quirks in EmrEtlRunner
module Snowplow
  module EmrEtlRunner
    module Utils

      include Contracts

      # Builds the region-appropriate bucket name for Snowplow's hosted assets.
      # Has to be region-specific because of https://github.com/boto/botocore/issues/424
      #
      # Parameters:
      # +standard_bucket+:: Snowplow's hosted asset bucket
      # +bucket+:: the specified hosted assets bucket
      # +region+:: the AWS region to source hosted assets from
      Contract String, String, String => String
      def get_hosted_assets_bucket(standard_bucket, bucket, region)
        bucket = bucket.chomp('/')
        suffix = if !bucket.eql? standard_bucket or region.eql? "eu-west-1" then "" else "-#{region}" end
        "#{bucket}#{suffix}/"
      end

      # Get commons-codec version required by Scala Hadoop Enrich for further replace
      # See: https://github.com/snowplow/snowplow/issues/2735
      Contract String => String
      def get_cc_version(she_version)
        she_version_normalized = Gem::Version.new(she_version)
        if she_version_normalized > Gem::Version.new("1.8.0")
          "1.10"
        else
          "1.5"
        end
      end

      # Is this collector format supported?
      Contract String => Bool
      def is_supported_collector_format(fmt)
        is_cloudfront_log(fmt) || fmt == "thrift" || is_ua_ndjson(fmt)
      end

      # Does this collector format represent CloudFront access logs?
      Contract String => Bool
      def is_cloudfront_log(collector_format)
        collector_format == "cloudfront" or
          collector_format.start_with?("tsv/com.amazon.aws.cloudfront/")
      end

      # Does this collector format represent ndjson/urbanairship?
      Contract String => Bool
      def is_ua_ndjson(collector_format)
        /^ndjson\/com\.urbanairship\.connect\/.+$/ === collector_format
      end

      # Returns the S3 endpoint for a given S3 region
      Contract String => String
      def get_s3_endpoint(s3_region)
        if s3_region == "us-east-1"
          "s3.amazonaws.com"
        else
          "s3-#{s3_region}.amazonaws.com"
        end
      end

      # Retrieves the paths of hadoop enrich, hadoop shred and hadoop elasticsearch
      Contract String, String, String, String => AssetsHash
      def get_assets(assets_bucket, hadoop_enrich_version, hadoop_shred_version, hadoop_elasticsearch_version)
        enrich_path_middle = hadoop_enrich_version[0] == '0' ? 'hadoop-etl/snowplow-hadoop-etl' : 'scala-hadoop-enrich/snowplow-hadoop-enrich'
        {
          :enrich   => "#{assets_bucket}3-enrich/#{enrich_path_middle}-#{hadoop_enrich_version}.jar",
          :shred    => "#{assets_bucket}3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-#{hadoop_shred_version}.jar",
          :elasticsearch => "#{assets_bucket}4-storage/hadoop-elasticsearch-sink/hadoop-elasticsearch-sink-#{hadoop_elasticsearch_version}.jar",
        }
      end

      # Returns a base64-encoded JSON containing an array of enrichment JSONs
      Contract ArrayOf[String] => String
      def build_enrichments_json(enrichments_array)
        enrichments_json_data = enrichments_array.map {|e| JSON.parse(e)}
        enrichments_json = {
          'schema' => 'iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0',
          'data'   => enrichments_json_data
        }

        Base64.strict_encode64(enrichments_json.to_json)
      end

      # We need to partition our output buckets by run ID, buckets already have trailing slashes
      #
      # Parameters:
      # +folder+:: the folder to append a run ID folder to
      # +run_id+:: the run ID to append
      # +retain+:: set to false if this folder should be nillified
      Contract Maybe[String], String, Bool => Maybe[String]
      def partition_by_run(folder, run_id, retain=true)
        "#{folder}run=#{run_id}/" if retain
      end

      # Converts the output_compression configuration field to
      Contract Maybe[String] => ArrayOf[String]
      def output_codec_from_compression_format(compression_format)
        if compression_format.nil?
          []
        else
          codec = compression_format.downcase
          [ '--outputCodec', codec == "gzip" ? "gz" : codec ]
        end
      end

      # Adds a match all glob to the end of the path
      Contract String => String
      def glob_path(path)
        path.end_with?('/*') ? path : "#{path}/*"
      end

    end
  end
end
