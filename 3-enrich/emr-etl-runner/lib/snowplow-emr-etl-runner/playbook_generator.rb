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
require 'base64'

# Implementation of Generator for playbooks
module Snowplow
  module EmrEtlRunner
    class PlaybookGenerator

      include Snowplow::EmrEtlRunner::Generator
      include Snowplow::EmrEtlRunner::Utils
      include Contracts

      STANDARD_HOSTED_ASSETS = "s3://snowplow-hosted-assets"

      Contract String => String
      def get_schema_name_from_version(version)
        get_schema_name("PlaybookConfig", "avro", version)
      end

      Contract ConfigHash, Bool, ArrayOf[String], String, ArrayOf[String] => Hash
      def create_datum(config, debug, skip, resolver, enrichments)
        enrich = not(skip.include?('enrich'))
        shred = not(skip.include?('shred'))
        s3distcp = not(skip.include?('s3distcp'))
        elasticsearch = not(skip.include?('elasticsearch'))

        {
          "region" => config[:aws][:emr][:region],
          "credentials" => {
            "accessKeyId" => config[:aws][:access_key_id],
            "secretAccessKey" => config[:aws][:secret_access_key]
          },
          "steps" =>
            get_steps(config, debug, enrich, shred, s3distcp, elasticsearch, resolver, enrichments)
        }
      end

      private

      Contract ConfigHash, Bool, Bool, Bool, Bool, Bool, String, ArrayOf[String] => ArrayOf[Hash]
      def get_steps(config, debug, enrich, shred, s3distcp, elasticsearch, resolver, enrichments)
        steps = []

        if debug
          steps << get_debugging_step(config[:aws][:emr][:region])
        end

        if config[:aws][:emr][:software][:hbase]
          steps << get_hbase_step(config[:aws][:emr][:software][:hbase])
        end

        run_tstamp = Time.new
        run_id = run_tstamp.strftime("%Y-%m-%d-%H-%M-%S")
        custom_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, config[:aws][:s3][:buckets][:assets], config[:aws][:emr][:region])
        assets = get_assets(
          custom_assets_bucket,
          config[:enrich][:versions][:hadoop_enrich],
          config[:enrich][:versions][:hadoop_shred],
          config[:enrich][:versions][:hadoop_elasticsearch])

        csbe = config[:aws][:s3][:buckets][:enriched]
        enrich_final_output = enrich ? partition_by_run(csbe[:good], run_id) : csbe[:good]
        enrich_step_output = s3distcp ? 'hdfs:///local/snowplow/enriched-events/' : enrich_final_output

        if enrich
          etl_tstamp = (run_tstamp.to_f * 1000).to_i.to_s
          steps += get_enrich_steps(config, s3distcp, assets[:enrich],
            enrich_step_output, enrich_final_output, run_id, etl_tstamp, resolver, enrichments)
        end

        if shred
          steps += get_shred_steps(config, s3distcp, enrich, assets[:shred],
            enrich_step_output, enrich_final_output, run_id, resolver)
        end

        if elasticsearch
          steps += get_es_steps(config, enrich, shred, assets[:elasticsearch], run_id)
        end

        steps
      end

      Contract ConfigHash, Bool, Bool, String, String => ArrayOf[Hash]
      def get_es_steps(config, enrich, shred, jar, run_id)
        elasticsearch_targets = config[:storage][:targets].select {|t| t[:type] == 'elasticsearch'}

        # The default sources are the enriched and shredded errors generated for this run
        default_sources = []
        default_sources << partition_by_run(config[:aws][:s3][:buckets][:enriched][:bad], run_id) if enrich
        default_sources << partition_by_run(config[:aws][:s3][:buckets][:shredded][:bad], run_id) if shred

        steps = elasticsearch_targets.flat_map { |target|
          sources = target[:sources] || default_sources
          sources.map { |s|
            args = []
            {
              '--input' => s,
              '--host' => target[:host],
              '--port' => target[:port].to_s,
              '--index' => target[:database],
              '--type' => target[:table],
              '--es_nodes_wan_only' => target[:es_nodes_wan_only] ? "true" : "false"
            }.reject { |k, v| v.nil? }
              .each do |k, v|
                args << k << v
              end
            step = get_custom_jar_step("Errors in #{s} -> Elasticsearch: #{target[:name]}", jar,
              [ 'com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob' ] + args)
            step
          }
        }

        # Wait 60 seconds before starting the first step so S3 can become consistent
        if (enrich || shred) && steps.any?
          steps[0]['arguments'] << '--delay' << '60'
        end
        steps
      end

      Contract ConfigHash, Bool, Bool, String, String, String,
        String, String => ArrayOf[Hash]
      def get_shred_steps(config, s3distcp, enrich, jar, enrich_step_output, enrich_final_output,
          run_id, resolver)
        steps = []

        legacy = (not (config[:aws][:emr][:ami_version] =~ /^[1-3].*/).nil?)
        s3_endpoint = get_s3_endpoint(config[:aws][:emr][:region])
        part_regex = '.*part-.*'

        csbs = config[:aws][:s3][:buckets][:shredded]
        shred_final_output = partition_by_run(csbs[:good], run_id)
        shred_step_output = s3distcp ? 'hdfs:///local/snowplow/shredded-events/' : shred_final_output

        if s3distcp and !enrich
          steps << get_s3distcp_step(legacy, 'S3DistCp: enriched S3 -> HDFS',
            enrich_final_output,
            enrich_step_output,
            s3_endpoint,
            [ '--srcPattern', part_regex ]
          )
        end

        steps << get_scalding_step('Shred enriched events', jar,
          'com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob',
          {
            :in     => glob_path(enrich_step_output),
            :good   => shred_step_output,
            :bad    => partition_by_run(csbs[:bad], run_id),
            :errors => partition_by_run(csbs[:errors], run_id, config[:enrich][:continue_on_unexpected_error])
          },
          [ '--iglu_config', Base64.strict_encode64(resolver) ]
        )

        if s3distcp
          output_codec = output_codec_from_compression_format(config[:enrich][:output_compression])
          steps << get_s3distcp_step(legacy, 'S3DistCp: shredded HDFS -> S3',
            shred_step_output,
            shred_final_output,
            s3_endpoint,
            [ '--srcPattern', part_regex ] + output_codec
          )
        end
        steps
      end

      Contract ConfigHash, Bool, String, String, String,
        String, String, String, ArrayOf[String] => ArrayOf[Hash]
      def get_enrich_steps(config, s3distcp, jar, enrich_step_output, enrich_final_output,
          run_id, etl_tstamp, resolver, enrichments)
        steps = []

        legacy = (not (config[:aws][:emr][:ami_version] =~ /^[1-3].*/).nil?)
        collector_format = config[:collectors][:format]
        s3_endpoint = get_s3_endpoint(config[:aws][:emr][:region])

        to_hdfs = is_supported_collector_format(collector_format) && s3distcp
        raw_input = config[:aws][:s3][:buckets][:raw][:processing]
        enrich_step_input = to_hdfs ? 'hdfs:///local/snowplow/raw-events/' : raw_input

        if to_hdfs
          added_args = if is_cloudfront_log(collector_format) || is_ua_ndjson(collector_format)
            [
              '--groupBy', is_ua_ndjson(collector_format) ? '.*(urbanairship).*' : '.*\\.([0-9]+-[0-9]+-[0-9]+)-[0-9]+\\..*',
              '--targetSize', '128',
              '--outputCodec', 'lzo'
            ]
          else
            []
          end
          steps << get_s3distcp_step(legacy, 'S3DistCp: raw S3 -> HDFS',
            raw_input,
            enrich_step_input,
            s3_endpoint,
            added_args
          )
        end

        steps << get_scalding_step('Enrich raw events', jar,
          'com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob',
          {
            :in     => enrich_step_input,
            :good   => enrich_step_output,
            :bad    => partition_by_run(config[:aws][:s3][:buckets][:enriched][:bad], run_id),
            :errors => partition_by_run(config[:aws][:s3][:buckets][:enriched][:errors], run_id, config[:enrich][:continue_on_unexpected_error])
          },
          [
            '--input_format', collector_format,
            '--etl_tstamp', etl_tstamp,
            '--iglu_config', Base64.strict_encode64(resolver),
            '--enrichments', build_enrichments_json(enrichments)
          ]
        )

        if s3distcp
          output_codec = output_codec_from_compression_format(config[:enrich][:output_compression])
          steps << get_s3distcp_step(legacy, 'S3DistCp: enriched HDFS -> S3',
            enrich_step_output,
            enrich_final_output,
            s3_endpoint,
            [ '--srcPattern', '.*part-.*' ] + output_codec
          )
          steps << get_s3distcp_step(legacy, 'S3DistCp: enriched HDFS _SUCCESS -> S3',
            enrich_step_output,
            enrich_final_output,
            s3_endpoint,
            [ '--srcPattern', '.*_SUCCESS' ]
          )
        end

        steps
      end

      Contract String => Hash
      def get_debugging_step(region)
        get_custom_jar_step('Setup Hadoop debugging',
          "s3://#{region}.elasticmapreduce/libs/script-runner/script-runner.jar",
          [ "s3://#{region}.elasticmapreduce/libs/state-pusher/0.1/fetch" ]
        )
      end

      Contract String, String, String, Hash, ArrayOf[String] => Hash
      def get_scalding_step(name, jar, main_class, folders, args=[])
        added_args = [main_class, '--hdfs', ] + args
        {
          "--input_folder" => folders[:in],
          "--output_folder" => folders[:good],
          "--bad_rows_folder" => folders[:bad],
          "--exceptions_folder" => folders[:errors]
        }.reject { |k, v| v.nil? }
          .each do |k, v|
            added_args << k << v
          end
        get_custom_jar_step(name, jar, added_args)
      end

      Contract Bool, String, String, String, String, ArrayOf[String] => Hash
      def get_s3distcp_step(legacy, name, src, dest, endpoint, args=[])
        jar = if legacy
          '/home/hadoop/lib/emr-s3distcp-1.0.jar'
        else
          '/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar'
        end
        get_custom_jar_step(name, jar, [
            "--src", src,
            "--dest", dest,
            "--s3Endpoint", endpoint
          ] + args
        )
      end

      Contract String, String => Hash
      def get_hbase_step(hbase_version)
        get_custom_jar_step(
          "Start HBase #{hbase_version}",
          "/home/hadoop/lib/hbase-#{hbase_version}.jar",
          [ "emr.hbase.backup.Main", "--start-master" ]
        )
      end

      Contract String, String, ArrayOf[String] => Hash
      def get_custom_jar_step(name, jar, args=[])
        {
          "type" => "CUSTOM_JAR",
          "name" => name,
          "actionOnFailure" => "TERMINATE_JOB_FLOW",
          "jar" => jar,
          "arguments" => args
        }
      end

    end
  end
end
