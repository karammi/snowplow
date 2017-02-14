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

require 'spec_helper'

Generator = Snowplow::EmrEtlRunner::Generator
Cli = Snowplow::EmrEtlRunner::Cli

describe Generator do

  class MockGenerator
    include Generator
  end

  subject { MockGenerator.new }

  let(:config) {
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/sparse_config.yml"
    Cli.load_config(filename, "")
  }

  describe '#generate' do
    it 'should take a config, a version and a filename as arguments' do
      expect(subject).to respond_to(:generate).with(3).argument
    end

    it 'should raise a RuntimeError since #get_schema is not implemented' do
      expect {
        subject.generate(config, 'v', 'f')
      }.to raise_error(RuntimeError,
        '#get_schema_name_from_version needs to be defined in all generators.')
    end
  end

  describe '#get_schema_name_from_version' do
    it 'should take a version as argument' do
      expect(subject).to respond_to(:get_schema_name_from_version).with(1).argument
    end

    it 'should raise a RuntimeError by default with only a version argument' do
      expect {
        subject.get_schema_name_from_version('')
      }.to raise_error(RuntimeError,
        '#get_schema_name_from_version needs to be defined in all generators.')
    end
  end

  describe '#get_schema_name' do
    it 'should take name, format and version as arguments' do
      expect(subject).to respond_to(:get_schema_name).with(3).argument
    end

    it 'should give back a schema name with only a version argument' do
      expect(subject.get_schema_name('n', 'f', 'v'))
        .to eq('com.snowplowanalytics.dataflowrunner/n/f/v')
    end
  end

  describe '#get_schema' do
    it 'should take a version as argument' do
      expect(subject).to respond_to(:get_schema).with(1).argument
    end

    it 'should raise a RuntimeError by default since get_schema_name_from_version is not impl' do
      expect {
        subject.get_schema('')
      }.to raise_error(RuntimeError,
        '#get_schema_name_from_version needs to be defined in all generators.')
    end
  end

  describe '#create_datum' do
    it 'should take a config as argument' do
      expect(subject).to respond_to(:create_datum).with(1).argument
    end

    it 'should raise a RuntimeError by default' do
      expect {
        subject.create_datum(config)
      }.to raise_error(RuntimeError, '#create_datum needs to be defined in all generators.')
    end
  end

  describe '#download_as_string' do
    it 'should take a string as argument' do
      expect(subject).to respond_to(:download_as_string).with(1).argument
    end

    it 'should download a file from the specified url' do
      filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/iglu_resolver.json"
      expect(subject.download_as_string(filename).length).to eq(404)
    end

    it 'should fail if the file doesnt exist' do
      expect {
        subject.download_as_string("notafile.txt").length
      }.to raise_error(Errno::ENOENT, 'No such file or directory - notafile.txt')
    end
  end
end
