# frozen_string_literal: true

require 'securerandom'
require 'benchmark/ips'
require './benchmarks/mongo_benchmark'

THREAD_COUNT = ENV.fetch('MONGO_THREADS', 10).to_i
BENCHMARK_TIME = ENV.fetch('BENCHMARK_TIME', 60).to_i

mongo_benchmarks = Array.new(THREAD_COUNT) { MongoBenchmark.new }

Benchmark.ips do |x|
  x.config(time: BENCHMARK_TIME, warmup: 2)

  x.report('DocumentDB: upsert records') do
    Array.new(THREAD_COUNT) do |i|
      Thread.new { mongo_benchmarks[i].upsert_record }
    end.map(&:join)
  end

  x.report('DocumentDB: read records') do
    Array.new(THREAD_COUNT) do |i|
      Thread.new { mongo_benchmarks[i].read_record }
    end.map(&:join)
  end
end

mongo_benchmarks.each { |benchmark| benchmark.client.close }
