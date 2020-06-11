# frozen_string_literal: true

require 'mongo'

Mongo::Logger.logger.level = Logger::FATAL

class MongoBenchmark

  CONNECTION_STRING = ENV.fetch('MONGO_CONNECTION_STRING', 'localhost:27018')
  CONNECTION_OPTIONS = {
    max_pool_size: 200,
    ssl: true,
    ssl_verify: false,
    ssl_verify_hostname: false,
    connect: :direct,
    ssl_ca_cert: ENV.fetch('MONGO_CA_CERT'),
    password: ENV.fetch('MONGO_PASSWORD'),
    user: ENV.fetch('MONGO_USER')
  }.freeze
  DATES = (Date.parse('2020-01-01')...Date.parse('2020-02-01')).to_a
  EVENTS = %w[delivered bounced opened clicked spam].freeze
  ACCOUNT_IDS = Array.new(1) { SecureRandom.uuid }
  DOMAIN_IDS = Array.new(700) { SecureRandom.uuid }
  RECIPIENT_MXES = Array.new(2) { SecureRandom.uuid }
  RECIPIENT_SERVICES = Array.new(30) { |i| "recipient_service_#{i}" }
  CATEGORIES = Array.new(30) { |i| "category#{i}" }
  OUTGOING_IPS = Array.new(2) { |i| "outgoing_ip_#{i}" }

  def recreate_table
    agg_counts.drop
    agg_counts.create
  end

  def agg_counts
    @agg_counts ||= client[:agg_counts]
  end

  def email_logs_agg
    @email_logs_agg ||= client[:email_logs_agg]
  end

  def client
    @client ||= Mongo::Client.new([CONNECTION_STRING], CONNECTION_OPTIONS)
  end

  def upsert_record
    agg_counts.find_one_and_update(
      record_attrs, {
        '$inc' => { count: 1 }
      },
      upsert: true
    )
  end

  def read_record
    email_logs_agg.find(
      event: Time.now.to_i.to_s + rand.to_s, # rubocop:disable Rails/TimeZone
      outbound_ip: %w[149.72.193.69 167.89.22.139].sample
    ).first
  end

  private

  def record_attrs
    {
      date: DATES.sample,
      event: EVENTS.sample,
      account_id: ACCOUNT_IDS.sample,
      domain_id: DOMAIN_IDS.sample,
      category: CATEGORIES.sample,
      outgoing_ip: OUTGOING_IPS.sample,
      recipient_mx: RECIPIENT_MXES.sample,
      recipient_service: RECIPIENT_SERVICES.sample
    }
  end

end
