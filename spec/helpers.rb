# frozen_string_literal: true

require 'logger'
require 'method_source'
require 'stringio'

def wait_for(timeout_milliseconds = 2000, &blk)
  timeout = timeout_milliseconds / 1000.0
  finish = Time.now + timeout
  result = nil

  while Time.now < finish && !(result = blk.call)
    sleep(0.001)
  end

  flunk("wait_for timed out:\n#{blk.source}") if !result
end

def test_config_for_backend(backend)
  config = {
    backend: backend,
    logger: Logger.new(IO::NULL),
  }

  case backend
  when :redis
    config[:redis_config] = { url: ENV['REDISURL'] }
  when :postgres
    config[:backend_options] = {
      host: ENV['PGHOST'],
      user: ENV['PGUSER'] || ENV['USER'],
      password: ENV['PGPASSWORD'],
      dbname: ENV['PGDATABASE'] || 'message_bus_test'
    }
  end
  config
end

# Captures logger output regardless of where each backend stashes its
# logger instance, so shared reconnect-continuity tests can detect the
# "subscribe failed, reconnecting in 1 second" line that both network
# backends emit right before their retry sleep.
def capture_backend_log(bus)
  log_output = StringIO.new
  logger = Logger.new(log_output)

  case CURRENT_BACKEND
  when :redis
    bus.instance_variable_set(:@logger, logger)
  when :postgres
    bus.instance_variable_get(:@config)[:logger] = logger
  end

  log_output
end

# Postgres polls wait_for_notify(10) by default; shorten that only for
# Postgres so reconnect-continuity assertions run quickly without
# affecting Redis, which reacts to disconnect immediately.
def speed_up_reconnect_detection(bus)
  instrument_postgres_client(bus) if CURRENT_BACKEND == :postgres
end

# Records LISTEN attempts and which thread closed each subscriber
# connection (pool connections are excluded), and shortens the notify poll
# so a reconnect request is picked up quickly.
def instrument_postgres_client(backend)
  recorded = { subscribes: [], close_threads: [] }
  client = backend.send(:client)
  real_subscribe = client.method(:subscribe)
  real_raw_pg_connection = client.method(:raw_pg_connection)

  client.define_singleton_method(:subscribe) do |channel, &blk|
    recorded[:subscribes] << channel
    real_subscribe.call(channel, &blk)
  end

  client.define_singleton_method(:raw_pg_connection) do
    conn = real_raw_pg_connection.call
    subscriber = false

    conn.define_singleton_method(:wait_for_notify) do |_timeout = nil, &blk|
      subscriber = true
      super(0.05, &blk)
    end

    conn.define_singleton_method(:close) do
      recorded[:close_threads] << Thread.current if subscriber
      super()
    end

    conn
  end

  recorded
end
