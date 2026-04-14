# frozen_string_literal: true

require_relative '../spec_helper'
require 'message_bus'

describe MessageBus do
  before do
    @bus = MessageBus::Instance.new
    @bus.site_id_lookup do
      "magic"
    end
    @bus.configure(test_config_for_backend(CURRENT_BACKEND))
  end

  after do
    @bus.reset!
    @bus.destroy
  end

  it "destroying immediately after `after_fork` does not lock" do
    10.times do
      @bus.on
      @bus.after_fork
      @bus.destroy
    end
  end

  describe "#base_route=" do
    it "adds leading and trailing slashes" do
      @bus.base_route = "my/base/route"
      @bus.base_route.must_equal '/my/base/route/'
    end

    it "leaves existing leading and trailing slashes" do
      @bus.base_route = "/my/base/route/"
      @bus.base_route.must_equal '/my/base/route/'
    end

    it "removes duplicate slashes" do
      @bus.base_route = "//my///base/route"
      @bus.base_route.must_equal '/my/base/route/'
    end
  end

  it "can subscribe from a point in time" do
    @bus.publish("/minion", "banana")

    data1 = []
    data2 = []
    data3 = []

    @bus.subscribe("/minion") do |msg|
      data1 << msg.data
    end

    @bus.subscribe("/minion", 0) do |msg|
      data2 << msg.data
    end

    @bus.subscribe("/minion", 1) do |msg|
      data3 << msg.data
    end

    @bus.publish("/minion", "bananana")
    @bus.publish("/minion", "it's so fluffy")

    wait_for(2000) do
      data3.length == 3 && data2.length == 3 && data1.length == 2
    end

    data1.must_equal ['bananana', "it's so fluffy"]
    data2.must_equal ['banana', 'bananana', "it's so fluffy"]
    data3.must_equal ['banana', 'bananana', "it's so fluffy"]
  end

  it "can transmit client_ids" do
    client_ids = nil

    @bus.subscribe("/chuck") do |msg|
      client_ids = msg.client_ids
    end

    @bus.publish("/chuck", { yeager: true }, client_ids: ['a', 'b'])
    wait_for(2000) { client_ids }

    client_ids.must_equal ['a', 'b']
  end

  it "should recover from a reset" do
    data = nil
    @bus.subscribe("/chuck") do |msg|
      data = msg.data
    end
    @bus.publish("/chuck", norris: true)
    @bus.publish("/chuck", norris: true)
    @bus.publish("/chuck", norris: true)

    @bus.backend_instance.reset!

    @bus.publish("/chuck", yeager: true)

    wait_for(2000) { data && data["yeager"] }

    data["yeager"].must_equal true
  end

  it "should recover from a backlog expiring" do
    data = nil
    @bus.subscribe("/chuck") do |msg|
      data = msg.data
    end
    @bus.publish("/chuck", norris: true)
    @bus.publish("/chuck", norris: true)
    @bus.publish("/chuck", norris: true)

    @bus.backend_instance.expire_all_backlogs!

    @bus.publish("/chuck", yeager: true)

    wait_for(2000) { data && data["yeager"] }

    data["yeager"].must_equal true
  end

  it "should automatically decode hashed messages" do
    data = nil
    @bus.subscribe("/chuck") do |msg|
      data = msg.data
    end
    @bus.publish("/chuck", norris: true)
    wait_for(2000) { data }

    data["norris"].must_equal true
  end

  it "should get a message if it subscribes to it" do
    user_ids, data, site_id, channel = nil

    @bus.subscribe("/chuck") do |msg|
      data = msg.data
      site_id = msg.site_id
      channel = msg.channel
      user_ids = msg.user_ids
    end

    @bus.publish("/chuck", "norris", user_ids: [1, 2, 3])

    wait_for(2000) { data }

    data.must_equal 'norris'
    site_id.must_equal 'magic'
    channel.must_equal '/chuck'
    user_ids.to_a.must_equal [1, 2, 3]
  end

  it "should get global messages if it subscribes to them" do
    data, site_id, channel = nil

    @bus.subscribe do |msg|
      data = msg.data
      site_id = msg.site_id
      channel = msg.channel
    end

    @bus.publish("/chuck", "norris")

    wait_for(2000) { data }

    data.must_equal 'norris'
    site_id.must_equal 'magic'
    channel.must_equal '/chuck'
  end

  it "should have the ability to grab the backlog messages in the correct order" do
    id = @bus.publish("/chuck", "norris")
    @bus.publish("/chuck", "foo")
    @bus.publish("/chuck", "bar")

    r = @bus.backlog("/chuck", id)

    r.map { |i| i.data }.to_a.must_equal ['foo', 'bar']
  end

  it "should correctly get full backlog of a channel" do
    @bus.publish("/chuck", "norris")
    @bus.publish("/chuck", "foo")
    @bus.publish("/chuckles", "bar")

    @bus.backlog("/chuck").map { |i| i.data }.to_a.must_equal ['norris', 'foo']
  end

  it "should correctly restrict the backlog size of a channel" do
    @bus.publish("/chuck", "norris")
    @bus.publish("/chuck", "foo", max_backlog_size: 1)

    @bus.backlog("/chuck").map { |i| i.data }.to_a.must_equal ['foo']
  end

  it "can be turned off" do
    @bus.off

    @bus.off?.must_equal true

    @bus.publish("/chuck", "norris")

    @bus.backlog("/chuck").to_a.must_equal []
  end

  it "can be turned off only for subscriptions" do
    @bus.off(disable_publish: false)

    @bus.off?.must_equal true

    data = []

    @bus.subscribe("/chuck") do |msg|
      data << msg.data
    end

    @bus.publish("/chuck", "norris")

    @bus.on

    @bus.subscribe("/chuck") do |msg|
      data << msg.data
    end

    @bus.publish("/chuck", "berry")

    wait_for(2000) { data.length > 0 }

    data.must_equal ["berry"]

    @bus.backlog("/chuck").map(&:data).to_a.must_equal ["norris", "berry"]
  end

  it "can call destroy multiple times" do
    @bus.destroy
    @bus.destroy
    @bus.destroy
  end

  it "can be turned on after destroy" do
    @bus.destroy

    @bus.on

    @bus.after_fork
  end

  it "allows you to look up last_message" do
    @bus.publish("/bob", "dylan")
    @bus.publish("/bob", "marley")
    @bus.last_message("/bob").data.must_equal "marley"
    assert_nil @bus.last_message("/nothing")
  end

  describe "#publish" do
    it "allows publishing to a explicit site" do
      data, site_id, channel = nil

      @bus.subscribe do |msg|
        data = msg.data
        site_id = msg.site_id
        channel = msg.channel
      end

      @bus.publish("/chuck", "norris", site_id: "law-and-order")

      wait_for(2000) { data }

      data.must_equal 'norris'
      site_id.must_equal 'law-and-order'
      channel.must_equal '/chuck'
    end
  end

  describe "global subscriptions" do
    before do
      seq = 0
      @bus.site_id_lookup do
        (seq += 1).to_s
      end
    end

    it "can get last_message" do
      @bus.publish("/global/test", "test")
      @bus.last_message("/global/test").data.must_equal "test"
    end

    it "can subscribe globally" do
      data = nil
      @bus.subscribe do |message|
        data = message.data
      end

      @bus.publish("/global/test", "test")
      wait_for(1000) { data }

      data.must_equal "test"
    end

    it "can subscribe to channel" do
      data = nil
      @bus.subscribe("/global/test") do |message|
        data = message.data
      end

      @bus.publish("/global/test", "test")
      wait_for(1000) { data }

      data.must_equal "test"
    end

    it "should exception if publishing restricted messages to user" do
      assert_raises(MessageBus::InvalidMessage) do
        @bus.publish("/global/test", "test", user_ids: [1])
      end
    end

    it "should exception if publishing restricted messages to group" do
      assert_raises(MessageBus::InvalidMessage) do
        @bus.publish("/global/test", "test", user_ids: [1])
      end
    end

    it "should raise if we publish to an empty group or user list" do
      assert_raises(MessageBus::InvalidMessageTarget) do
        @bus.publish "/foo", "bar", user_ids: []
      end

      assert_raises(MessageBus::InvalidMessageTarget) do
        @bus.publish "/foo", "bar", group_ids: []
      end

      assert_raises(MessageBus::InvalidMessageTarget) do
        @bus.publish "/foo", "bar", client_ids: []
      end

      assert_raises(MessageBus::InvalidMessageTarget) do
        @bus.publish "/foo", "bar", group_ids: [], user_ids: [1]
      end

      assert_raises(MessageBus::InvalidMessageTarget) do
        @bus.publish "/foo", "bar", group_ids: [1], user_ids: []
      end
    end
  end

  it "should support forking properly" do
    test_never :memory

    data = []
    @bus.subscribe do |msg|
      data << msg.data
    end

    @bus.publish("/hello", "pre-fork")
    wait_for(2000) { data.length == 1 }

    if child = Process.fork
      # The child was forked and we received its PID

      # Wait for fork to finish so we're asserting that we can still publish after it has
      Process.wait(child)

      @bus.publish("/hello", "continuation")
    else
      begin
        @bus.after_fork
        GC.start
        @bus.publish("/hello", "from-fork")
      ensure
        exit!(0)
      end
    end

    wait_for(2000) { data.length == 3 }

    @bus.publish("/hello", "after-fork")

    wait_for(2000) { data.length == 4 }

    data.must_equal(["pre-fork", "from-fork", "continuation", "after-fork"])
  end

  describe '#register_client_message_filter' do
    it 'should register the message filter correctly' do
      @bus.register_client_message_filter('/test')

      @bus.client_message_filters.must_equal([])

      @bus.register_client_message_filter('/test') { puts "hello world" }

      channel, blk = @bus.client_message_filters[0]

      blk.must_respond_to(:call)
      channel.must_equal('/test')
    end
  end

  # === Context for test generation ===
  # MessageBus::Implementation
  # Key state: @last_message (Time), @subscribed (bool), @destroyed (bool)
  # Keepalive: timer fires every keepalive_interval; when
  #   Time.now - @last_message > keepalive_interval * 3
  #   → logs warning + calls backend_instance.request_reconnect
  #   → resets @last_message to avoid cascade during reconnect window
  # Reconnect: backend#global_subscribe wraps in rescue/retry; request_reconnect
  #   closes the underlying socket so the blocked read raises and retry fires.
  # Backend contract: request_reconnect is a no-op on Base; Postgres closes the
  #   PG subscribe connection; Redis disconnects the subscriber connection.
  # =====================================
  describe "keepalive recovery" do
    FAST_KEEPALIVE = 0.5

    before do
      @original_min_keepalive = MessageBus::Implementation::MIN_KEEPALIVE
      MessageBus::Implementation.const_set(:MIN_KEEPALIVE, 0)

      @log_output = StringIO.new
      @bus.configure(keepalive_interval: FAST_KEEPALIVE, logger: Logger.new(@log_output))
    end

    after do
      MessageBus::Implementation.const_set(:MIN_KEEPALIVE, @original_min_keepalive)
    end

    # Simulates a half-open connection by replacing global_subscribe with a
    # blocking loop that never yields messages. The loop exits when the bus's
    # keepalive watchdog fires request_reconnect or when global_unsubscribe sets
    # @subscribed = false (teardown path).
    #
    # @param backend [MessageBus::Backend::Base] the backend to patch
    # @param resume_after_stall [Boolean] whether to delegate to the real
    #   global_subscribe after the stall clears, restoring message flow
    # @return [Array(Proc, Proc)] two zero-argument callables:
    #   - first returns true once the keepalive watchdog has fired request_reconnect
    #   - second returns true once the fake stall has exited and @subscribed has
    #     been cleared, meaning any subsequent subscribed=true is from the real backend
    def simulate_stuck_subscriber(backend, resume_after_stall: true)
      real_gs = backend.method(:global_subscribe)
      reconnect_fired = false
      stall_exited = false

      backend.define_singleton_method(:request_reconnect) do
        reconnect_fired = true
      end

      backend.define_singleton_method(:global_subscribe) do |last_id = nil, &blk|
        @subscribed = true
        # 1ms sleep so the subscriber thread reacts within ~1ms of reconnect_fired
        loop { sleep 0.001; break if reconnect_fired || !@subscribed }

        should_resume = reconnect_fired && @subscribed && resume_after_stall

        backend.singleton_class.remove_method(:global_subscribe)
        backend.singleton_class.remove_method(:request_reconnect)

        if should_resume
          @subscribed = false
          stall_exited = true  # set after @subscribed=false, before real_gs
          real_gs.call(last_id, &blk)
        else
          stall_exited = true
        end
      end

      [-> { reconnect_fired }, -> { stall_exited }]
    end

    it "recovers message delivery after a stuck subscriber" do
      reconnect_fired, stall_exited = simulate_stuck_subscriber(@bus.backend_instance)

      @bus.after_fork
      wait_for(1000) { @bus.listening? }

      received = []
      @bus.subscribe("/recovery-test") { |msg| received << msg.data }

      # Wait for watchdog to fire, then for the fake stall to fully exit
      # (stall_exited is set after @subscribed=false), then for the real
      # backend to confirm it is subscribed before publishing.
      wait_for(4000) { reconnect_fired.call }
      wait_for(2000) { stall_exited.call }
      wait_for(2000) { @bus.backend_instance.subscribed }

      @bus.publish("/recovery-test", "post-recovery")

      wait_for(2000) { received.include?("post-recovery") }

      received.must_include("post-recovery")
      @bus.listening?.must_equal true
    end

    it "logs a warning when the keepalive detects a stuck subscriber" do
      _reconnect_fired, _stall_exited = simulate_stuck_subscriber(@bus.backend_instance, resume_after_stall: false)

      @bus.after_fork
      wait_for(1000) { @bus.listening? }

      wait_for(4000) { @log_output.string.include?("no longer functioning correctly") }

      @log_output.string.must_include "timed out"
      @log_output.string.must_include "no longer functioning correctly"
    end

    it "exits cleanly when destroy is called during a stall" do
      _reconnect_fired, _stall_exited = simulate_stuck_subscriber(@bus.backend_instance, resume_after_stall: false)

      @bus.after_fork
      wait_for(1000) { @bus.listening? }

      @bus.destroy
      @bus.listening?.must_equal false
    end

    it "resets @last_message so the watchdog does not cascade during reconnect" do
      reconnect_count = 0
      backend = @bus.backend_instance
      real_gs = backend.method(:global_subscribe)

      backend.define_singleton_method(:request_reconnect) do
        reconnect_count += 1
      end

      backend.define_singleton_method(:global_subscribe) do |last_id = nil, &blk|
        @subscribed = true
        loop { sleep 0.05; break if reconnect_count > 0 || !@subscribed }
        backend.singleton_class.remove_method(:global_subscribe)
        backend.singleton_class.remove_method(:request_reconnect)
        @subscribed = false
        real_gs.call(last_id, &blk) if reconnect_count > 0
      end

      @bus.after_fork
      wait_for(1000) { @bus.listening? }

      # Wait for the first reconnect to fire, then let two more keepalive cycles
      # pass. The @last_message reset in the bus must prevent a second request.
      wait_for(4000) { reconnect_count > 0 }
      sleep(FAST_KEEPALIVE * 3)

      reconnect_count.must_equal 1
    end

    it "does not crash the timer thread when request_reconnect raises" do
      backend = @bus.backend_instance
      real_gs = backend.method(:global_subscribe)
      error_raised = false

      backend.define_singleton_method(:request_reconnect) do
        error_raised = true
        raise IOError, "simulated connection error"
      end

      # The IOError is raised in the timer thread and caught by its on_error
      # handler. The subscriber loop still exits via error_raised flag because
      # the assignment happens before the raise.
      backend.define_singleton_method(:global_subscribe) do |last_id = nil, &blk|
        @subscribed = true
        loop { sleep 0.05; break if error_raised || !@subscribed }
        backend.singleton_class.remove_method(:global_subscribe)
        backend.singleton_class.remove_method(:request_reconnect)
        @subscribed = false
        real_gs.call(last_id, &blk)
      end

      @bus.after_fork
      wait_for(1000) { @bus.listening? }

      # Timer on_error logs "Failed to process job: <error message>"
      wait_for(4000) { @log_output.string.include?("Failed to process job") }

      @bus.listening?.must_equal true
      @log_output.string.must_include "simulated connection error"
    end
  end
end
