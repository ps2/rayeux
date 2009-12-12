#!/usr/bin/env ruby
require "json"
require 'httpclient'

module Rayeux
  class Transport
    
    def initialize(type, http_client)
      @in_queue = Queue.new
      @out_queue = Queue.new
      @num_threads = 2 # one for the long poll, another for requests
      @type = type
      @envelopes = []
      @http = http_client
      start_threads
    end

    def get_type
      return @type
    end
    
    def get_response
      @out_queue.pop
    end
    
    def start_threads
      @num_threads.times do
        Thread.new do
          loop do
            begin
              envelope = @in_queue.pop
              @out_queue.push(transport_send(envelope))
            rescue
              puts "Transport exception: " + e.message + " " + e.backtrace.join("\n")
            end
          end
        end
      end
    end

    def set_timeout(seconds)
      @http.receive_timeout = seconds
    end

    def send_msg(envelope, longpoll)
      @in_queue.push(envelope)
    end
    
    def complete(request, success, longpoll)
      puts "** Completing #{request[:id]} longpoll=#{longpoll.inspect}"
      #if longpoll
      #  longpoll_complete(request)
      #else
      #  internal_complete(request, success)
      #end
    end
    
    private

    def abort
      requests.each do |request|
        # TODO
        debug('Aborting request', request)
        #  if (request.xhr)
        #      request.xhr.abort();
        #}
        #if (_longpollRequest)
        #{
        #    debug('Aborting request ', _longpollRequest);
        #    if (_longpollRequest.xhr) _longpollRequest.xhr.abort();
        #}
        #_longpollRequest = nil;
        #_requests = [];
        #_envelopes = [];
      end
    end
  end
    
  class LongPollingTransport < Transport
    private 
    def transport_send(envelope)
      begin
        if envelope[:sleep]
          sleep envelope[:sleep] / 1000.0
        end
        headers = {'Content-Type' => 'text/json;charset=UTF-8', 'X-Requested-With' => 'XMLHttpRequest'}
        resp = @http.post(envelope[:url], envelope[:messages].to_json, headers)
        envelope[:success] = resp.status == 200
        envelope[:response] = resp
        envelope[:reason] = resp.reason
        #if resp.status != 200
        #  envelope[:on_failure].call(request, resp.reason, nil)
        #else
        #  envelope[:on_success].call(request, resp)
        #end
      rescue Exception => e
        envelope[:success] = false
        envelope[:reason] = e.message
      end
      envelope
    end
  end
  
  class CallbackPollingTransport < Transport
    def transport_send(envelope)
      raise "Not Implemented"
    end
  end

  class Client
    def initialize(configuration, name = nil, handshake_props = nil)
      @message_id = 0
      @name = name || 'default'
      @log_level = 'warn' # 'warn','info','debug'
      @status = 'disconnected'
      @client_id = nil
      @batch = 0
      @message_queue = []
      @listeners = {}
      @backoff = 0
      @scheduled_send = nil
      @extensions = []
      @advice = {}
      @reestablish = false
      @scheduled_send = nil
      
      configure(configuration)
      handshake(handshake_props)
    end
    
    def process_messages
      while envelope = @transport.get_response
        #puts "Received: #{envelope.inspect}"
        if envelope[:success]
          envelope[:on_success].call(envelope[:response])
        else
          envelope[:on_failure].call(envelope[:reason], nil)
        end
      end
    end
    
    # Adds a listener for bayeux messages, performing the given callback in the given scope
    # when a message for the given channel arrives.
    # channel: the channel the listener is interested to
    # callback: the callback to call when a message is sent to the channel
    # returns the subscription handle to be passed to {@link #removeListener(object)}
    # @see #removeListener(object)
    def add_listener(channel, &block)
      internal_add_listener(channel, block, false)
    end

    # Find the extension registered with the given name.
    # @param name the name of the extension to find
    # @return the extension found or null if no extension with the given name has been registered
    def get_extension(name)
      @extensions.find {|e| e[:name] == name }
    end
    
    # Returns a string representing the status of the bayeux communication with the comet server.
    def get_status
      @status
    end
    
    # Starts a the batch of messages to be sent in a single request.
    # see end_batch(send_messages)
    def start_batch
      @batch += 1
    end

    # Ends the batch of messages to be sent in a single request,
    # optionally sending messages present in the message queue depending
    # on the given argument.
    # send_messages: whether to send the messages in the queue or not
    # see start_batch
    def end_batch(send_messages = true)
      @batch -= 1
      batch = 0 if @batch < 0
      if send_messages && @batch == 0 && !is_disconnected
        messages = @message_queue
        message_queue = []
        if messages.size > 0 
          internal_send(messages, false)
        end
      end
    end    
  
    def next_message_id
      @message_id += 1
    end
    
    # Subscribes to the given channel, calling the passed block
    # when a message for the channel arrives.
    # channel: the channel to subscribe to
    # subscribe_props: an object to be merged with the subscribe message
    # block: the block to call when a message is sent to the channel
    # returns the subscription handle to be passed to #unsubscribe(object)
    def subscribe(channel, subscribe_props = {}, &block)
      # Only send the message to the server if this clientId has not yet subscribed to the channel
      do_send = !has_subscriptions(channel)

      subscription = internal_add_listener(channel, block, true)

      if do_send
        # Send the subscription message after the subscription registration to avoid
        # races where the server would send a message to the subscribers, but here
        # on the client the subscription has not been added yet to the data structures
        bayeux_message = {
          :channel      => '/meta/subscribe',
          :subscription => channel
        }
        message = subscribe_props.merge(bayeux_message)
        queue_send(message)
      end

      return subscription
    end

    def has_subscriptions(channel)
      subscriptions = @listeners[channel] || []
      !subscriptions.empty?
    end

    # Unsubscribes the subscription obtained with a call to {@link #subscribe(string, object, function)}.
    # subscription: the subscription to unsubscribe.
    def unsubscribe(subscription, unsubscribe_props)
      # Remove the local listener before sending the message
      # This ensures that if the server fails, this client does not get notifications
      remove_listener(subscription)

      channel = subscription[0]
      # Only send the message to the server if this client_id unsubscribes the last subscription
      if !has_subscriptions(channel)
        bayeux_message = {
          :channel => '/meta/unsubscribe',
          :subscription => channel
        }
        message = unsubscribe_props.merge(bayeux_message)
        queue_send(message)
      end
    end

    # Publishes a message on the given channel, containing the given content.
    # @param channel the channel to publish the message to
    # @param content the content of the message
    # @param publishProps an object to be merged with the publish message
    def publish(channel, content, publish_props = {})
      bayeux_message = {
          :channel => channel,
          :data    => content
      }
      queue_send(publish_props.merge(bayeux_message))
    end
    
    # Sets the log level for console logging.
    # Valid values are the strings 'error', 'warn', 'info' and 'debug', from
    # less verbose to more verbose.
    # @param level the log level string
    def set_log_level(level)
      @log_level = level
    end
    
    private

    def internal_add_listener(channel, callback, is_subscription)
      # The data structure is a map<channel, subscription[]>, where each subscription
      # holds the callback to be called and its scope.

      subscription = {
          :callback => callback,
          :subscription => is_subscription == true
      };

      subscriptions = @listeners[channel]
      if !subscriptions
        subscriptions = []
        @listeners[channel] = subscriptions
      end
      
      subscriptions.push(subscription)
      subscription_id = subscriptions.size

      debug('internal_add_listener', channel, callback, subscription_id)

      # The subscription to allow removal of the listener is made of the channel and the index
      [channel, subscription_id]
    end

    # Removes the subscription obtained with a call to {@link #addListener(string, object, function)}.
    # @param subscription the subscription to unsubscribe.    
    def remove_listener(subscription)
      internal_remove_listener(subscription)
    end

    def internal_remove_listener(subscription)
      subscriptions = @listeners[subscription[0]]
      if subscriptions
        subscriptions.delete_at(subscription[1])
        debug('rm listener', subscription)
      end
    end

    # Removes all listeners registered with add_listener(channel, scope, callback) or
    # subscribe(channel, scope, callback).
    def clear_listeners
      @listeners = {}
    end

    # Removes all subscriptions added via {@link #subscribe(channel, scope, callback, subscribeProps)},
    # but does not remove the listeners added via {@link add_listener(channel, scope, callback)}.
    def clear_subscriptions
      internal_clear_subscriptions
    end

    def clear_subscriptions
      @listeners.each do |channel,subscriptions|
        debug('rm subscriptions', channel, subscriptions)
        subscriptions.delete_if {|s| s[:subscription]}
      end
    end

    
    # Sets the backoff period used to increase the backoff time when retrying an unsuccessful or failed message.
    # Default value is 1 second, which means if there is a persistent failure the retries will happen
    # after 1 second, then after 2 seconds, then after 3 seconds, etc. So for example with 15 seconds of
    # elapsed time, there will be 5 retries (at 1, 3, 6, 10 and 15 seconds elapsed).
    # @param period the backoff period to set
    # @see #getBackoffIncrement()
    
    def set_backoff_increment(period)
      @backoff_increment = period
    end

    # Returns the backoff period used to increase the backoff time when retrying an unsuccessful or failed message.
    # @see #setBackoffIncrement(period)
    def get_backoff_increment
      @backoff_increment
    end

    # Returns the backoff period to wait before retrying an unsuccessful or failed message.
    def get_backoff_period
      @backoff
    end
    
    def increase_backoff
      if @backoff < @max_backoff
        @backoff += @backoff_increment
      end
    end

    # Registers an extension whose callbacks are called for every incoming message
    # (that comes from the server to this client implementation) and for every
    # outgoing message (that originates from this client implementation for the
    # server).
    # The format of the extension object is the following:
    # <pre>
    # {
    #     incoming: function(message) { ... },
    #     outgoing: function(message) { ... }
    # }
    # </pre>
    # Both properties are optional, but if they are present they will be called
    # respectively for each incoming message and for each outgoing message.
    # @param name the name of the extension
    # @param extension the extension to register
    # @return true if the extension was registered, false otherwise
    # @see #unregisterExtension(name)

    def register_extension(name, extension)
      existing = extensions.any? {|e| e[:name] == name}
      
      if !existing
        @extensions.push({
          :name => name,
          :extension => extension
        })
        debug('Registered extension', name)

        # Callback for extensions
        if extension[:registered] 
          extension[:registered].call(extension, name, this)
        end

        true
      else
        debug('Could not register extension with name \'{}\': another extension with the same name already exists');
        false
      end
    end

    
    # Unregister an extension previously registered with
    # {@link #registerExtension(name, extension)}.
    # @param name the name of the extension to unregister.
    # @return true if the extension was unregistered, false otherwise
    def unregister_extension(name)
      unregistered = false
      @extensions.delete_if do |extension|
        if extension[:name] == name
          unregistered = true
          if extension[:unregistered]
            extension[:unregistered].call(extension)
          end
          true
        end
      end
      unregistered
    end

    def next_message_id
      @message_id += 1
      @message_id - 1
    end

    
    # Converts the given response into an array of bayeux messages
    # @param response the response to convert
    # @return an array of bayeux messages obtained by converting the response
    
    def convert_to_messages(response)
      json = JSON.parse(response.content)
    end

    def set_status(new_status)
      debug('status',@status,'->',new_status);
      @status = new_status
    end

    def is_disconnected
      @status == 'disconnecting' || @status == 'disconnected'
    end
    
    def handshake(handshake_props, delay=nil)
      debug 'handshake'
      @client_id = nil

      clear_subscriptions

      # Start a batch.
      # This is needed because handshake and connect are async.
      # It may happen that the application calls init() then subscribe()
      # and the subscribe message is sent before the connect message, if
      # the subscribe message is not held until the connect message is sent.
      # So here we start a batch to hold temporarly any message until
      # the connection is fully established.
      @batch = 0;
      start_batch

      handshake_props ||= {}

      bayeux_message = {
          :version => '1.0',
          :minimumVersion => '0.9',
          :channel => '/meta/handshake',
          :supportedConnectionTypes => ['long-polling', 'callback-polling']
      }
    
      # Do not allow the user to mess with the required properties,
      # so merge first the user properties and *then* the bayeux message
      message = handshake_props.merge(bayeux_message)

      # We started a batch to hold the application messages,
      # so here we must bypass it and send immediately.
      set_status('handshaking')
      debug('handshake send',message)
      internal_send([message], false, delay)
    end
  
    def configure(configuration)
      debug('configure cometd ', configuration)
      # Support old style param, where only the comet URL was passed
      if configuration.is_a?(String) || configuration.is_a?(URI::HTTP)
        configuration = { :url => configuration }
      end
    
      configuration ||= {}

      @url = configuration[:url]
      if @url.nil?
        raise "Missing required configuration parameter 'url' specifying the comet server URL"
      end
  
      @http = configuration[:http_client] || HTTPClient.new
        
      @backoff_increment = configuration[:backoff_increment] || 1000
      @max_backoff = configuration[:max_backoff] || 60000
      @log_level = configuration[:log_level] || 'info'
      @reverse_incoming_extensions = configuration[:reverse_incoming_extensions] != false

      # Temporary setup a transport to send the initial handshake
      # The transport may be changed as a result of handshake
      @transport = new_long_polling_transport
      debug('transport', @transport)
    end
  
    def new_long_polling_transport
      LongPollingTransport.new("long-polling", @http)
    end
  
    def internal_send(messages, long_poll, delay = nil)
      # We must be sure that the messages have a clientId.
      # This is not guaranteed since the handshake may take time to return
      # (and hence the clientId is not known yet) and the application
      # may create other messages.
      messages = messages.map do |message|
        message['id'] = next_message_id
        message['clientId'] = @client_id if @client_id
        message = apply_outgoing_extensions(message)
      end
      messages.compact!
    
      return if messages.empty?
    
      success_callback = lambda do |response|
        begin
          handle_response(response, long_poll)
        rescue Exception => x
          warn("handle_response exception", x, x.backtrace.join("\n") )
        end
      end
    
      failure_callback = lambda do |reason, exception|
        begin
          handle_failure(messages, reason, exception, long_poll)
        rescue Exception => x
          warn("handle_failure exception: ", x.inspect, x.backtrace.join("\n"))
        end
      end

      #var self = this;
      envelope = {
        :url      => @url,
        :sleep    => delay,
        :messages => messages,
        :on_success => success_callback,
        :on_failure => failure_callback
      }
      debug('internal_send', envelope)
      @transport.send_msg(envelope, long_poll)
    end
  
    def debug(msg, *rest)
      if @log_level == 'debug'
        puts msg + ": " + rest.map {|r| r.inspect} .join(", ")
      end
    end
    
    def warn(msg, *rest)
      puts "Warning: " + msg.inspect + ": " + rest.map {|r| r.inspect} .join(", ")
    end
  
    def receive(message)
      if message["advice"]
        @advice = message["advice"]
      end

      channel = message["channel"]
      case channel
        when '/meta/handshake'
          handshake_response(message)
        when '/meta/connect'
          connect_response(message)
        when '/meta/disconnect'
          disconnect_response(message)
        when '/meta/subscribe'
          subscribe_response(message)
        when '/meta/unsubscribe'
          unsubscribe_response(message)
        else
          message_response(message)
      end
    end

    def handle_response(response, longpoll)
      messages = convert_to_messages(response)
      debug('Received', messages)

      messages = messages.map {|m| apply_incoming_extensions(m) }
      messages.compact!      
      messages.each {|m| receive(m)}
    end
    
    def apply_incoming_extensions(message)
      @extensions.each do |extension|
        callback = extension[:extension][:incoming]
        if (callback)
          message = apply_extension(extension[:name], callback, message)
        end
      end
      message
    end

    def apply_outgoing_extensions(message)
      @extensions.each do |extension|
        callback = extension[:extension][:outgoing]
        if (callback)
          message = apply_extension(extension[:name], callback, message)
        end
      end
      message
    end

    def apply_extension(name, callback, message)
      begin
        message = callback.call(message)
      rescue Exception => e
        warn("extension exception", e.message, e.backtrace.join("\n"))
      end
      message
    end

    def handle_failure(messages, reason, exception, longpoll)

      debug('Failed', messages)

      messages.each do |message|
        channel = message[:channel]
        puts "processing failed message on channel: #{channel}"
        case channel
        when '/meta/handshake'
          handshake_failure(message)
        when '/meta/connect'
          connect_failure(message)
        when '/meta/disconnect'
          disconnect_failure(message)
        when '/meta/subscribe'
          subscribe_failure(message)
        when '/meta/unsubscribe'
          unsubscribe_failure(message)
        else
          message_failure(message)
        end
      end
    end

    def handshake_response(message)
      if message["successful"]
        # Save clientId, figure out transport, then follow the advice to connect
        @client_id = message["clientId"]

        new_transport = find_transport(message)
        if new_transport.nil?
          raise 'Could not agree on transport with server'
        elsif @transport.get_type != new_transport.get_type
          debug('transport', @transport, '->', new_transport)
          @transport = new_transport
        end

        # Notify the listeners
        # Here the new transport is in place, as well as the clientId, so
        # the listener can perform a publish() if it wants, and the listeners
        # are notified before the connect below.
        message[:reestablish] = @reestablish
        @reestablish = true
        notify_listeners('/meta/handshake', message)

        action = @advice["reconnect"] || 'retry'
        if action == 'retry'
          delayed_connect
        end
        
      else
        should_retry = !is_disconnected && (@advice["reconnect"] != 'none')
        if !should_retry 
          set_status('disconnected')
        end

        notify_listeners('/meta/handshake', message)
        notify_listeners('/meta/unsuccessful', message)

        # Only try again if we haven't been disconnected and
        # the advice permits us to retry the handshake
        if should_retry
          increase_backoff
          delayed_handshake
        end
      end
    end

    def handshake_failure(message)
      # Notify listeners
      failure_message = {
          :successful => false,
          :failure    => true,
          :channel    => '/meta/handshake',
          :request    => message,
          :advice     => {
            :action   => 'retry',
            :interval => @backoff
          }
      }

      should_retry = !is_disconnected && @advice["reconnect"] != 'none'
      if !should_retry
        set_status('disconnected')
      end

      notify_listeners('/meta/handshake', failure_message)
      notify_listeners('/meta/unsuccessful', failure_message)

      # Only try again if we haven't been disconnected and the
      # advice permits us to try again
      if should_retry
        increase_backoff
        delayed_handshake
      end
    end
    
    def find_transport(handshake_response)
      transport_types = handshake_response["supportedConnectionTypes"]
      # Check if we can keep long-polling
      if transport_types.include?('long-polling')
        @transport
      elsif transportTypes.include?('callback-polling')
        new_callback_polling_transport
      else
        nil
      end
    end
    
    def delayed_handshake
      set_status('handshaking')
      handshake(@handshake_props, get_next_delay)
    end

    def delayed_connect
      set_status('connecting')
      internal_connect(get_next_delay)
    end
    
    def get_next_delay
      delay = @backoff
      if @advice["interval"] && @advice["interval"].to_f > 0
        delay += @advice["interval"]
      end
      delay
    end
    
    def internal_connect(delay = nil)
      debug('connect')
      message = {
        :channel => '/meta/connect',
        :connectionType => @transport.get_type
      }
      set_status('connecting')
      internal_send([message], true, delay)
      set_status('connected')
    end

    def queue_send(message)
      if (@batch > 0)
        @message_queue.push(message)
      else
        internal_send([message], false)
      end
    end

    def connect_response(message)
      action = is_disconnected ? 'none' : (@advice["reconnect"] || 'retry')
      if !is_disconnected
        set_status(action == 'retry' ? 'connecting' : 'disconnecting')
      end
      
      if @advice["timeout"]
        # Set transport level timeout to comet timeout + 10 seconds
        @transport.set_timeout(@advice["timeout"].to_i / 1000.0 + 10)
      end

      if message["successful"]
        # End the batch and allow held messages from the application
        # to go to the server (see _handshake() where we start the batch).
        # The batch is ended before notifying the listeners, so that
        # listeners can batch other cometd operations
        end_batch(true)

        # Notify the listeners after the status change but before the next connect
        notify_listeners('/meta/connect', message)

        # Connect was successful.
        # Normally, the advice will say "reconnect: 'retry', interval: 0"
        # and the server will hold the request, so when a response returns
        # we immediately call the server again (long polling)
        case action
          when 'retry':
            reset_backoff
            delayed_connect
          else
            reset_backoff
            set_status('disconnected')
        end
        
      else
        # Notify the listeners after the status change but before the next action
        notify_listeners('/meta/connect', message)
        notify_listeners('/meta/unsuccessful', message)

        # Connect was not successful.
        # This may happen when the server crashed, the current clientId
        # will be invalid, and the server will ask to handshake again
        case action
          when 'retry'
            increase_backoff
            delayed_connect
          when 'handshake'
            # End the batch but do not send the messages until we connect successfully
            end_batch(false)
            reset_backoff
            delayed_handshake
          when 'none':
            reset_backoff
            set_status('disconnected')
        end
      end
    end

    def connect_failure(message)
      debug("connect failure", message)
      
      # Notify listeners
      failure_message = {
        :successful => false,
        :failure    => true,
        :channel    => '/meta/connect',
        :request    => message,
        :advice     => {
          :action   => 'retry',
          :interval => @backoff
        }
      }
      notify_listeners('/meta/connect', failure_message)
      notify_listeners('/meta/unsuccessful', failure_message)

      if !is_disconnected
        action = @advice["reconnect"] ? @advice["reconnect"] : 'retry'
        case action
          when 'retry'
            increase_backoff
            delayed_connect
          when 'handshake'
            reset_backoff
            delayed_handshake
          when 'none'
            reset_backoff
          else
            debug('Unrecognized action', action)
        end
      end
    end

    def disconnect_response(message)
      if message["successful"]
        disconnect(false)
        notify_listeners('/meta/disconnect', message)
      else
        disconnect(true)
        notify_listeners('/meta/disconnect', message)
        notify_listeners('/meta/usuccessful', message)
      end
    end

    def disconnect(abort)
      cancel_delayed_send
      if abort
        @transport.abort
      end
      @client_id = nil
      set_status('disconnected')
      @batch = 0
      @messageQueue = []
      reset_backoff
    end

    def disconnect_failure(message)
      disconnect(true)

      failure_message = {
        :successful => false,
        :failure    => true,
        :channel    => '/meta/disconnect',
        :request    => message,
        :advice     => {
          :action   => 'none',
          :interval => 0
        }
      }
      notify_listeners('/meta/disconnect', failure_message)
      notify_listeners('/meta/unsuccessful', failure_message)
    end

    def subscribe_response(message)
      if message["successful"]
        notify_listeners('/meta/subscribe', message)
      else
        notify_listeners('/meta/subscribe', message)
        notify_listeners('/meta/unsuccessful', message)
      end
    end

    def subscribe_failure(message)
      failure_message = {
        :successful => false,
        :failure    => true,
        :channel    => '/meta/subscribe',
        :request    => message,
        :advice     => {
          :action   => 'none',
          :interval => 0
        }
      }
      notify_listeners('/meta/subscribe', failure_message)
      notify_listeners('/meta/unsuccessful', failure_message)
    end

    def unsubscribe_response(message)
      if message["successful"]
        notify_listeners('/meta/unsubscribe', message)
      else
        notify_listeners('/meta/unsubscribe', message)
        notify_listeners('/meta/unsuccessful', message)
      end
    end    

    def unsubscribe_failure(message)
      failure_message = {
        :successful => false,
        :failure    => true,
        :channel    => '/meta/unsubscribe',
        :request    => message,
        :advice     => {
          :action   => 'none',
          :interval => 0
        }
      }
      notify_listeners('/meta/unsubscribe', failure_message)
      notify_listeners('/meta/unsuccessful', failure_message)
    end

    def message_response(message)
      if message["successful"].nil?
        if message["data"]
          # It is a plain message, and not a bayeux meta message
          notify_listeners(message["channel"], message)
        else
          debug('Unknown message', message)
        end
      else
        if message["successful"]
          notify_listeners('/meta/publish', message)
        else
          notify_listeners('/meta/publish', message)
          notify_listeners('/meta/unsuccessful', message)
        end
      end
    end

    def message_failure(message)
      failure_message = {
        :successful => false,
        :failure    => true,
        :channel    => message["channel"],
        :request    => message,
        :advice => {
          :action   => 'none',
          :interval => 0
        }
      }
      notify_listeners('/meta/publish', failure_message)
      notify_listeners('/meta/unsuccessful', failure_message)
    end

    def notify_listeners(channel, message)
      # Notify direct listeners
      notify(channel, message)

      # Notify the globbing listeners
      channel_parts = channel.split("/");
      last = channel_parts.size - 1;
      last.downto(0) do |i|
        channel_part = channel_parts.slice(0, i).join('/') + '/*';
        # We don't want to notify /foo/* if the channel is /foo/bar/baz,
        # so we stop at the first non recursive globbing
        if (i == last) 
          notify(channel_part, message)
        end
        # Add the recursive globber and notify
        channel_part += '*'
        notify(channel_part, message)
      end
    end

    def notify(channel, message)
      subscriptions = @listeners[channel] || []
      subscriptions.compact.each do |subscription|
        begin
          subscription[:callback].call(message);
        rescue Exception => e
          warn(subscription,message,e)
        end
      end
    end
    
    def reset_backoff
      @backoff = 0
    end
    
  end
end

