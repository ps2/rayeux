#!/usr/bin/env ruby
require 'rubygems'
require 'rayeux'

class ChatClient
  def initialize(url, name)
    @name = name
    @client = Rayeux::Client.new(url)
    @connected = false
    #@client.set_log_level('debug')
    
    @client.add_listener('/meta/handshake') do |m|
      @connected = false
    end

    @client.add_listener('/meta/connect') do |m|
      meta_connect(m)
    end
  end
  
  def run
    @client.process_messages
  end

  def meta_connect(m)
    was_connected = @connected
    @connected = m["successful"]
    if was_connected
      if @connected
        # Normal operation, a long poll that reconnects
      else
        # Disconnected
        puts "Disconnected!"
      end
    else
      if @connected
        # Connected
        puts "Connected!"
        subscribe
      else
        # Could not connect
        puts "Could not connect!"
      end
    end
  end
  
  def received_chat_message(from, text)
    puts "Got chat demo message from #{from}: #{text}"
    if text == 'ping'
      send_chat_message('pong')
    end
  end
  
  def send_chat_message(text)
    @client.publish("/chat/demo", {
      :user => @name,
      :chat => text
    })
  end
  
  def subscribe
    @client.subscribe("/chat/demo") do |m|
      if m["data"].is_a?(Hash)
        received_chat_message(m["data"]["user"], m["data"]["chat"])
      end
    end
    
    @client.publish("/chat/demo", {
      :user => @name,
      :join => true,
      :chat => "#{@name} has joined"
    })
  end

end

client = ChatClient.new('http://localhost:8080/cometd/cometd', "rayeux")
client.run
