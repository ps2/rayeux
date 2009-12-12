# Rayeux: A ruby client to communicate with a comet server using Bayeux.

Provides client side functionality to talk to a Bayeux server such as the cometd server in Jetty.

Rayeux is heavily based on the javascript cometd library available here: http://cometdproject.dojotoolkit.org/

## Install & Use:

* sudo gem install rayeux -s http://gemcutter.org
* See the chat_example.rb file for an example client that will talk to the chat demo included with Jetty (http://www.mortbay.org/jetty/)

## Dependencies

* httpclient - http://github.com/nahi/httpclient

## Todo
 
* Support ack extension
* Port to eventmachine-httpclient for a cleaner event driven model

## Patches/Pull Requests

* This library currently pretty rough around the edges.  Would love to know about any improvements or bug fixes.
* Feedback and comments are very welcome.

## Copyright

Copyright (c) 2009 Pete Schwamb. See LICENSE for details.
