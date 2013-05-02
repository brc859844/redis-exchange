# RabbitMQ Redis Exchange

Latest tagged version works with (or more specifically has been tested with) RabbitMQ 3.1.0, Redis 2.6.12, and 
eredis Redis Erlang client v1.0.4. This version will not work with earlier versions of RabbitMQ.


This is a custom exchange type for RabbitMQ that will publish any message sent to it onto Redis, using the 
routing key for the Redis channel name. Any Redis clients subscribed to the channel will receive the messages.

The code (and this README file) have been shamelessly adapted from https://github.com/jbrisbin/riak-exchange.

## Installation

To install from source:

    git clone https://github.com/brc859844/redis-exchange
    cd redis-exchange
    make deps
    make
    make package
    cp deps/*.ez $RABBITMQ_HOME/plugins
    cp dist/*.ez $RABBITMQ_HOME/plugins

Next, enable the plugin:

    rabbitmq-plugins enable rabbit_exchange_type_redis


## Configuration

To use the Redis exchange type, declare your exchange as type "x-redis". In addition to forwarding messages to 
Redis, this also acts like a regular exchange so you can have consumers bound to this exchange and they will 
receive the messages as well as the messages going to Redis.

To configure what Redis server to connect to, pass some arguments to the exchange declaration:

* `host` - Hostname or IP of the Riak server to connect to.
* `port` - Port number of the Riak server to connect to.
* `maxclients` - The maximum number of clients to create in the pool (use more clients for higher-traffic exchanges).

The Redis exchange can act like any valid RabbitMQ exchange type (direct, fanout, topic, etc). Set an argument on 
your exchange when you declare it named `type_module` and give it a valid RabbitMQ exchange type module. You can use 
the ones built in that come with RabbitMQ, or you can use any custom ones you or a third party have written. Typical
exchnage types and the corresponding type module names are as follows:

* `direct`: `rabbit_exchange_type_direct`
* `fanout`: `rabbit_exchange_type_fanout`
* `topic`: `rabbit_exchange_type_topic`

If you don't specify anything, the exchange will default to a topic exchange.


