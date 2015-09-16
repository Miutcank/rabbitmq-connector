var config = require('./config');
var amqplib = require('amqplib');
var log = require('./utils/logger')();
var Promise = require('bluebird');
var RABBIT_URI = config.rabbitMQ.uri;
var PUBLISHER = 'publisher';

var connector = module.exports = function rabbitMqConnectorConstructor(env) {

	connector.channels = [];
	if (env) {
		config.env = env;
	}

	function connect() {
		if (connector.connection) {
			return new Promise(function giveConnectorBack(resolve) {
				resolve(connector.connection);
			});
		}

		log.info({ url: RABBIT_URI }, 'Connecting to RabbitMQ...');

		return amqplib.connect(RABBIT_URI)
			.tap(function cacheConnectionAndLogSuccessfulConnection(connection) {
				connector.connection = connection;
				log.info({ url: RABBIT_URI }, 'Connection to RabbitMQ established.');
			})
			.catch(function logFailedConnection(error) {
				log.error({ url: RABBIT_URI, error: error }, 'Connection to RabbitMQ failed.');
				throw error;
			});
	}

	function getChannel(id) {
		return connect()
			.then(function giveChannelBack(connection) {
				if (connector.channels[id]) {
					return connector.channels[id];
				} else {
					return createChannel(connection, id);
				}
			})
	}

	function createChannel(connection, id) {
		return connection.createChannel()
			.tap(function cacheChannel(channel) {
				connector.channels[id] = channel;
				log.info({ channelId: id }, 'RabbitMQ channel established.');
			})
			.catch(function logFailedChannelCreation(error) {
				log.error({ url: RABBIT_URI, error: error }, 'RabbitMQ Channel creation failed.');
				throw error;
			});
	}

	function checkChannel(queueId) {
		if (!connector.channels[queueId]) {
			log.error('AMQP Channel not found');
			throw Error('Channel not found');
		}
	}

	connector.createReadChannel = function createReadChannel(exchangeId, queueId) {
		var exchangeName = config.env + '.' + exchangeId;
		var queueName = config.env + '.' + queueId;
		return getChannel(queueId)
			.then(function setPrefetchLimit(channel) {
				var prefetchPromise = channel.prefetch(config.rabbitMQ.prefetchCount, false); // throttling
				return [channel, prefetchPromise];
			})
			.spread(function assertExchange(channel) {
				var assertExchangePromise = channel.assertExchange(exchangeName, 'fanout', {durable: true});
				log.info({ exchange: exchangeName }, 'Assert channel exchange');
				return [channel, assertExchangePromise];
			})
			.spread(function assertQueue(channel) {
				var assertQueuePromise = channel.assertQueue(queueName, {exclusive: false});
				log.info({ queue: queueName }, 'Asserting queue exists.');
				return [channel, assertQueuePromise];
			})
			.spread(function bindQueue(channel, queryIsOk) {
				connector.channels[queueId] = channel;
				var res = channel.bindQueue(queryIsOk.queue, exchangeName, '');
				log.info({ queryIsOk: queryIsOk, queueId: queueId }, 'Binding to queue.');
				return [queryIsOk.queue, res, channel];
			})
			.spread(function returnReadChannel(queue, res, channel) {
				log.info({ queue: queue, res: res }, 'Returning read channel.');
				return [channel, queue];
			})
			.catch(function logError(error) {
				log.info({ error: error }, 'Creating channel failed');
				throw error;
			});
	};

	connector.consume = function consume(exchangeId, queueId, messageHandler) {
		return connector.createReadChannel(exchangeId, queueId)
			.spread(function consumeMessages(channel, queue) {
				log.info({ queueId: queueId }, 'Trying to read queue.');
				return channel.consume(queue, messageHandler, { noAck: false });
			})
			.tap(function logConsume() {
				log.info({ queueId: queueId }, 'Waiting for queue.');
			})
			.catch(function logError(error) {
				log.info({ error: error },
					'An error occured while consuming from the queue');
				throw error;
			});
	};

	connector.acknowledge = function acknowledge(queueId, message) {
		log.debug('acknowledging', {queueId: queueId});
        checkChannel(queueId);
        connector.channels[queueId].ack(message);
    };

    connector.reject = function reject(queueId, message) {
		log.error('rejecting', {queueId: queueId});
        checkChannel(queueId);
        connector.channels[queueId].nack(message);
    };

	connector.publish = function publish(exchangeId, data){
		exchangeId = config.env + '.' + exchangeId;
		return getChannel(PUBLISHER)
			.then(function assertExchange(channel){
				channel.assertExchange(exchangeId, 'fanout', {durable: true});
				return channel;
			})
			.then(function publishMessage(channel){
				channel.publish(exchangeId, '', new Buffer(JSON.stringify(data)));
				log.debug({exchangeId: exchangeId, data: data}, 'RabbitMQ message published');
			})
			.catch(function handleError(e){
				log.error({exchangeId: exchangeId, data: data, err: e}, 'Publishing to rabbitMQ failed');
				throw e;
			});
	};

	getChannel(PUBLISHER);
	return connector;
};
