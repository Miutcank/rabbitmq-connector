var config = require('./config');
var amqplib = require('amqplib');
var log = require('./utils/logger')();
var Promise = require('bluebird');
var RABBIT_URI = config.rabbitMQ.uri;

var connector = module.exports = function rabbitMqConnectorConstructor(){
	connector.channels = [];

	// connect
	connector.connect = function connect() {
		if (connector.connection) {
			return new Promise(function giveConnectorBack(resolve) {
				resolve(connector.connection);
			});
		}

		log.info({ url: RABBIT_URI }, 'Connecting to RabbitMQ');

		return amqplib.connect(RABBIT_URI)
			.tap(function cacheConnectionAndLogSuccessfulConnection(connection) {
				connector.connection = connection;
				log.info('Connection to RabbitMQ established.');
			})
			.catch(function logFailedConnection(error) {
				log.error({ url: RABBIT_URI, error: error },
					'Connection to RabbitMQ failed.');
				throw error;
			});
	};

	// publish
	connector.publish = function publish(exchangeId, data){
		exchangeId = config.env + '.' + exchangeId;
		return connector.connect()
			.then(function createChannel(){
				return getChannel();
			})
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

	// create read channel
	connector.createReadChannel = function createReadChannel(exchangeId, queueId) {
		var exchangeName = config.env + '.' + exchangeId;
		var queueName = config.env + '.' + queueId;
		return getChannel()
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

	function getChannel(){
		return new Promise(function giveChannelBack(resolve) {
			resolve(connector.channels[0]);
		});
	}

	function checkChannel(queueId) {
		if (!connector.channels[queueId]) {
			log.error('AMQP Channel not found');
			throw Error('Channel not found');
		}
	}

    connector.acknowledge = function acknowledge(queueId, message) {
        checkChannel(queueId);
        connector.channels[queueId].ack(message);
    };

    connector.reject = function reject(queueId, message) {
        checkChannel(queueId);
        connector.channels[queueId].nack(message);
    };

    // connect and open a channel when opened.
    amqplib.connect(RABBIT_URI)
			.then(function cacheConnectionAndLogSuccessfulConnection(connection) {
				connector.connection = connection;
				log.info('Connection to RabbitMQ established.', {URI: RABBIT_URI});
				return connector.connection.createChannel();
			})
			.then(function cacheChannel(channel){
				connector.channels.push(channel);
				log.info('RabbitMQ channel created successfully');
			})
			.catch(function handleError(e){
				log.error({error: e}, 'Error while connecting to RabbitMQ.');
			});

	return connector;

};
