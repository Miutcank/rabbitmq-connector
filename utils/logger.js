var config = require('../config').logger;
var bunyan = require('bunyan');
module.exports = function createLogger(){
	var log = bunyan.createLogger({
		name: 'rabbitmq-connector',
		streams: [
		{
			type: 'raw',
			stream: require('bunyan-logstash-tcp').createStream({
				host: config.logStash.host,
				port: config.logStash.port,
				tags: ['bunyan', 'rabbitmq-connector']
			})
			.on('error', function func(err) {
				console.error('[rabbitmq-connector] Error in bunyan-logstash-tcp stream');
				console.error(err);
			})
		},
		{
			stream: process.stdout
		}
		]
	});
	return log;
};
