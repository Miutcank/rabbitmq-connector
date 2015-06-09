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
		},
		{
			stream: process.stdout
		}
		]
	});
	return log;
};
