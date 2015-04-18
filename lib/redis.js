var redis = require('redis');


var redis_exports = {};

/**
 * creates redis client
 * @param {object} [options=] options, see also RedisOptionsFactory
 * @return {*}
 */
redis_exports.redis = function(options) {
    var options_redis = new RedisOptionsFactory(options);
    return redis.createClient(options_redis.port, options_redis.host, options_redis.options);
};

/**
 * make options available
 * @param {object} [options=] redis connection settings, can be an object or empty; object may contain host, port and options (see also node_redis)
 */
function RedisOptionsFactory (options) {
    options = options || {};

    this.host = options.host || 'localhost';
    this.port = options.port || 6379;
    this.options = options.options || {};
};
redis_exports.RedisOptionsFactory = RedisOptionsFactory;


exports = module.exports = redis_exports;