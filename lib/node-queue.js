var Redis = require('./redis').redis;
var Promise = require("bluebird");

exports = module.exports = Queue;

function Queue(options) {
    options = options || {};

    this.app_prefix = options.prefix || 'ncsq';
    this.app_prefix += ':';

    if (options.redis_client) {
        this.redis = options.redis_client;
    }
    else {
        this.redis = new Redis(options.redis);
    }

    this.redis.on("error", function (err) {
        console.log("Error: " + err);
    });
}

/**
 * add a job to the queue
 * @param {string} [queue] name of the queue
 * @param {*} [job] data which will be stored in the queue
 * @returns {Promise}
 */
Queue.prototype.enqueue = function(queue, job) {
    var redis = this.redis;
    var app_prefix = this.app_prefix;

    return new Promise(function (resolve, reject) {
        redis.rpush(app_prefix + queue + ':jobs', JSON.stringify(job), function (err, reply) {
            if (err) {
                reject(err);
            }
            else {
                resolve(reply);
            }
        });
    });
};

/**
 *
 * @param {string} [queue] name of the queue
 * @param {number} [limit=1] amount of jobs that will be retrieved
 * @returns {Promise}
 */
Queue.prototype.dequeue = function(queue, limit) {
    limit = parseInt(limit);
    if (isNaN(limit)) {
        limit = 1;
    }

    var redis = this.redis;
    var app_prefix = this.app_prefix;

    return new Promise(function (resolve, reject) {
        var multi = redis.multi();

        multi.lrange(app_prefix + queue + ':jobs', 0, limit-1);

        multi.ltrim(app_prefix + queue + ':jobs', limit, -1);

        multi.exec(function(err, results) {
            if (err) {
                reject(err);
            }
            else {
                var reply = [];

                results[0].forEach(function(result) {
                    reply.push(JSON.parse(result));
                });

                resolve(reply);
            }
        });
    });
};

/**
 * returns the number of jobs in the queue
 * @param {string} [queue] name of the queue
 * @returns {Promise}
 */
Queue.prototype.countJobs = function(queue) {
    var redis = this.redis;
    var app_prefix = this.app_prefix;

    return new Promise(function (resolve, reject) {
        redis.llen(app_prefix + queue + ':jobs', function(err, reply) {
            if (err) {
                reject(err);
            }
            else {
                resolve(reply);
            }
        });
    });
};