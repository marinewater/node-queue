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
 * @param {string} queue name of the queue
 * @param {*} job data which will be stored in the queue
 * @param {Number} user_id user id
 * @param {Number} [priority=3] set priority of job; has to be in the range of 1 to 5, 1 is fastest, 5 is slowest
 * @returns {Promise}
 */
Queue.prototype.enqueue = function(queue, job, user_id, priority) {
    var redis = this.redis;
    var app_prefix = this.app_prefix;

    return new Promise(function (resolve, reject) {
        user_id = parseInt(user_id);
        if (isNaN(user_id)) {
            reject('user id has to be a number');
        }

        priority = parseInt(priority);
        if (isNaN(priority) || priority < 1 || priority > 5) {
            priority = 3;
        }

        redis.incr(app_prefix + queue + ':id', function(err, id) {
            var job_data = JSON.stringify({
                data: job,
                user_id: user_id,
                priority: priority,
                id: id
            });

            redis.set(app_prefix + queue + ':jobs:' + id, job_data, function() {
                var score = Date.now() + (priority * 1000000000000000);

                redis.zadd(app_prefix + queue + ':user:' + user_id.toString(), score, id, function() {
                    redis.zadd(app_prefix + queue + ':queue', score, id, function (err, reply) {
                        if (err) {
                            reject(err);
                        }
                        else {
                            resolve(reply);
                        }
                    });
                })
            });
        })
    });
};

/**
 *
 * @param {string} queue name of the queue
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

        multi.zrange(app_prefix + queue + ':queue', 0, limit-1);

        multi.zremrangebyrank(app_prefix + queue + ':queue', 0, limit-1);

        multi.exec(function(err, job_ids) {
            if (err) {
                reject(err);
            }
            else {
                var multi2 = redis.multi();

                var reply = [];

                var job_list = [];

                job_ids[0].forEach(function(job_id) {
                    job_list.push(app_prefix + queue + ':jobs:' + job_id);
                });

                multi2.mget(job_list);
                multi2.del(job_list);


                multi2.exec(function(err, jobs) {
                    jobs[0].forEach(function(result) {
                        var job_data = JSON.parse(result);
                        reply.push(job_data.data);

                        redis.zrem(app_prefix + queue + ':user:' + job_data.user_id.toString(), job_data.id, function() {

                        });
                    });
                    resolve(reply);
                });
            }
        });
    });
};

/**
 * returns the number of jobs in the queue
 * @param {string} queue name of the queue
 * @returns {Promise}
 */
Queue.prototype.countJobs = function(queue) {
    var redis = this.redis;
    var app_prefix = this.app_prefix;

    return new Promise(function (resolve, reject) {
        redis.zcount(app_prefix + queue + ':queue', '-inf', '+inf', function(err, reply) {
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
 * get the position of the next job for the specified user
 * @param {string} queue name of the queue
 * @param {Number} user_id user id
 */
Queue.prototype.getPosition = function(queue, user_id) {
    var redis = this.redis;
    var app_prefix = this.app_prefix;

    return new Promise(function (resolve, reject) {
        user_id = parseInt(user_id);
        if (isNaN(user_id)) {
            reject('user id has to be a number');
        }

        redis.zrange(app_prefix + queue + ':user:' + user_id.toString(), 0, 0, function (reject, job_id) {
            redis.zrank(app_prefix + queue + ':queue', job_id[0], function(reject, position) {
                if (position === null) {
                    resolve(null);
                }
                else {
                    resolve(position+1);
                }
            });
        });
    });
};