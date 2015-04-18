var assert = require('assert');
var sinon = require('sinon');

var redis = require('redis');

var lib_redis = require('../lib/redis');

describe('Redis', function(){
    describe('RedisOptionsFactory', function(){
        it('should return default options when no parameter is passed', function() {
            var options_redis = new lib_redis.RedisOptionsFactory();

            assert.equal(options_redis.host, 'localhost');
            assert.equal(options_redis.port, 6379);
            assert.deepEqual(options_redis.options, {});
        });

        it('should set options when provided', function() {
            var options_redis = new lib_redis.RedisOptionsFactory({
                host: 'some.host',
                port: 12345,
                options: {
                    deep: 'space',
                    five: 4,
                    three: 'two',
                    1: 'go'
                }
            });

            assert.equal(options_redis.host, 'some.host');
            assert.equal(options_redis.port, 12345);
            assert.deepEqual(options_redis.options, {
                deep: 'space',
                five: 4,
                three: 'two',
                1: 'go'
            });
        });
    });

    describe('redis', function() {
        before(function() {
            sinon.stub(redis, "createClient");
        });

        it('should create redis client', function() {
            lib_redis.redis({
                host: 'some.host',
                port: 12345,
                options: {
                    deep: 'space',
                    five: 4,
                    three: 'two',
                    1: 'go'
                }
            });

            assert(redis.createClient.calledWith(12345, 'some.host', {
                deep: 'space',
                five: 4,
                three: 'two',
                1: 'go'
            }));
        });

        after(function() {
            redis.createClient.restore();
        });
    });
});