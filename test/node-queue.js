var assert = require('assert');
var sinon = require('sinon');

var Queue = require('../lib/node-queue');

describe('node-redis', function() {
    describe('enqueue', function() {
        it('should add a job to the queue', function(done) {
            var rpushMock = {
                rpush: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(rpushMock);

            mockClient.expects('rpush').once().withArgs('ncsq:test-queue:jobs', JSON.stringify('test-job')).yields(null, 'test');
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: rpushMock
            });
            queue.enqueue('test-queue', 'test-job').then(function() {
                mockClient.verify();
                mockClient.restore();
                done();
            }).catch(function() {
                mockClient.restore();
                done("promise should not be rejected");
            });
        });

        it('should fail to add a job to the queue', function(done) {
            var rpushMock = {
                rpush: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(rpushMock);

            mockClient.expects('rpush').once().withArgs('ncsq:test-queue:jobs', JSON.stringify('test-job')).yields('err', null);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: rpushMock
            });
            queue.enqueue('test-queue', 'test-job').then(function() {
                mockClient.restore();
                done("promise should not be resolved");
            }).catch(function() {
                mockClient.verify();
                mockClient.restore();
                done();
            });
        });
    });

    describe('dequeue', function() {
        it('should pop a job from the queue', function (done) {
            var multiMock = {
                lrange: function () {},
                ltrim: function () {},
                exec: function() {}
            };
            var mockClient1 = sinon.mock(multiMock);

            mockClient1.expects('lrange').once().withArgs('ncsq:test-queue:jobs', 0);
            mockClient1.expects('ltrim').once().withArgs('ncsq:test-queue:jobs', 1, -1);
            mockClient1.expects('exec').once().yields(null, [[JSON.stringify('test')], 'OK']);


            var redisMock = {
                multi: function() { return multiMock; },
                on: function() {}
            };

            var mockClient2 = sinon.mock(redisMock);
            mockClient2.expects('on');


            var queue = new Queue({
                redis_client: redisMock
            });
            queue.dequeue('test-queue', 'test-job').then(function (reply) {
                mockClient1.verify();
                mockClient1.restore();
                mockClient2.restore();
                assert.equal(reply[0], 'test');
                done();
            }).catch(function () {
                mockClient1.restore();
                mockClient2.restore();
                done("promise should not be rejected");
            });
        });

        it('should fail to pop a job from the queue', function (done) {
            var multiMock = {
                lrange: function () {},
                ltrim: function () {},
                exec: function() {}
            };
            var mockClient1 = sinon.mock(multiMock);

            mockClient1.expects('lrange').once().withArgs('ncsq:test-queue:jobs', 0);
            mockClient1.expects('ltrim').once().withArgs('ncsq:test-queue:jobs', 1, -1);
            mockClient1.expects('exec').once().yields('err', null);


            var redisMock = {
                multi: function() { return multiMock; },
                on: function() {}
            };

            var mockClient2 = sinon.mock(redisMock);
            mockClient2.expects('on');


            var queue = new Queue({
                redis_client: redisMock
            });
            queue.dequeue('test-queue', 'test-job').then(function (reply) {
                mockClient1.verify();
                mockClient1.restore();
                mockClient2.restore();
                done("promise should not be resolved");
            }).catch(function () {
                mockClient1.restore();
                mockClient2.restore();
                done();
            });
        });
    });

    describe('countJobs', function() {
        it('should return the number of jobs in the queue', function(done) {
            var llenMock = {
                llen: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(llenMock);

            mockClient.expects('llen').once().withArgs('ncsq:test-queue:jobs').yields(null, 2);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: llenMock
            });
            queue.countJobs('test-queue').then(function(job_count) {
                assert.deepEqual(job_count, 2);

                mockClient.verify();
                mockClient.restore();

                done();
            }).catch(function() {
                mockClient.restore();
                done("promise should not be rejected");
            });
        });
    });

    describe('Queue', function() {
        it('should create a queue and change the prefix', function () {
            var redisMock = {
                on: function() {}
            };

            var mockClient = sinon.mock(redisMock);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: redisMock,
                prefix: 'prefix'
            });

            assert.equal(queue.app_prefix, 'prefix:');
        });

        it('should create a queue and use the default prefix', function () {
            var redisMock = {
                on: function() {}
            };

            var mockClient = sinon.mock(redisMock);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: redisMock
            });

            assert.equal(queue.app_prefix, 'ncsq:');
        });
    });
});