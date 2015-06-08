var assert = require('assert');
var sinon = require('sinon');

var Queue = require('../lib/node-queue');

describe('node-redis', function() {
    describe('enqueue', function() {
        it('should add a job to the queue', function(done) {
            var redisMock = {
                incr: function () {},
                set: function () {},
                zadd: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(redisMock);

            var job_data = JSON.stringify({
                data: {job: 'done'},
                user_id: 567,
                priority: 3,
                id: 123
            });

            mockClient.expects('incr').once().withArgs('ncsq:test-queue:id').yields(null, 123);
            mockClient.expects('set').once().withArgs('ncsq:test-queue:jobs:123', job_data).yields(null, 123);
            mockClient.expects('zadd').once().withArgs('ncsq:test-queue:user:567', sinon.match.number, 123).yields(null, null);
            mockClient.expects('zadd').once().withArgs('ncsq:test-queue:queue', sinon.match.number, 123).yields(null, null);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: redisMock
            });
            queue.enqueue('test-queue', {job: 'done'}, 567).then(function() {
                mockClient.verify();
                mockClient.restore();
                done();
            }).catch(function(err) {
                mockClient.restore();
                done(err);
            });
        });
        it('should add a job to the queue with different priority', function(done) {
            var redisMock = {
                incr: function () {},
                set: function () {},
                zadd: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(redisMock);

            var job_data = JSON.stringify({
                data: {job: 'done'},
                user_id: 567,
                priority: 5,
                id: 123
            });

            mockClient.expects('incr').once().withArgs('ncsq:test-queue:id').yields(null, 123);
            mockClient.expects('set').once().withArgs('ncsq:test-queue:jobs:123', job_data).yields(null, 123);
            mockClient.expects('zadd').once().withArgs('ncsq:test-queue:user:567', sinon.match.number, 123).yields(null, null);
            mockClient.expects('zadd').once().withArgs('ncsq:test-queue:queue', sinon.match.number, 123).yields(null, null);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: redisMock
            });
            queue.enqueue('test-queue', {job: 'done'}, 567, 5).then(function() {
                mockClient.verify();
                mockClient.restore();
                done();
            }).catch(function(err) {
                mockClient.restore();
                done(err);
            });
        });

        it('should fail to add a job to the queue', function(done) {
            var redisMock = {
                incr: function () {},
                set: function () {},
                zadd: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(redisMock);

            var job_data = JSON.stringify({
                data: {job: 'done'},
                user_id: 567,
                priority: 5,
                id: 123
            });

            mockClient.expects('incr').once().withArgs('ncsq:test-queue:id').yields('err', null);
            mockClient.expects('set').once().withArgs('ncsq:test-queue:jobs:123', job_data).yields(null, 123);
            mockClient.expects('zadd').once().withArgs('ncsq:test-queue:user:567', sinon.match.number, 123).yields(null, null);
            mockClient.expects('zadd').once().withArgs('ncsq:test-queue:queue', sinon.match.number, 123).yields(null, null);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: redisMock
            });
            queue.enqueue('test-queue', {job: 'done'}, 567, 5).then(function() {
                mockClient.verify();
                mockClient.restore();
                done('should fail');
            }).catch(function(err) {
                mockClient.restore();
                done();
            });
        });
    });

    describe('dequeue', function() {
        it('should pop a job from the queue', function (done) {
            var multiMock1 = {
                zrange: function () {},
                zremrangebyrank: function () {},
                mget: function () {},
                del: function () {},
                exec: function() {}
            };
            var mockClient1 = sinon.mock(multiMock1);

            mockClient1.expects('zrange').once().withArgs('ncsq:test-queue:queue', 0, 4);
            mockClient1.expects('zremrangebyrank').once().withArgs('ncsq:test-queue:queue', 0, 4);
            mockClient1.expects('exec').once().yields(null, [[3, 5], 'OK']);


            var multiMock2 = {
                mget: function () {},
                del: function () {},
                exec: function() {}
            };
            var mockClient3 = sinon.mock(multiMock2);

            mockClient3.expects('mget').once().withArgs([
                'ncsq:test-queue:jobs:3',
                'ncsq:test-queue:jobs:5'
            ]);
            mockClient3.expects('del').once().withArgs([
                'ncsq:test-queue:jobs:3',
                'ncsq:test-queue:jobs:5'
            ]);

            mockClient3.expects('exec').once().yields(null, [[
                JSON.stringify({
                    data: {job: 'done1'},
                    user_id: 567,
                    priority: 3,
                    id: 3
                }),
                JSON.stringify({
                    data: {job: 'done2'},
                    user_id: 890,
                    priority: 4,
                    id: 5
                })
            ], 'OK']);


            var redisMock = {
                multi: function() {},
                zrem: function() {},
                on: function() {}
            };

            var mockClient2 = sinon.mock(redisMock);

            var multi = mockClient2.expects('multi').twice().onCall(0).returns(multiMock1).onCall(1).returns(multiMock2);
            mockClient2.expects('on');
            mockClient2.expects('zrem').once().withArgs('ncsq:test-queue:user:567', 3);
            mockClient2.expects('zrem').once().withArgs('ncsq:test-queue:user:890', 5);


            var queue = new Queue({
                redis_client: redisMock
            });

            queue.dequeue('test-queue', 5).then(function (reply) {
                mockClient1.verify();
                mockClient1.restore();
                mockClient2.restore();
                assert.deepEqual(reply[0], {job: 'done1'});
                assert.deepEqual(reply[1], {job: 'done2'});
                done();
            }).catch(function (err) {
                mockClient1.restore();
                mockClient2.restore();
                done(err);
            });
        });
    });

    describe('countJobs', function() {
        it('should return the number of jobs in the queue', function(done) {
            var zcountMock = {
                zcount: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(zcountMock);

            mockClient.expects('zcount').once().withArgs('ncsq:test-queue:queue', '-inf', '+inf').yields(null, 2);
            mockClient.expects('on');


            var queue = new Queue({
                redis_client: zcountMock
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

    describe('getPosition', function() {
        it('get the first position of a job for a user id', function(done) {
            var redisMock = {
                zrange: function () {},
                zrank: function () {},
                on: function() {}
            };
            var mockClient = sinon.mock(redisMock);

            mockClient.expects('zrange').once().withArgs('ncsq:test-queue:user:123', 0, 0).yields(null, [2]);
            mockClient.expects('zrank').once().withArgs('ncsq:test-queue:queue', 2).yields(null, 5);
            mockClient.expects('on');

            var queue = new Queue({
                redis_client: redisMock
            });
            queue.getPosition('test-queue', 123).then(function(position) {
                assert.deepEqual(position, 6);

                mockClient.verify();
                mockClient.restore();

                done();
            }).catch(function(err) {
                mockClient.restore();
                done(err);
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