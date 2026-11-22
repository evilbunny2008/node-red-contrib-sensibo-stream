'use strict';
const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const { SensiboAccount } = require('../lib/sensibo-account');

const KEY = 'test-key';

function startServer(handler) {
    return new Promise((resolve) => {
        const server = http.createServer(handler);
        server.listen(0, '127.0.0.1', () => {
            resolve({ server, baseUrl: `http://127.0.0.1:${server.address().port}/api/v2` });
        });
    });
}

function once(emitter, name, pred = () => true, timeout = 3000) {
    return new Promise((resolve, reject) => {
        const t = setTimeout(() => { emitter.off(name, fn); reject(new Error('timeout waiting for ' + name)); }, timeout);
        function fn(v) { if (pred(v)) { clearTimeout(t); emitter.off(name, fn); resolve(v); } }
        emitter.on(name, fn);
    });
}

const POD = { id: 'AbCd1234', room: { name: 'Bedroom' }, acState: { on: false }, measurements: { temperature: 20, humidity: 40 } };

function api(req, res, state) {
    const url = new URL(req.url, 'http://x');
    // Like the real API: REST endpoints only accept the apiKey query parameter.
    if (url.searchParams.get('apiKey') !== KEY) {
        res.statusCode = 401;
        return res.end(JSON.stringify({ status: 'error', reason: 'Unauthorized' }));
    }
    if (url.pathname === '/api/v2/users/me/pods') {
        state.podsCalls = (state.podsCalls || 0) + 1;
        res.setHeader('Content-Type', 'application/json');
        return res.end(JSON.stringify({ status: 'success', result: [POD] }));
    }
    if (url.pathname === '/api/v2/pods/AbCd1234') {
        state.podCalls = (state.podCalls || 0) + 1;
        res.setHeader('Content-Type', 'application/json');
        return res.end(JSON.stringify({ status: 'success', result: Object.assign({}, POD, { acState: { on: true } }) }));
    }
    return false;
}

test('stream: hello -> initial load, measurements merged, changed -> device re-read', async () => {
    const state = {};
    let streamRes;
    const { server, baseUrl } = await startServer((req, res) => {
        if (req.url.startsWith('/api/v2/stream')) {
            assert.strictEqual(req.headers['x-api-key'], KEY);
            assert.ok(!req.url.includes('apiKey'), 'key must not be in the URL');
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            streamRes = res;
            res.write('event: hello\ndata: {"version":1,"heartbeatSeconds":120,"pods":1}\n\n');
            return;
        }
        if (api(req, res, state) === false) { res.statusCode = 404; res.end(); }
    });
    const acct = new SensiboAccount({ apiKey: KEY, baseUrl });
    try {
        const loaded = once(acct, 'device', (d) => d.reason === 'hello');
        acct.start();
        const first = await loaded;
        assert.strictEqual(first.device.room.name, 'Bedroom');

        const meas = once(acct, 'event', (e) => e.event === 'measurements');
        streamRes.write(': heartbeat\n\nevent: measurements\ndata: {"id":"AbCd1234","measurements":{"temperature":23.4}}\n\n');
        const m = await meas;
        assert.strictEqual(m.data.measurements.temperature, 23.4);
        assert.strictEqual(acct.getDevice('AbCd1234').measurements.temperature, 23.4);
        assert.strictEqual(acct.getDevice('AbCd1234').measurements.humidity, 40, 'merge keeps other fields');

        const changed = once(acct, 'device', (d) => d.reason === 'changed');
        streamRes.write('event: changed\ndata: {"id":"AbCd1234","fields":["acState"]}\n\n');
        const d = await changed;
        assert.deepStrictEqual(d.fields, ['acState']);
        assert.strictEqual(d.device.acState.on, true);
    } finally {
        acct.stop();
        server.closeAllConnections();
        server.close();
    }
});

test('stream: resync and server close trigger reconnect', async () => {
    let connects = 0;
    const { server, baseUrl } = await startServer((req, res) => {
        if (req.url.startsWith('/api/v2/stream')) {
            connects++;
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write('event: hello\ndata: {"version":1,"heartbeatSeconds":120,"pods":1}\n\n');
            if (connects === 1) res.write('event: resync\ndata: {}\n\n');
            else if (connects === 2) res.end();
            return;
        }
        if (api(req, res, {}) === false) { res.statusCode = 404; res.end(); }
    });
    const acct = new SensiboAccount({ apiKey: KEY, baseUrl, resyncDelayMs: 10, backoffBaseMs: 10, jitterMs: 10 });
    try {
        const resync = once(acct, 'reconnecting', (r) => r.reason === 'resync');
        acct.start();
        await resync;
        await once(acct, 'reconnecting', (r) => r.reason === 'stream closed');
        await once(acct, 'status', (s) => s.state === 'connected' && connects === 3);
    } finally {
        acct.stop();
        server.closeAllConnections();
        server.close();
    }
});

test('stream: 429 honours Retry-After', async () => {
    const { server, baseUrl } = await startServer((req, res) => {
        res.writeHead(429, { 'Retry-After': '7' });
        res.end('slow down');
    });
    const acct = new SensiboAccount({ apiKey: KEY, baseUrl, backoffBaseMs: 10, jitterMs: 0 });
    try {
        const r = once(acct, 'reconnecting');
        acct.start();
        const { delay } = await r;
        assert.ok(delay >= 7000, `delay ${delay} should be >= Retry-After`);
        assert.strictEqual(acct.state, 'unavailable');
    } finally {
        acct.stop();
        server.closeAllConnections();
        server.close();
    }
});

test('stream: heartbeat watchdog reconnects after two silent intervals', async () => {
    let connects = 0;
    const { server, baseUrl } = await startServer((req, res) => {
        if (req.url.startsWith('/api/v2/stream')) {
            connects++;
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write('event: hello\ndata: {"version":1,"heartbeatSeconds":0.01,"pods":0}\n\n');
            return; // then go silent
        }
        if (api(req, res, {}) === false) { res.statusCode = 404; res.end(); }
    });
    const acct = new SensiboAccount({ apiKey: KEY, baseUrl, backoffBaseMs: 10, jitterMs: 0, heartbeatGraceMs: 50 });
    try {
        const r = once(acct, 'reconnecting', (x) => x.reason === 'heartbeat timeout');
        acct.start();
        await r;
    } finally {
        acct.stop();
        server.closeAllConnections();
        server.close();
    }
});

test('backoff grows and is capped', async () => {
    const { server, baseUrl } = await startServer((req, res) => { res.statusCode = 500; res.end(); });
    const acct = new SensiboAccount({ apiKey: KEY, baseUrl, backoffBaseMs: 1000, backoffMaxMs: 4000, jitterMs: 0 });
    const delays = [];
    acct.on('reconnecting', ({ delay }) => delays.push(delay));
    // drive the failure path directly without waiting real time
    acct.running = true;
    for (let i = 0; i < 5; i++) acct._scheduleReconnect({ reason: 'x' });
    acct.stop();
    server.close();
    assert.deepStrictEqual(delays, [1000, 2000, 4000, 4000, 4000]);
});

test('stream unavailable at start-up still loads devices once over REST', async () => {
    const state = {};
    const { server, baseUrl } = await startServer((req, res) => {
        if (req.url.startsWith('/api/v2/stream')) { res.writeHead(503, { 'Retry-After': '60' }); return res.end(); }
        if (api(req, res, state) === false) { res.statusCode = 404; res.end(); }
    });
    const acct = new SensiboAccount({ apiKey: KEY, baseUrl, jitterMs: 0 });
    try {
        const d = once(acct, 'device', (x) => x.reason === 'startup');
        acct.start();
        assert.strictEqual((await d).id, 'AbCd1234');
        assert.strictEqual(acct.loaded, true);
        assert.deepStrictEqual(acct.getLoadedDevices().map((x) => x.id), ['AbCd1234']);
        assert.strictEqual(state.podsCalls, 1);
    } finally {
        acct.stop();
        server.closeAllConnections();
        server.close();
    }
});
