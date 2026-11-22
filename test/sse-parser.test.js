'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { SSEParser } = require('../lib/sse-parser');

function collect() {
    const events = [], comments = [];
    const p = new SSEParser({ onEvent: (e) => events.push(e), onComment: (c) => comments.push(c) });
    return { p, events, comments };
}

test('parses the documented Sensibo example', () => {
    const { p, events } = collect();
    p.push('event: hello\ndata: {"version":1,"heartbeatSeconds":120,"pods":2}\n\n' +
        'event: changed\ndata: {"id":"AbCd1234","fields":["acState"]}\n\n');
    assert.deepStrictEqual(events.map((e) => e.event), ['hello', 'changed']);
    assert.deepStrictEqual(JSON.parse(events[1].data), { id: 'AbCd1234', fields: ['acState'] });
});

test('handles chunks split anywhere, CRLF and CR split across chunks', () => {
    const { p, events } = collect();
    const raw = 'event: measurements\r\ndata: {"id":"x"}\r\n\r\n';
    for (const ch of raw) p.push(ch);
    p.push('data: a\r');
    p.push('\ndata: b\r');
    p.push('\r');
    assert.strictEqual(events.length, 2);
    assert.strictEqual(events[0].event, 'measurements');
    assert.strictEqual(events[1].event, 'message');
    assert.strictEqual(events[1].data, 'a\nb');
});

test('heartbeat comments are reported and do not dispatch events', () => {
    const { p, events, comments } = collect();
    p.push(':\n: keepalive\n\n');
    assert.strictEqual(events.length, 0);
    assert.deepStrictEqual(comments, ['', 'keepalive']);
});
