'use strict';

/**
 * Minimal, spec-compliant Server-Sent Events parser.
 * https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
 *
 * Feed it decoded text chunks with push(); it calls
 *   onEvent({ event, data, id })  for every dispatched event
 *   onComment(text)               for every comment line (":" prefix; Sensibo heartbeats)
 */
class SSEParser {
    constructor({ onEvent, onComment, onRetry } = {}) {
        this.onEvent = onEvent || (() => {});
        this.onComment = onComment || (() => {});
        this.onRetry = onRetry || (() => {});
        this.reset();
    }

    reset() {
        this.buffer = '';
        this.skipLF = false;
        this.eventType = '';
        this.dataLines = [];
        this.lastEventId = '';
    }

    push(chunk) {
        let text = chunk;
        if (this.skipLF && text.length > 0) {
            if (text[0] === '\n') text = text.slice(1);
            this.skipLF = false;
        }
        this.buffer += text;

        let start = 0;
        for (let i = 0; i < this.buffer.length; i++) {
            const ch = this.buffer[i];
            if (ch !== '\n' && ch !== '\r') continue;
            this._line(this.buffer.slice(start, i));
            if (ch === '\r') {
                if (i + 1 < this.buffer.length) {
                    if (this.buffer[i + 1] === '\n') i++;
                } else {
                    // CR at the end of the chunk: a following LF belongs to it.
                    this.skipLF = true;
                }
            }
            start = i + 1;
        }
        this.buffer = this.buffer.slice(start);
    }

    _line(line) {
        if (line === '') {
            this._dispatch();
            return;
        }
        if (line[0] === ':') {
            this.onComment(line.slice(1).replace(/^ /, ''));
            return;
        }
        const colon = line.indexOf(':');
        let field, value;
        if (colon === -1) {
            field = line;
            value = '';
        } else {
            field = line.slice(0, colon);
            value = line.slice(colon + 1);
            if (value[0] === ' ') value = value.slice(1);
        }
        switch (field) {
            case 'event':
                this.eventType = value;
                break;
            case 'data':
                this.dataLines.push(value);
                break;
            case 'id':
                if (!value.includes('\0')) this.lastEventId = value;
                break;
            case 'retry':
                if (/^\d+$/.test(value)) this.onRetry(parseInt(value, 10));
                break;
            default:
                break; // ignore unknown fields
        }
    }

    _dispatch() {
        if (this.dataLines.length === 0) {
            this.eventType = '';
            return;
        }
        const evt = {
            event: this.eventType || 'message',
            data: this.dataLines.join('\n'),
            id: this.lastEventId
        };
        this.eventType = '';
        this.dataLines = [];
        this.onEvent(evt);
    }
}

module.exports = { SSEParser };
