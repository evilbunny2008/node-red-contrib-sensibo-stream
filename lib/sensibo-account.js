'use strict';

const { EventEmitter } = require('events');
const { SSEParser } = require('./sse-parser');
const { SensiboApi, parseRetryAfter, DEFAULT_BASE_URL } = require('./api');

const MIN_POLL_MS = 15 * 60 * 1000;

/**
 * One SensiboAccount = one open GET /stream connection for one API key.
 *
 * Implements the Sensibo "Rules for Clients":
 *  - reconnect with exponential backoff (capped at 5 min) plus jitter
 *  - honour Retry-After on 429 / 503, and keep slow-polling while the stream is down
 *  - treat > 2 heartbeat intervals of silence as a dead connection
 *  - reopen on `resync`
 *  - fetch all devices right after `hello`, and slow-poll every >= 15 min
 *  - merge `measurements` into a cached device object; re-read a device on `changed`
 *
 * Emits:
 *  'status'  { state, text }
 *  'event'   { event, data, id }      every parsed stream event (data already JSON-parsed)
 *  'device'  { id, device, reason, fields }   a device was (re)loaded or updated
 *  'warn'    Error                    non-fatal problems (never 'error', so nothing throws)
 */
class SensiboAccount extends EventEmitter {
    constructor(opts = {}) {
        super();
        this.setMaxListeners(0);
        this.api = opts.api || new SensiboApi({ apiKey: opts.apiKey, baseUrl: opts.baseUrl, fetchImpl: opts.fetchImpl });
        this.baseUrl = (opts.baseUrl || DEFAULT_BASE_URL).replace(/\/+$/, '');
        this.fetch = opts.fetchImpl || globalThis.fetch;

        this.backoffBaseMs = opts.backoffBaseMs ?? 2000;
        this.backoffMaxMs = opts.backoffMaxMs ?? 5 * 60 * 1000;
        this.resyncDelayMs = opts.resyncDelayMs ?? 3000;
        this.jitterMs = opts.jitterMs ?? 3000;
        this.pollIntervalMs = opts.allowFastPoll ? (opts.pollIntervalMs ?? MIN_POLL_MS)
            : Math.max(MIN_POLL_MS, opts.pollIntervalMs ?? MIN_POLL_MS);
        this.fetchOnChange = opts.fetchOnChange !== false;
        this.defaultHeartbeatSeconds = opts.defaultHeartbeatSeconds ?? 120;
        this.heartbeatGraceMs = opts.heartbeatGraceMs ?? 5000;

        this.devices = new Map();
        this.state = 'stopped';
        this.failures = 0;
        this.running = false;
        this._abort = null;
        this._reconnectTimer = null;
        this._watchdog = null;
        this._pollTimer = null;
        this._podFetches = new Map(); // id -> { pending: bool, fields: Set }
        this._loadedIds = new Set();  // devices whose full state came from the REST API
        this.loaded = false;          // at least one full device load succeeded
        this._fallbackLoadDone = false;
    }

    // ---------------------------------------------------------------- lifecycle

    start() {
        if (this.running) return;
        this.running = true;
        this.failures = 0;
        this._fallbackLoadDone = false;
        this._schedulePoll();
        this._connect();
    }

    stop() {
        this.running = false;
        clearTimeout(this._reconnectTimer);
        clearTimeout(this._watchdog);
        clearTimeout(this._pollTimer);
        this._reconnectTimer = this._watchdog = this._pollTimer = null;
        if (this._abort) this._abort.abort();
        this._abort = null;
        this._setStatus('stopped', 'stopped');
    }

    /** Force-close the current stream and reconnect promptly. */
    reconnect() {
        if (!this.running) return;
        this.failures = 0;
        this._closeStream();
        clearTimeout(this._reconnectTimer);
        this._reconnectTimer = setTimeout(() => this._connect(), 0);
    }

    getDevice(id) {
        return this.devices.get(id);
    }

    /** Devices whose full state has been loaded from the REST API (for late subscribers). */
    getLoadedDevices() {
        return Array.from(this._loadedIds).map((id) => this.devices.get(id)).filter(Boolean);
    }

    // ---------------------------------------------------------------- stream

    async _connect() {
        if (!this.running) return;
        this._reconnectTimer = null;
        const abort = new AbortController();
        this._abort = abort;
        this._setStatus('connecting', 'connecting');

        let res;
        try {
            res = await this.fetch(this.baseUrl + '/stream', {
                method: 'GET',
                headers: {
                    'X-Api-Key': this.api.apiKey,
                    'Accept': 'text/event-stream',
                    'Accept-Encoding': 'gzip',
                    'Cache-Control': 'no-cache'
                },
                signal: abort.signal
            });
        } catch (err) {
            if (abort.signal.aborted) return;
            this._emitWarn(err);
            this._scheduleReconnect({ reason: err.message || 'connection failed' });
            return;
        }
        if (abort.signal.aborted || !this.running) {
            cancelBody(res);
            return;
        }

        if (!res.ok) {
            try { await res.text(); } catch (_) {}
            const retryAfter = parseRetryAfter(res.headers.get('retry-after'));
            if (res.status === 429 || res.status === 503) {
                this._setStatus('unavailable', `stream unavailable (HTTP ${res.status}), polling`);
                this._scheduleReconnect({ minDelay: retryAfter, reason: `HTTP ${res.status}`, keepStatus: true });
            } else if (res.status === 401 || res.status === 403) {
                this._setStatus('error', `auth failed (HTTP ${res.status}) - check API key`);
                this._emitWarn(new Error(`Sensibo stream rejected the API key (HTTP ${res.status})`));
                // Do not hammer the API with a bad key: go straight to the max backoff.
                this.failures = 100;
                this._scheduleReconnect({ minDelay: retryAfter, reason: `HTTP ${res.status}`, keepStatus: true });
            } else {
                this._scheduleReconnect({ minDelay: retryAfter, reason: `HTTP ${res.status}` });
            }
            return;
        }

        this._setStatus('connected', 'connected');
        this._heartbeatMs = this.defaultHeartbeatSeconds * 1000;
        this._armWatchdog();

        let resync = false;
        const parser = new SSEParser({
            onComment: () => { /* heartbeat: watchdog already re-armed per chunk */ },
            onEvent: (evt) => {
                if (evt.event === 'resync') resync = true;
                this._handleEvent(evt);
            }
        });

        try {
            const decoder = new TextDecoder('utf-8');
            for await (const chunk of res.body) {
                if (abort.signal.aborted) break;
                this._armWatchdog();
                parser.push(decoder.decode(chunk, { stream: true }));
                if (resync) break;
            }
        } catch (err) {
            if (!abort.signal.aborted) this._emitWarn(err);
        } finally {
            clearTimeout(this._watchdog);
            this._watchdog = null;
            cancelBody(res);
        }

        if (this._abort === abort) this._abort = null;
        if (!this.running) return;
        if (abort.signal.aborted && abort._reason !== 'watchdog') return; // explicit reconnect/stop
        if (resync) {
            this._scheduleReconnect({ fixed: this.resyncDelayMs, reason: 'resync' });
        } else if (abort._reason === 'watchdog') {
            this._scheduleReconnect({ reason: 'heartbeat timeout' });
        } else {
            this._scheduleReconnect({ reason: 'stream closed' });
        }
    }

    _closeStream(reason) {
        if (this._abort) {
            this._abort._reason = reason;
            this._abort.abort();
        }
    }

    _armWatchdog() {
        clearTimeout(this._watchdog);
        // "If no data arrives for more than two heartbeat intervals, close and reconnect."
        const ms = this._heartbeatMs * 2 + this.heartbeatGraceMs;
        this._watchdog = setTimeout(() => {
            this._emitWarn(new Error(`No data from Sensibo stream for ${Math.round(ms / 1000)}s, reconnecting`));
            this._closeStream('watchdog');
        }, ms);
        if (this._watchdog.unref) this._watchdog.unref();
    }

    _scheduleReconnect({ minDelay = 0, fixed, reason, keepStatus } = {}) {
        if (!this.running) return;
        clearTimeout(this._reconnectTimer);
        let delay;
        if (fixed !== undefined) {
            delay = fixed + Math.random() * this.jitterMs;
        } else {
            const exp = Math.min(this.backoffMaxMs, this.backoffBaseMs * Math.pow(2, Math.min(this.failures, 20)));
            delay = Math.min(this.backoffMaxMs, exp + Math.random() * this.jitterMs);
            this.failures++;
        }
        delay = Math.max(delay, minDelay);
        if (!keepStatus) {
            this._setStatus('disconnected', `${reason}, retry in ${Math.round(delay / 1000)}s`);
        }
        // Stream not available at start-up: load the devices once over REST so values
        // are available now instead of after the first slow poll.
        if (!this.loaded && !this._fallbackLoadDone) {
            this._fallbackLoadDone = true;
            this.refresh('startup');
        }
        this._reconnectTimer = setTimeout(() => this._connect(), delay);
        this.emit('reconnecting', { delay, reason });
    }

    // ---------------------------------------------------------------- events

    _handleEvent(evt) {
        let data = evt.data;
        try { data = JSON.parse(evt.data); } catch (_) { /* leave as string */ }
        const out = { event: evt.event, data, id: evt.id };

        switch (evt.event) {
            case 'hello':
                this.failures = 0;
                if (data && data.heartbeatSeconds > 0) {
                    this._heartbeatMs = data.heartbeatSeconds * 1000;
                    this._armWatchdog();
                }
                this._setStatus('connected', `connected (${data && data.pods !== undefined ? data.pods + ' devices' : 'ok'})`);
                this.emit('event', out);
                // Changes may have happened before the stream opened: fetch everything once now.
                this.refresh('hello');
                return;
            case 'measurements':
                this._mergeMeasurements(data);
                break;
            case 'changed':
                this.emit('event', out);
                if (this.fetchOnChange && data && data.id) {
                    this._fetchPod(data.podId || data.id, data.fields);
                }
                return;
            default:
                break;
        }
        this.emit('event', out);
    }

    _mergeMeasurements(data) {
        if (!data || !data.id || !data.measurements) return;
        // For room/window sensors `id` is the sensor and `podId` its device;
        // sensors are cached under their own id.
        const entry = this.devices.get(data.id) || { id: data.id };
        if (data.podId) entry.podId = data.podId;
        entry.measurements = Object.assign({}, entry.measurements, data.measurements);
        this.devices.set(data.id, entry);
    }

    // ---------------------------------------------------------------- REST

    /** Fetch all devices and emit a 'device' event for each. */
    async refresh(reason = 'poll') {
        try {
            const pods = await this.api.getPods();
            if (!Array.isArray(pods)) return [];
            for (const pod of pods) {
                if (!pod || !pod.id) continue;
                this.devices.set(pod.id, pod);
                this._loadedIds.add(pod.id);
                this.emit('device', { id: pod.id, device: pod, reason, fields: [] });
            }
            this.loaded = true;
            return pods;
        } catch (err) {
            this._emitWarn(err);
            return [];
        }
    }

    _fetchPod(id, fields) {
        // Coalesce: if a fetch for this device is in flight, just re-run once after it.
        const inflight = this._podFetches.get(id);
        const f = Array.isArray(fields) ? fields : [];
        if (inflight) {
            inflight.pending = true;
            f.forEach((x) => inflight.fields.add(x));
            if (f.length === 0) inflight.all = true;
            return;
        }
        const entry = { pending: false, fields: new Set(f), all: f.length === 0 };
        this._podFetches.set(id, entry);
        const run = async () => {
            const changed = entry.all ? [] : Array.from(entry.fields);
            entry.fields = new Set();
            entry.all = false;
            entry.pending = false;
            try {
                const pod = await this.api.getPod(id);
                if (pod) {
                    if (!pod.id) pod.id = id;
                    this.devices.set(id, pod);
                    this._loadedIds.add(id);
                    this.emit('device', { id, device: pod, reason: 'changed', fields: changed });
                }
            } catch (err) {
                this._emitWarn(err);
            }
            if (entry.pending && this.running) return run();
            this._podFetches.delete(id);
        };
        run();
    }

    _schedulePoll() {
        clearTimeout(this._pollTimer);
        if (!this.running || !(this.pollIntervalMs > 0)) return;
        this._pollTimer = setTimeout(async () => {
            await this.refresh('poll');
            this._schedulePoll();
        }, this.pollIntervalMs);
    }

    // ---------------------------------------------------------------- helpers

    _setStatus(state, text) {
        this.state = state;
        this.emit('status', { state, text });
    }

    _emitWarn(err) {
        this.emit('warn', err);
    }
}

function cancelBody(res) {
    try {
        if (res && res.body && typeof res.body.cancel === 'function') {
            Promise.resolve(res.body.cancel()).catch(() => {});
        }
    } catch (_) { /* already closed */ }
}

module.exports = { SensiboAccount, MIN_POLL_MS };
