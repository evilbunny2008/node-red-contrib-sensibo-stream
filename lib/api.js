'use strict';

const DEFAULT_BASE_URL = 'https://home.sensibo.com/api/v2';

class SensiboApiError extends Error {
    constructor(message, status, body) {
        super(message);
        this.name = 'SensiboApiError';
        this.status = status;
        this.body = body;
    }
}

/**
 * Small REST client for the parts of the Sensibo API the stream needs.
 * Uses the global fetch (Node >= 18), which transparently decompresses gzip.
 */
class SensiboApi {
    constructor({ apiKey, baseUrl = DEFAULT_BASE_URL, fetchImpl } = {}) {
        if (!apiKey) throw new Error('Sensibo API key is required');
        this.apiKey = apiKey;
        this.baseUrl = baseUrl.replace(/\/+$/, '');
        this.fetch = fetchImpl || globalThis.fetch;
    }

    // Note: only GET /stream accepts the X-Api-Key header. The REST endpoints
    // authenticate with the documented `apiKey` query parameter (they return 401
    // for a header-only request), so request() adds it to the URL.
    headers(extra) {
        return Object.assign({
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip'
        }, extra);
    }

    async request(method, path, { query, body, signal } = {}) {
        const url = new URL(this.baseUrl + path);
        url.searchParams.set('apiKey', this.apiKey);
        if (query) {
            for (const [k, v] of Object.entries(query)) {
                if (v !== undefined && v !== null) url.searchParams.set(k, v);
            }
        }
        const init = { method, headers: this.headers(), signal };
        if (body !== undefined) {
            init.headers['Content-Type'] = 'application/json';
            init.body = JSON.stringify(body);
        }
        const res = await this.fetch(url, init);
        const text = await res.text();
        let json;
        try { json = text ? JSON.parse(text) : undefined; } catch (_) { json = undefined; }
        if (!res.ok) {
            const reason = (json && (json.reason || json.message)) || res.statusText || 'request failed';
            const err = new SensiboApiError(`Sensibo API ${method} ${path}: HTTP ${res.status} ${reason}`, res.status, json || text);
            err.retryAfter = parseRetryAfter(res.headers.get('retry-after'));
            throw err;
        }
        return json && Object.prototype.hasOwnProperty.call(json, 'result') ? json.result : json;
    }

    getPods({ fields = '*', signal } = {}) {
        return this.request('GET', '/users/me/pods', { query: { fields }, signal });
    }

    /** AC state history, newest first (max 20). */
    getAcStates(id, { limit = 1, signal } = {}) {
        return this.request('GET', `/pods/${encodeURIComponent(id)}/acStates`, { query: { limit }, signal });
    }

    getPod(id, { fields = '*', signal } = {}) {
        return this.request('GET', `/pods/${encodeURIComponent(id)}`, { query: { fields }, signal });
    }
}

/** Parse a Retry-After header (delta-seconds or HTTP-date) into milliseconds. */
function parseRetryAfter(value, now = Date.now()) {
    if (value === null || value === undefined || value === '') return 0;
    const s = String(value).trim();
    if (/^\d+(\.\d+)?$/.test(s)) return Math.round(parseFloat(s) * 1000);
    const t = Date.parse(s);
    if (!isNaN(t)) return Math.max(0, t - now);
    return 0;
}

module.exports = { SensiboApi, SensiboApiError, parseRetryAfter, DEFAULT_BASE_URL };
