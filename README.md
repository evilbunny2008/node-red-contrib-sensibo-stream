# node-red-contrib-sensibo-stream

Node-RED nodes for the [Sensibo API v2 Event Stream](https://support.sensibo.com/api/#event-stream):
real-time temperature/humidity measurements and AC state changes over Server-Sent Events,
instead of polling.

No runtime dependencies. Requires Node.js 18+ and Node-RED 3+.

## Install

From your Node-RED user directory (usually `~/.node-red`):

```sh
npm install /path/to/node-red-contrib-sensibo-stream
```

Then restart Node-RED. An example flow is available under *Import → Examples*.

## Nodes

### `sensibo-stream-account`
Your Sensibo account. Get an API key at <https://home.sensibo.com/me/api>.
The key is stored as a Node-RED credential. It is the same key the older REST-based packages use:
the stream gets it in the `X-Api-Key` header, REST calls in the documented `apiKey` query parameter.

All stream nodes using the same account **share one connection** — Sensibo limits an
account to 10 open streams, and one stream already carries every device.

### `sensibo-event-stream`
Emits a message per event. Use a *switch* on `msg.event` to route them.

| `msg.event`    | When                                                     | `msg.payload`                         |
|----------------|----------------------------------------------------------|---------------------------------------|
| `measurements` | New readings (max once/min per device, instant for motion) | `{ id, measurements, podId? }`      |
| `changed`      | Something changed on a device                            | `{ id, fields }`                      |
| `device`       | Full device re-read after a `changed` event              | full device object from `/pods/{id}`  |
| `poll`         | Start-up (all devices), 15-min slow poll, or input       | full device object                    |
| `hello`        | Stream opened                                            | `{ version, heartbeatSeconds, pods }` |
| `resync`       | Server is closing the stream (auto-reconnects)           | `{}`                                  |

On start-up every device is sent once as a `poll` message, so all values are available
immediately — even if the stream itself is unavailable, or only this node was redeployed
(`msg.reason` is `hello`, `startup` or `cached`).

Other properties: `msg.topic` (`sensibo/<event>/<deviceId>`), `msg.deviceId`, `msg.podId`
(sensor measurements), `msg.fields`, and optionally `msg.device` — the cached device state
with stream measurements merged in.

You can limit output to specific device ids (comma-separated).

**Input:** `msg.payload = "reconnect"` reopens the stream; anything else re-reads all devices.

## Sensibo's client rules, implemented

- Reconnect with exponential backoff + jitter, capped at 5 minutes
- On HTTP 429/503, wait at least `Retry-After`; keep slow-polling meanwhile
- Reconnect if nothing (not even a heartbeat) arrives for 2 heartbeat intervals
- Reopen promptly after `resync`
- Fetch all devices right after `hello`, then every ≥ 15 minutes
- gzip enabled; API key kept out of the stream URL; bad keys back off to the maximum delay

Controlling the AC (on/off, mode, temperature) is not part of the stream — use the REST API,
e.g. an *http request* node to `POST https://home.sensibo.com/api/v2/pods/{id}/acStates`
with body `{"acState":{"on":true}}`, or another Sensibo Node-RED package.

## Development

```sh
npm test
```

Tests run against a local mock SSE server. Set `SENSIBO_BASE_URL` to point the nodes at a
mock API when testing inside Node-RED.
