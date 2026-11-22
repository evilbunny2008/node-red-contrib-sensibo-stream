# node-red-contrib-sensibo-stream

Node-RED nodes for the [Sensibo API v2 Event Stream](https://support.sensibo.com/api/#event-stream):
real-time temperature/humidity measurements and AC state changes over Server-Sent Events,
instead of polling.

- One shared stream connection per account, with all of Sensibo's client rules built in
- Every device's full state on start-up, so values are available immediately
- Optional lookup of **what caused** an AC change (app, remote, Climate React, schedule…)
- No runtime dependencies. Requires Node.js 18+ and Node-RED 3+

It works alongside [node-red-contrib-sensibo](https://flows.nodered.org/node/node-red-contrib-sensibo)
(which uses the older REST polling) — the node types have different names, so both can be installed.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Nodes](#nodes)
- [Output messages](#output-messages)
- [What caused a change](#what-caused-a-change)
- [Recipes](#recipes): store state in global context · read it elsewhere · control the AC · rate limiting
- [How the connection behaves](#how-the-connection-behaves)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Install

In Node-RED: *Menu → Manage palette → Install*, search for `node-red-contrib-sensibo-stream`.

Or from your Node-RED user directory (often `~/.node-red`, check *Settings file* / *User directory*
in the Node-RED start-up log), as the user Node-RED runs as:

```sh
npm install node-red-contrib-sensibo-stream
```

Then restart Node-RED **and reload the editor in the browser** (Ctrl+Shift+R) — see
[Troubleshooting](#troubleshooting) for why the reload matters after every upgrade.

## Quick start

1. Create an API key at <https://home.sensibo.com/me/api>. It is the same kind of key the older
   Sensibo packages use.
2. Drag **sensibo stream** (palette category *home automation*) into a flow.
3. Open it, click the pencil next to *Account*, paste the API key, click *Add*.
4. Wire it to a **debug** node and deploy.

Within a few seconds you get one `poll` message per device with its full state, then live
`measurements` and `device` messages as things change.

Examples: *Menu → Import → Examples → node-red-contrib-sensibo-stream*.

## Nodes

### Account (`sensibo-stream-account`, config node)

| Setting | Default | Meaning |
|---|---|---|
| API key | | Stored as a Node-RED credential. |
| Slow poll | 15 min | All devices are re-read at this interval because not every field has a stream event. Sensibo asks for ≥ 15 minutes. Polling continues at this rate while the stream is unavailable. |
| Re-read device on `changed` events | on | Fetch `/pods/{id}` after a `changed` event and emit a `device` message with the full new state. |
| Look up what caused AC state changes | off | See [What caused a change](#what-caused-a-change). One extra API call per AC change. |

All stream nodes using the same account **share one connection** — Sensibo allows at most
10 open streams per account, and one stream carries every device.

### sensibo stream (`sensibo-event-stream`)

| Setting | Meaning |
|---|---|
| Account | The account above. |
| Devices | Blank for all, or comma-separated device ids to limit output. |
| Output | Which message types to send (see below). |
| Attach cached device state | Adds `msg.device` — the last known full state with stream measurements merged in. |

**Input** — `msg.payload = "reconnect"` reopens the stream; any other message re-reads all
devices and emits `poll` messages.

## Output messages

Route them with a **switch** node on `msg.event`.

| `msg.event` | When | `msg.payload` | Default |
|---|---|---|---|
| `measurements` | New readings — at most once a minute per device, only when a value changed; motion/presence instantly | `{ id, measurements, podId? }` | on |
| `device` | Full device re-read after a `changed` event | full device object (as `/pods/{id}`) | on |
| `poll` | Start-up, every slow poll, or on input | full device object | on |
| `changed` | Raw "something changed" notice | `{ id, fields }` | on |
| `hello` | Stream opened | `{ version, heartbeatSeconds, pods }` | off |
| `resync` | Server is closing the stream (reconnects automatically) | `{}` | off |

AC settings are in `msg.payload.acState` on `device` and `poll` messages:

```json
{ "on": true, "mode": "cool", "targetTemperature": 22, "temperatureUnit": "C",
  "fanLevel": "high", "swing": "fixedTop", "horizontalSwing": "fixedRight",
  "timestamp": { "time": "2026-11-22T05:35:22.091891Z", "secondsAgo": 0 } }
```

Other message properties:

| Property | Present on | Meaning |
|---|---|---|
| `topic` | all | `sensibo/<event>/<deviceId>` |
| `deviceId` | device events | Device (or sensor) id |
| `podId` | sensor `measurements` | The device a room/window sensor belongs to |
| `fields` | `changed`, `device` | What changed, e.g. `["acState"]`. Empty = anything may have changed (schedule, timer, Climate React) |
| `reason` | `poll`, `device` | Why the state was loaded: `hello` (stream opened), `startup` (stream unavailable at start-up), `cached` (only this node was redeployed), `poll`, `refresh` (input), `changed` |
| `cause`, `change` | `device` | What caused the AC change — see below |
| `device` | when enabled | Cached full device state |

> **`measurements` messages only contain measurements.** AC state only arrives in `device` and
> `poll` messages, so keep those enabled if you need it.

## What caused a change

The stream itself only says *that* a device changed. Enable **Look up what caused AC state
changes** on the account, and after each AC change the node also reads the latest entry of
Sensibo's AC state history (`/pods/{id}/acStates`) and adds it to the `device` message:

- `msg.cause` — the entry's `reason`, for example:

  | `msg.cause` | Typically means |
  |---|---|
  | `UserRequest` | Sensibo app, web app or API |
  | `ExternalIrCommand` | The AC's own IR remote |
  | `Trigger` | Climate React |
  | `ScheduledCommand` | A Sensibo schedule or timer |

- `msg.change` — the whole history entry (e.g. `reason`, `causedByUser`, `changedProperties`,
  `status`, `time`).

Notes:

- These history fields are **not part of Sensibo's published API schema**. They are passed through
  as Sensibo returns them; the list of values above is what has been observed, not an official
  list. Check `msg.change` in a debug node if you need other details.
- Only AC state changes are looked up, so changes to e.g. connection status cost nothing extra.
- The cause is only known for changes. `poll` messages and values loaded at start-up have none.

## Recipes

### Store the current state in global context

Wire this **function** node (0 outputs) after the stream node. Enable `device`, `poll` and
`measurements` on the stream node, and the cause lookup on the account if you want `lastChange`.
It is also in the *store-state-in-global* example.

```javascript
// Keeps global "sensibo" up to date:
//   sensibo.<deviceId> and sensibo["<room name>"]
//     -> { id, name, acState, measurements, connected, lastChange, updated }
const all = global.get('sensibo') || {};
const id = msg.deviceId;
if (!id) return null;

const entry = all[id] || { id };

if (msg.event === 'device' || msg.event === 'poll') {
    const d = msg.payload;                         // full device from Sensibo
    entry.name = d.room ? d.room.name : entry.name;
    if (d.acState) entry.acState = d.acState;
    if (d.measurements) entry.measurements = Object.assign({}, entry.measurements, d.measurements);
    if (d.connectionStatus) entry.connected = d.connectionStatus.isAlive;

    if (msg.change) {                              // what caused this AC change
        const c = msg.change;
        entry.lastChange = {
            reason: msg.cause,
            causedBy: c.causedByUser ? (c.causedByUser.firstName || c.causedByUser.username) : null,
            changed: c.changedProperties,
            status: c.status,
            time: c.time ? c.time.time : new Date().toISOString()
        };
        node.status({ text: `${entry.name || id}: ${msg.cause || '?'}` });
    }
} else if (msg.event === 'measurements') {
    entry.measurements = Object.assign({}, entry.measurements, msg.payload.measurements);
} else {
    return null;
}

entry.updated = new Date().toISOString();
all[id] = entry;
if (entry.name) all[entry.name] = entry;           // also reachable by room name
global.set('sensibo', all);
return null;
```

No persistent context store is needed: the stream sends every device on start-up, so the
context refills within seconds of a restart.

Room names are easier to read but change if you rename the room in the Sensibo app; device
ids never change.

### Read it elsewhere

- **change** node: *Set* `msg.payload` *to* `global` `sensibo["Bedroom"].acState`
- **switch** node: property `global` `sensibo["Bedroom"].acState.on`, rule *is true*
- **function** node:

  ```javascript
  msg.payload = global.get('sensibo')?.["Bedroom"]?.acState;   // undefined until loaded
  return msg;
  ```

- To watch it change: the *Context Data* sidebar → *Global* → refresh.

`acState.timestamp.secondsAgo` is only accurate at the moment it arrived; use `updated` or
`acState.timestamp.time` to judge age.

### Control the AC

The stream is receive-only (Server-Sent Events are one-way); commands go through Sensibo's REST
API. After a command, the stream reports the result as a `changed` → `device` message, which
doubles as confirmation.

- The **sensibo send** node from `node-red-contrib-sensibo` (`msg.on`, `msg.mode`,
  `msg.targetTemperature`, `msg.fanlevel`, `msg.swing`), or
- an **http request** node: method `POST`, URL
  `https://home.sensibo.com/api/v2/pods/<deviceId>/acStates?apiKey=<key>`, body
  `{"acState":{"on":true,"mode":"cool","targetTemperature":22}}`.

For a single setting send just that field — a full `acState` overwrites all fields.

> Node-RED's **http request** node does not use gzip, and Sensibo grants a higher rate limit
> to gzip clients. Avoid polling Sensibo with it; the stream node handles all reads with gzip.

### Rate limiting your commands

Put a **delay** node (*Rate Limit*, *All messages*, e.g. 1 msg / 5 s) before whatever sends
commands. For sliders or bursts where only the final value matters, use a **trigger** node as a
debounce instead (send nothing, then the latest message after 3 s, *extend delay if new message
arrives*).

The stream node's own traffic needs no limiting: one connection, slow poll ≥ 15 min,
`Retry-After` respected, one re-read per change (bursts for the same device are coalesced).

## How the connection behaves

Implements Sensibo's [rules for clients](https://support.sensibo.com/api/#event-stream):

- Reconnects with exponential backoff plus jitter, capped at 5 minutes
- On HTTP 429/503 waits at least `Retry-After` and keeps slow-polling meanwhile
- Reconnects if nothing — not even a heartbeat — arrives for two heartbeat intervals
- Reopens promptly after `resync` (Sensibo closes streams after some hours, or when the
  account's device list changes)
- Fetches all devices right after `hello`, then every slow poll
- gzip on every request; the API key goes in the `X-Api-Key` header for the stream and the
  documented `apiKey` query parameter for REST calls
- An invalid key (HTTP 401/403) backs off straight to the maximum delay

Node status: green *connected (N devices)*, yellow *connecting* / *stream unavailable, polling*,
red *auth failed* or *disconnected, retry in Ns*.

## Troubleshooting

**A new setting is missing, or doesn't stick after deploy.** The editor loads node definitions
only when the page loads. After upgrading the package and restarting Node-RED, reload the editor
(Ctrl+Shift+R). An editor tab opened before the upgrade keeps showing the old options and
silently drops new settings when you deploy.

**`type_already_registered`.** Versions before 0.0.2 used node type names that clash with
`node-red-contrib-sensibo`. Upgrade, then replace any old nodes in your flows.

**No nodes in the palette.** Look under *home automation*, or search the palette for
"sensibo". Make sure you installed into the user directory of the Node-RED instance you are
running (the start-up log shows it) and restarted it.

**No values at start-up.** Tick `poll` on the stream node. Nodes saved with an older version
may have it unticked.

**`HTTP 401` on `/users/me/pods`.** Fixed in 0.0.5 — upgrade.

**`API Limit exceeded. Use "Accept-Encoding: gzip"…`** Something is calling Sensibo without
gzip — usually an **http request** node in your own flow, polling or in a loop. Let the stream
node do the reads, and rate-limit your commands. Limits clear after a few minutes.

**`msg.cause` is missing.** Enable *Look up what caused AC state changes* on the account node
(reload the editor first if you don't see it). It only appears on `device` messages after an
AC state change.

**"msg properties can no longer override set node properties".** On an **http request** node,
clear the URL field and set Method to *set by msg.method* if you set them from `msg`.

## Development

```sh
npm test
```

Tests run against a local mock SSE/REST server (no network, no API key). To run the nodes inside
Node-RED against a mock API, set `SENSIBO_BASE_URL`, e.g.
`SENSIBO_BASE_URL=http://127.0.0.1:18999/api/v2 node-red`.

Releases are published to npm from GitHub Actions (trusted publishing) when a GitHub release is
created; the release tag sets the version.

See [CHANGELOG.md](CHANGELOG.md) for changes.

## License

See [LICENSE](LICENSE).
