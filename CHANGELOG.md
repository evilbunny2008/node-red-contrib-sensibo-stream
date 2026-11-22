# Changelog

## Unreleased
- Documentation: full README (output reference, cause lookup, recipes, troubleshooting),
  richer in-editor help, new *store-state-in-global* example flow.

## 0.0.11
- New account option **Look up what caused AC state changes**: `device` messages after an AC
  change carry `msg.cause` (e.g. `ExternalIrCommand`) and `msg.change` (the AC state history entry).

## 0.0.6 – 0.0.10
- Repository metadata, `package-lock.json`, GitHub Actions publishing with npm trusted publishing.

## 0.0.5
- Fix: REST calls (`/users/me/pods`, `/pods/{id}`) returned HTTP 401 — they now send the
  `apiKey` query parameter; the stream keeps using the `X-Api-Key` header.

## 0.0.4
- Every device is sent as a `poll` message on start-up; `poll` output enabled by default.
- Devices are loaded over REST if the stream is unavailable at start-up.
- A redeployed stream node replays the cached state (`msg.reason = "cached"`).

## 0.0.3
- Stream node moved to the *home automation* palette category.

## 0.0.2
- Node types renamed to `sensibo-stream-account` / `sensibo-event-stream` to avoid
  `type_already_registered` with `node-red-contrib-sensibo`.

## 0.0.1
- First version: shared SSE connection per account, Sensibo client rules, measurements merging,
  device re-read on `changed`, slow poll.
