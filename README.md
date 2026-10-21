# go-webhook-inspector

A small, self-contained RequestBin-style webhook inspector, written in Go
using only the standard library (`net/http`, `embed`, no third-party
dependencies). It hands out unique inbound endpoint URLs, captures every
HTTP request made against them in memory, streams newly captured requests
to the browser live via Server-Sent Events, and can replay a captured
request to any target URL.

There is no database — everything lives in process memory and is lost on
restart. Each bin keeps the most recent 100 requests (oldest evicted first).

## Features

- `POST /api/bins` — create a new bin, returns `{id, url}`.
- `ANY /in/<bin_id>` (and any sub-path under it) — inbound capture endpoint.
  Accepts any HTTP method, records timestamp, method, path, headers, query
  params, and body (capped at 1MB), and responds `200 OK`.
- `GET /api/bins/<bin_id>/requests` — list captured requests (JSON), newest
  first.
- `GET /api/bins/<bin_id>/stream` — Server-Sent Events stream that pushes
  each newly captured request live to connected clients.
- `POST /api/bins/<bin_id>/requests/<req_id>/replay` — replays a captured
  request's method/headers/body to a target URL supplied as
  `{"target_url": "https://..."}`, and returns the target's response
  status/headers/body.
- A minimal single-page frontend (plain HTML/CSS/JS, no build step, embedded
  into the binary via `embed`) showing the bin's inbound URL, a live list of
  incoming requests (via `EventSource`), click-to-expand request details,
  and a "replay to..." box per request.

## Project layout

```
main.go       — entrypoint, embeds static/, wires up the HTTP server
store.go      — in-memory, concurrency-safe bin/request store (ring buffer + pub/sub)
handlers.go   — HTTP handlers (create bin, inbound capture, list, SSE stream, replay)
store_test.go — unit tests for the store, including concurrent-access tests
static/       — embedded frontend (index.html, inline CSS/JS)
```

## Requirements

- Go 1.22+ (uses `net/http`'s method/wildcard routing patterns, e.g.
  `POST /api/bins` and `{binID}` path values).

## Build & run

```sh
go build -o webhook-inspector .
./webhook-inspector                 # listens on :8080 by default
./webhook-inspector -addr :9000     # or pick a different port
```

Then open http://localhost:8080/ in a browser. A bin is created
automatically on load; copy the shown URL and send requests to it (e.g.
`curl -X POST http://localhost:8080/in/<bin_id> -d '{"hello":"world"}'`) to
see them appear live.

If creating a bin or loading its requests fails, the page displays an error
and a **Retry** button. Retry reuses a successfully created bin when only
loading requests failed.

## Run without building

```sh
go run .
```

## Tests

Run the unit tests, including the concurrent-access tests, with the race
detector:

```sh
go vet ./...
go test -race ./...
```

## Example: manual walkthrough with curl

```sh
# 1. Create a bin
curl -s -X POST http://localhost:8080/api/bins
# => {"id":"90fa9bebd8e3387a","url":"/in/90fa9bebd8e3387a"}

# 2. Send it a request
curl -s -X POST http://localhost:8080/in/90fa9bebd8e3387a?foo=bar \
  -H "X-Custom: hello" -d '{"hello":"world"}'

# 3. List what was captured
curl -s http://localhost:8080/api/bins/90fa9bebd8e3387a/requests

# 4. Replay a captured request to another URL (use its "id" from step 3)
curl -s -X POST \
  http://localhost:8080/api/bins/90fa9bebd8e3387a/requests/<req_id>/replay \
  -H "Content-Type: application/json" \
  -d '{"target_url":"http://localhost:9000/somewhere"}'
```

## Design notes

- **Storage**: each bin is a mutex-protected, newest-first slice capped at
  100 entries (`MaxRequestsPerBin`); adding past capacity evicts the oldest
  entry (ring-buffer semantics).
- **Live updates**: each bin also keeps a set of subscriber channels. A
  capture fans the new request out to all subscribers non-blockingly (a slow
  or stalled SSE client drops events rather than blocking the inbound
  capture path). The SSE handler translates that channel into
  `event: request` / `data: <json>` frames and sends periodic keepalive
  comments to keep the connection alive.
- **Concurrency safety**: the top-level `Store` uses an `RWMutex` to guard
  the bin map; each `bin` uses its own `Mutex` for its request slice and a
  separate mutex for its subscriber set, so inbound captures, request
  listing, and SSE subscription/publish can all happen concurrently without
  data races (verified with `go test -race`).
- **Body cap**: inbound bodies are read via `io.LimitReader` and capped at
  1MB (`MaxBodyBytes`); anything beyond that is truncated and flagged with
  `body_truncated: true`.
- **Replay**: replays the original method/body to the given `target_url`
  with the original headers forwarded, minus `Host`, `Content-Length`, and
  `Connection`, which are not safe to forward verbatim to a new destination.

Replay also strips standard hop-by-hop headers and any headers named by the original `Connection` header while preserving the request body and other headers.

## Continuous integration

CI selects the Go version from `go.mod`, runs `go vet ./...` and `go test -race -count=1 ./...`, and builds all packages.

Checks run on pushes and pull requests and can be started manually from the Actions tab. The workflow has read-only repository permissions, cancels superseded runs, and stops after 15 minutes.
