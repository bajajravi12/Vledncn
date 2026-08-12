# vless-worker

A VLESS-over-WebSocket (WSS) endpoint running on Cloudflare Workers. TLS
termination and the public `:443` listener are handled entirely by
Cloudflare's edge — the Worker only ever sees an already-decrypted HTTP/
WebSocket request and never binds to a port itself.

```
Client → TLS :443 → Cloudflare edge → Worker (WebSocket) → VLESS parser → outbound TCP (cloudflare:sockets) → destination
```

## Project structure

```
vless-worker/
├── src/
│   └── index.js      # Worker entry point: HTTP routing, WS upgrade, VLESS parsing, TCP bridge
├── wrangler.toml      # Wrangler configuration (no secrets in here)
├── package.json
└── README.md
```

## 1. Install

```bash
npm install
```

## 2. Log in to Cloudflare

```bash
npx wrangler login
```

## 3. Configure the UUID secret

The UUID is **never** hard-coded. Set it as a Worker secret:

```bash
npx wrangler secret put UUID
```

You'll be prompted to paste a UUID (v4 format, e.g.
`123e4567-e89b-12d3-a456-426614174000`). Generate one locally, e.g.:

```bash
python3 -c "import uuid; print(uuid.uuid4())"
```

## 4. (Optional) Change the WebSocket path

The default path is `/vless`, set via `WS_PATH` in `wrangler.toml`:

```toml
[vars]
WS_PATH = "/vless"
```

Edit this value before deploying if you want a different, less guessable
path.

## 5. Deploy

```bash
npx wrangler deploy
```

After deployment, Wrangler prints your Worker's URL:

```
https://vless-worker.YOUR-SUBDOMAIN.workers.dev
```

TLS is already active on `*.workers.dev` by default, so this URL is usable
as-is over `wss://` on port 443.

### Using a custom domain

1. Add the domain to your Cloudflare account and make sure it's proxied
   (orange cloud) through Cloudflare.
2. In the Cloudflare dashboard: **Workers & Pages → your worker → Settings
   → Domains & Routes → Add → Custom Domain**, and enter your domain
   (e.g. `proxy.example.com`).
3. Cloudflare automatically issues/attaches a TLS certificate and routes
   `https://proxy.example.com` (and `wss://proxy.example.com`) to the
   Worker on port 443 — no extra config in `wrangler.toml` needed.

## 6. Client configuration

Generic VLESS client settings (fill in your own values — nothing here
should be hard-coded):

```
Protocol:   VLESS
Address:    YOUR_DOMAIN            (e.g. proxy.example.com or vless-worker.YOUR-SUBDOMAIN.workers.dev)
Port:       443
UUID:       YOUR_UUID              (the value you set with `wrangler secret put UUID`)
Encryption: none
Transport:  WebSocket (ws)
TLS:        enabled
Path:       /vless                 (or your custom WS_PATH)
Host:       YOUR_DOMAIN
SNI:        YOUR_DOMAIN
```

## 7. Testing

```bash
# 1. Health endpoint
curl https://YOUR_DOMAIN/health
# → {"status":"ok"}

# 2. Normal HTTP request
curl https://YOUR_DOMAIN/
# → VLESS Worker is running

# 3. Invalid WebSocket path (should 404)
curl -i -H "Upgrade: websocket" -H "Connection: Upgrade" \
     -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
     -H "Sec-WebSocket-Version: 13" \
     https://YOUR_DOMAIN/wrong-path

# 4. Valid path but no Upgrade header (should 426)
curl -i https://YOUR_DOMAIN/vless

# 5+. Invalid UUID, malformed VLESS header, valid upgrade, TCP failure,
#     WS/TCP close, bidirectional forwarding — these require an actual
#     VLESS client (e.g. v2rayN, v2rayNG, Xray, sing-box, Shadowrocket,
#     Clash Meta) pointed at your deployed Worker, since they involve the
#     binary VLESS wire protocol rather than plain HTTP.
```

Recommended manual test matrix once you have a client configured:

| Test                              | Expected result                              |
|------------------------------------|-----------------------------------------------|
| Correct UUID, reachable target     | Connection succeeds, traffic flows both ways  |
| Wrong UUID                         | WebSocket closes immediately, no data forwarded |
| Malformed/truncated VLESS header   | WebSocket closes, no crash, no stack trace leaked |
| UDP command                        | Rejected (only TCP is implemented)            |
| Unreachable/refused destination    | WebSocket closes cleanly, Worker doesn't hang |
| Client disconnects mid-transfer    | TCP socket is closed, no orphaned connection  |
| Destination closes mid-transfer    | WebSocket is closed, no orphaned connection   |

## Security notes

- UUID is read only from the `UUID` secret — never logged, never echoed
  back in responses or error messages.
- Any header that fails UUID / structure validation causes an immediate,
  generic close with no details about *why* it failed.
- Only VLESS command `1` (TCP) is supported. UDP (`2`) and MUX (`3`) are
  explicitly rejected rather than faked.
- A 10-second timeout closes the WebSocket if a client opens a connection
  but never sends a valid VLESS header, preventing orphaned sessions.
- All error paths return generic messages/status codes — no internal
  errors, stack traces, or destination details are exposed to the client.

## Known Cloudflare Workers limitations

- **UDP is not supported.** The `cloudflare:sockets` API only exposes TCP
  `connect()`; there is no outbound UDP socket API on Workers today. VLESS
  UDP requests are therefore rejected rather than faked — there is no
  workaround at the platform level.
- **Outbound TCP restrictions.** `cloudflare:sockets` blocks a handful of
  ports commonly associated with abuse (e.g. SMTP on 25), and cannot be
  used to reach Cloudflare's own infrastructure IP ranges. This is a
  platform-level restriction, not something this code can bypass.
- **Execution model.** Workers only bill/limit *CPU time*, not wall-clock
  time, so a long-lived WebSocket connection that's mostly idle (waiting
  on I/O) does not by itself hit CPU limits. However, very CPU-heavy
  per-connection logic could still hit plan-specific CPU time limits.

## Cloudflare's usage terms

Cloudflare's terms of service for Workers restrict using the platform to
operate a general-purpose VPN or traffic-obfuscation proxy service. Before
deploying this for anything beyond personal/testing use, review
Cloudflare's current Workers-specific terms (and the TCP Sockets API
documentation) to confirm your use case is compliant, since violating
those terms can result in account suspension.
