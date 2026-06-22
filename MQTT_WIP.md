# Mochi MQTT — Autoscale & Clustering WIP

Design notes from discussion. Not yet implemented; this is a blueprint for
deploying mochi as multi-instance autoscale with Redis as the message bus.

## Context / Goal

- Deploy mochi as **autoscale instances**: 10 at peak, down to 2 at midnight.
- Behind NLB `:1883` sit several mochi brokers (A, B, C, ...).
- Use **Redis as the message bus** + session store.
- Guarantee: **a client receives a message it subscribed to effectively once** (no duplicates
  in normal operation), even during autoscale / reconnect / crash. True exactly-once is not
  achievable end-to-end (see "Exactly-once honesty" below); the design is at-least-once
  transport + dedup.

## Key facts about mochi

- Mochi's built-in redis hook (`hooks/storage/redis`) is a **persistence hook**, NOT a
  clustering hook. It stores session/subscription/retained/inflight to Redis for single-server
  recovery — it does **not** route a message from broker A to a subscriber on broker B.
- **The storage hook restores data only ONCE, at boot** (`readStore()` runs when the hook is
  added, `server.go:354–360, 1555`). A client reconnecting to an already-running node gets
  **nothing** restored from Redis. Session portability across nodes does NOT come for free —
  it must be built (see scale-down section).
- **Multiple nodes sharing one redis storage hook clobber each other**: every node loads ALL
  clients/subscriptions at boot and writes to the SAME keys (inflight, session state). It is
  single-server in both directions, not just for routing.
- **`OnPublish` fires for inline/injected packets too** (`server.go:913`). `server.Publish()`
  → `InjectPacket` → `OnPublish` with `cl.Net.Inline == true`. Any hook that must not re-process
  bus-injected messages (the bus hook itself, the timestamp hook) MUST guard on `cl.Net.Inline`.
- Each instance's subscription table lives in local RAM. A `wkwk/s` subscriber on broker B
  is invisible to brokers A/C.
- Therefore **inter-node fan-out must be built yourself** via hooks:
  - `OnPublish` → intercept inbound messages
  - `server.Publish()` → inject outbound messages to local subscribers
  - Redis in the middle as the bus.

## Bus architecture

```
   clients ──► NLB :1883 (TCP passthrough, sticky)
        ┌───────────┼───────────┐
      mochi-A     mochi-B     mochi-C
        └───────────┼───────────┘
                  REDIS  (bus + session store)
```

- **Inbound:** an `OnPublish` hook publishes every MQTT message to a Redis Stream.
  The hook MUST skip inline clients (`if cl.Net.Inline { return pk }`) or every bus-read
  message gets re-XADDed → infinite loop.
- **Outbound:** each instance runs a consumer goroutine that reads the stream, then calls
  `server.Publish()` → mochi fans out to the LOCAL subscribers that match. Carry the original
  **QoS and retain flag** over the bus and pass them to `server.Publish()` — dropping QoS to 0
  breaks offline queueing for QoS 1 subscribers on remote nodes.
- **Use Redis Streams (not Pub/Sub)** — durable, replayable, not fire-and-forget. Important
  for the "client must receive its message" guarantee.
- **One consumer group PER NODE** (e.g. group name = broker ID), not one shared group. A single
  shared group load-balances entries across consumers — the opposite of the broadcast needed
  here. Alternatively plain `XREAD` from `$`. With per-node groups:
  - clean up dead nodes' groups on scale-down (`XGROUP DESTROY`), or their PELs accumulate;
  - `MAXLEN ~` trimming can discard entries a briefly-down node never acked — stream retention
    is the hard bound on the replay guarantee.

## Autoscale (10 → 2)

- **Scale-up:** trivial. New instance boots → registers with NLB → consumer goroutine starts.
  No coordination needed because every node already sees all messages via the bus.
- **Scale-down:** the danger zone. When the ASG kills an instance, all its TCP connections drop.
  - **Connection draining:** trap SIGTERM (already in `cmd/main.go`), stop accepting new
    connections, give a reconnect window, then `server.Close()`. Set ASG/k8s grace period
    ~30–60s, matched to NLB deregistration delay.
  - **Rely on client auto-reconnect** (paho/MQTTX reconnect automatically).
  - **Persistent sessions are mandatory:** clients connect with `CleanStart=false` +
    `SessionExpiryInterval` + a **stable ClientID**. Without a restored session, a client
    reconnecting to a fresh node has no subscriptions → silently misses messages.
  - **⚠️ The stock redis storage hook does NOT provide this.** It restores only at boot
    (see Key facts). Session portability must be custom-built: on `OnSessionEstablish`,
    fetch the client's subscriptions + offline queue from Redis and rehydrate them into the
    local node (re-subscribe in the topic tree, enqueue pending QoS 1/2). This is the single
    biggest piece of custom work in the design.
  - `SessionExpiryInterval` is client-requested; cap it server-side via
    `Options.Capabilities.MaximumSessionExpiryInterval`.
- **Two separate Redis mechanisms, both required:**
  1. Redis **session store** (custom rehydration layer, NOT the stock storage hook as-is)
     → session portability across nodes.
  2. Redis **bus (Streams)** → live routing between nodes.

## Anti-duplicate mechanism (CORE)

Scenario: publish AAA to `:1883` → lands on one broker. Subscriber `wkwk/s` is on B.

### Rule 1 — Origin tag
Each message gets an `origin` (ID of the first broker to receive it) + a unique `msgID`, then
is broadcast to the bus. A broker delivers to local subscribers either at **ingress** OR at
**bus-read**, **never both**.

```
OnPublish:
    if cl.Net.Inline: return pk    # bus-injected — do NOT re-broadcast (loop!)
    msg.origin = myBrokerID
    msg.msgID  = uuid()
    redis.XADD(stream, msg)        # broadcast to bus (incl. topic, payload, qos, retain)
    return pk                      # mochi delivers to THIS broker's LOCAL subscribers

consumer goroutine (reading from bus):
    if msg.origin == myBrokerID:
        ack; skip                  # already delivered locally at ingress
    else:
        server.Publish(msg.topic, msg.payload, msg.retain, msg.qos)  # local subscribers only
```

| Case | Ingress local delivery | Bus-read delivery |
|---|---|---|
| Publish on B | B delivers (local sub matches) | A,C → no sub. B reads its own → **skip** ✅ |
| Publish on A | A delivers (no sub → nobody) | B reads (origin≠B) → delivers ✅. A skips. C → no sub |

→ The client on B always receives **exactly one** copy.

### Rule 2 — Idempotency gate `SET NX EX` (key is PER NODE)
The origin tag is correct in the happy path, but races remain: QoS-1 redelivery, consumer-group
redelivery after crash, reconnect overlap during scale-down. Add a gate before delivery:

```
before server.Publish():
    ok = redis.SET("seen:"+msgID+":"+myBrokerID, "1", NX=true, EX=300)
    if !ok: return        # this NODE already delivered within window → drop
    server.Publish(...)   # first delivery on this node → send
```

- **⚠️ The key MUST include the broker ID.** A cluster-global `seen:<msgID>` key drops
  legitimate deliveries: if subscribers exist on both B and C, both read the message from the
  stream, only the first wins the `NX`, and the other broker's subscribers silently miss it.
  The gate's job is deduping **redeliveries to the same node**, not arbitrating between nodes.
- `NX` = set only if key doesn't exist → **atomic check-and-set**, only one wins.
  Avoids the race gap of the naive `EXISTS` then `SET` pattern (which can double-deliver).
- `EX 300` = key auto-expires → the "seen" set never grows unbounded.
- Choose `EX` ≥ **max redelivery window** (QoS-1 retry + bus redelivery + reconnect overlap),
  and ≥ bus stream retention. 300s is a safe default.
- **Optimization:** since the key is per-node anyway, an in-process LRU (msgID → seen) can
  replace the Redis round-trip entirely. Redis only buys persistence of the seen-set across a
  node restart — and a restarted node has a new consumer position anyway. Start with the LRU.

### Exactly-once honesty
- `SET NX` **before** `server.Publish()` = at-most-once within the window: crash between the
  SET and the delivery → message marked seen, consumer-group redelivery skips it → **lost**.
  Setting it after delivery has the mirror race (duplicate). Pick duplicate-vs-loss per topic
  class; for telemetry, loss of one reading is usually fine.
- Broker→client QoS-1 retransmits (DUP flag) happen **below** this gate — only the client app
  can dedup those (by msgID in the payload/user-property).

### Summary
1. **Origin tag** → kills structural duplicates.
2. **`msgID` + per-node seen-gate** → kills race/redelivery/QoS duplicates.
Used together → **effectively one delivery** (exactly-once in non-crash paths).

## QoS
- Clients subscribe at **QoS 1** + rely on `msgID` dedup to collapse QoS-1 duplicates into one.
- **Avoid QoS 2 over the bus** — its 4-step handshake state would need cluster coordination,
  complex, rarely worth it. **QoS 1 + app-level dedup-by-msgID = the standard answer.**

## TTL — three DIFFERENT clocks (don't merge them)

| TTL | What | How to choose | Default |
|---|---|---|---|
| **Bus stream retention** | How long AAA lives in the Redis Stream | = max node-down window that still needs replay | `XADD MAXLEN ~ N`, ~10–30s up to ~5 min |
| **Dedup key (`seen:msgID`)** | How long to remember a msgID | ≥ max redelivery window | 300s |
| **Session expiry** | How long a disconnected client's session is kept | ≈ max reconnect window | ~120s |
| **Message expiry** (MQTT5) | Per-message TTL for offline subscriber | = how long the payload stays meaningful | telemetry: 30–60s; command: long/none |

### The "how long do we store AAA in Redis?" question
→ It means the **bus stream**: keep it ~**5 minutes** (via `MAXLEN ~`), because once every live
node has read it, AAA only matters for replaying to a node that was briefly down. A node down
>5 min is treated as gone & its clients have reconnected to a live node (getting live traffic, not replay).

- The bus is an **in-flight conveyor, NOT an archive**. If clients reliably reconnect within
  seconds, bus retention can be **very short (10–30s)**.
- Durability for **offline** clients = the **session queue's** job (storage hook), not the bus.
- Common mistake: making the bus a long-term store.

### Rule of thumb
- Bus retention ≈ max node-outage window (seconds–minutes).
- Session expiry ≈ max reconnect window (1–5 min, default 120s).
- Message expiry ≈ payload freshness (seconds for telemetry, long/none for commands).
- Dedup key `EX` **≥** bus retention + max redelivery window (inequality, not equality —
  300s dedup over a 30s bus is fine; the reverse is not).

## Retained messages over the bus

Retained state lives in each node's **local topic tree** — a retained publish on A does
nothing for a new subscriber on B unless propagated:

- Carry the **retain flag** on the bus and pass `retain=true` to `server.Publish()` so every
  node stores it in its own tree.
- New subscribers then get retained values from their local node — no cross-node lookup needed.
- The dedup gate must NOT apply to setting retained state (an "old" retained message re-read
  at node boot/replay is how a fresh node catches up); dedup applies to live fan-out only.

## Cross-node session takeover

Mochi handles takeover **within one node** natively. Cluster-wide needs two pieces:
1. **Ownership key** in Redis (`owner:<clientID> = brokerID`, set on connect) — detects that
   a live session exists elsewhere.
2. **Remote kick**: a control message over the bus ("disconnect clientID X") so the old node
   actually closes the stale connection. The key alone changes nothing on the other node.

## Failure policy: Redis down

If `XADD` fails in `OnPublish`, decide explicitly:
- **Fail open** (return pk anyway): local subscribers still get the message; remote
  subscribers silently miss it. Right for telemetry.
- **Fail closed** (return error → publish rejected, client retries): consistent but turns a
  Redis blip into a publish outage. Right for commands.
Consider per-topic-class policy + a local retry buffer for the fail-open path.

## Implementation checklist

| Concern | Mechanism |
|---|---|
| Cross-node routing | `OnPublish` → Redis Stream → per-node consumer → `server.Publish()` (carry qos+retain) |
| No re-broadcast loop | Bus hook + timestamp hook skip `cl.Net.Inline` in `OnPublish` |
| No double-delivery on origin node | Tag origin ID; skip local re-inject for own messages |
| Idempotent delivery | Unique `msgID` + **per-node** seen-gate (in-process LRU; Redis `SET NX EX` optional) |
| Session survives node death | **Custom rehydration** on `OnSessionEstablish` from Redis (stock storage hook restores only at boot) + stable ClientID + `CleanStart=false` |
| One live session per client | `OnConnect` ownership key + **remote-kick control message** over the bus |
| Retained across nodes | Propagate retain flag on bus → each node stores in local tree |
| Graceful scale-down | SIGTERM drain + NLB deregistration delay + auto-reconnect |
| Bus durability | Redis **Streams** + **one consumer group per node** (cleanup on scale-down) |
| Bus retention TTL | ≈ max node outage window |
| Session TTL | `SessionExpiryInterval` ≈ max reconnect window (~120s) |
| Message TTL | `MessageExpiryInterval` ≈ payload freshness |
| QoS | QoS 1 + app dedup; avoid QoS 2 over the bus |

## Repo notes

- `cmd/main.go` already has: TCP `:1883`, WS `:1882`, info dashboard `:8080`, `AllowHook`, `timestamp` hook.
- `timestamp` hook: appends `:<unix-millis>` to the payload when the topic's **final segment is `s`**
  (e.g. `wkwk/s` → `hello` becomes `hello:1749470400000`; `sensors`/`status` are NOT stamped).
  This is behavior, not a bug. ✅ Inline guard added (`if cl != nil && cl.Net.Inline`): bus-injected
  messages are no longer re-stamped, so remote-node subscribers see the same single-stamped payload
  as the origin node.
- `server.go:913`: `OnPublish` is invoked for inline/injected packets. `server.go:354–360, 1555`:
  `readStore()` restores from storage hooks once at boot only.
- Commit `b433215` "Improve message expiry" — relevant for per-message Message TTL.
- Relevant available hooks: `OnPublish`, `OnConnect`, `OnSessionEstablish`, `OnClientExpired`,
  `OnRetainedExpired`, `OnSelectSubscribers`, etc.
- Redis persistence example at `examples/persistence/redis/main.go`.

## Done so far

- ✅ **Bus hook** (`hooks/bus/bus.go`): `OnPublish` origin-tagger with `cl.Net.Inline` guard +
      `msgID` (xid) + `XADD` carrying topic/payload/qos/retain to a Redis Stream; per-node consumer
      group (group name == broker ID) + goroutine that re-injects remote-origin messages via
      `server.Publish` and skips its own origin (Rule 1 origin-tag dedup). Fail-open on `XADD` error.
      Wired into `cmd/main.go` behind `-broker-id` + `-redis` flags (off by default; single-instance
      unchanged). Requires server `Options.InlineClient=true` (set in main when bus is enabled) —
      `server.Publish` errors without it.
- ✅ **Timestamp inline guard** — prevents `hello:<ts>:<ts>` double-stamping on remote nodes.
- ⏳ Verified end-to-end with two instances on one Redis (sub on B / pub on A, both directions):
      exactly one copy per node, identical single-stamped payload. **No bus-hook unit tests yet**
      (miniredis is available) — manual e2e only.

## TODO (not yet done)

- [ ] **Bus-hook unit tests** with `miniredis` (in-process server + fake Redis): assert XADD shape,
      origin-skip, remote re-inject, qos/retain carry-over.
- [ ] Per-node dedup gate (Rule 2): in-process LRU keyed by msgID, slotted into `dispatch` right
      before `server.Publish` (Redis `SET NX EX` with `seen:<msgID>:<brokerID>` only if cross-restart
      persistence proves necessary). Origin tag handles happy-path dups; gate handles QoS-1 / crash
      / consumer-group redelivery races.
- [ ] **Session rehydration** (the big one): on `OnSessionEstablish`, fetch subscriptions +
      offline queue from Redis and rehydrate into the local node. Stock storage hook restores
      only at boot, and multiple nodes sharing it clobber each other's keys — needs a
      per-cluster schema, not the stock hook as-is.
- [ ] `OnConnect` takeover: ownership key + remote-kick control message over the bus.
- [ ] Retained-message propagation (retain flag over bus, exempt from dedup gate).
- [ ] Decide Redis-down policy per topic class (fail open vs fail closed).
- [ ] Consumer-group cleanup on scale-down (`XGROUP DESTROY` for dead nodes).
- [ ] Graceful SIGTERM drain + tune NLB deregistration delay.
- [ ] Determine final TTL values against real traffic profile.
