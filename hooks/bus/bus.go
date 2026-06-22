// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 yusufmalikul
// SPDX-FileContributor: yusufmalikul

// Package bus provides a clustering hook that routes published messages between
// multiple mochi-mqtt instances using Redis Streams as the message bus.
//
// Each broker holds its subscription table in local RAM, so a subscriber on
// broker B is invisible to brokers A and C. This hook bridges that gap:
//
//   - Inbound: OnPublish XADDs every client-published message to a shared Redis
//     Stream, tagged with the originating broker ID and a unique msgID.
//   - Outbound: a per-node consumer goroutine reads the stream and re-injects
//     messages that originated elsewhere into the local node via server.Publish,
//     which fans them out to whichever local subscribers match.
//
// A broker delivers to its local subscribers either at ingress (for messages it
// received directly) OR at bus-read (for messages from other brokers), never
// both — the origin tag is what prevents the double delivery. Race-level
// duplicates (QoS-1 redelivery, consumer-group redelivery after a crash) are
// NOT yet handled here; that is the job of the seen-gate added on top of this.
package bus

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/go-redis/redis/v8"
	"github.com/rs/xid"
)

const (
	defaultStream    = "mochi:bus"
	defaultMaxLen    = 10000           // approximate stream cap via XADD MAXLEN ~
	defaultBlock     = 5 * time.Second // XReadGroup block timeout
	defaultBatchSize = 64
)

// Options configures the bus hook.
type Options struct {
	// RedisOptions configures the connection to the Redis bus. Required.
	RedisOptions *redis.Options

	// Server is the mochi server the consumer goroutine injects remote messages
	// into via Server.Publish. Required. The server MUST be created with
	// Options.InlineClient=true, otherwise Server.Publish returns an error and
	// no remote messages are delivered.
	Server *mqtt.Server

	// BrokerID uniquely identifies this broker in the cluster. It is used as the
	// origin tag and as the per-node consumer group name. Required and must be
	// stable+unique per instance.
	BrokerID string

	// Stream is the Redis Stream key used as the bus. Defaults to "mochi:bus".
	Stream string

	// MaxLen approximately caps the stream length (XADD MAXLEN ~). The bus is an
	// in-flight conveyor, not an archive: keep this small. Defaults to 10000.
	MaxLen int64
}

// Hook routes published messages between brokers over a Redis Stream.
type Hook struct {
	mqtt.HookBase
	config *Options
	db     *redis.Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ID returns the ID of the hook.
func (h *Hook) ID() string {
	return "bus"
}

// Provides indicates which hook methods this hook provides.
func (h *Hook) Provides(b byte) bool {
	return b == mqtt.OnPublish
}

// Init validates config, connects to Redis, creates the per-node consumer group
// and starts the consumer goroutine.
func (h *Hook) Init(config any) error {
	if _, ok := config.(*Options); !ok || config == nil {
		return mqtt.ErrInvalidConfigType
	}
	o := config.(*Options)

	if o.RedisOptions == nil {
		return errors.New("bus: RedisOptions is required")
	}
	if o.Server == nil {
		return errors.New("bus: Server is required")
	}
	if o.BrokerID == "" {
		return errors.New("bus: BrokerID is required")
	}
	if o.Stream == "" {
		o.Stream = defaultStream
	}
	if o.MaxLen == 0 {
		o.MaxLen = defaultMaxLen
	}
	h.config = o

	h.ctx, h.cancel = context.WithCancel(context.Background())
	h.db = redis.NewClient(o.RedisOptions)
	if err := h.db.Ping(h.ctx).Err(); err != nil {
		return err
	}

	// One consumer group PER NODE (group name == broker ID). A shared group
	// would load-balance entries across consumers — the opposite of the
	// broadcast every node needs. MKSTREAM creates the stream if absent;
	// BUSYGROUP means the group already exists, which is fine.
	if err := h.db.XGroupCreateMkStream(h.ctx, o.Stream, o.BrokerID, "$").Err(); err != nil &&
		err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}

	h.Log.Info("bus hook started", "broker", o.BrokerID, "stream", o.Stream)

	h.wg.Add(1)
	go h.consume()

	return nil
}

// Stop cancels the consumer goroutine and closes the Redis connection.
func (h *Hook) Stop() error {
	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait()
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// OnPublish broadcasts a client-published message to the bus. Inline (bus-
// injected) packets are skipped so re-injected messages are not re-XADDed,
// which would otherwise loop forever.
func (h *Hook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl != nil && cl.Net.Inline {
		return pk, nil
	}

	values := map[string]any{
		"origin":  h.config.BrokerID,
		"msgID":   xid.New().String(),
		"topic":   pk.TopicName,
		"payload": pk.Payload,
		"qos":     strconv.Itoa(int(pk.FixedHeader.Qos)),
		"retain":  strconv.FormatBool(pk.FixedHeader.Retain),
	}

	if err := h.db.XAdd(h.ctx, &redis.XAddArgs{
		Stream: h.config.Stream,
		MaxLen: h.config.MaxLen,
		Approx: true,
		Values: values,
	}).Err(); err != nil {
		// Fail open: the local subscribers still get the message via mochi's
		// normal delivery (we return pk below). Remote subscribers miss it.
		// This is the right default for telemetry; commands may want fail-closed.
		h.Log.Error("bus XADD failed", "error", err, "topic", pk.TopicName)
	}

	return pk, nil
}

// consume reads the bus and re-injects remote-origin messages into this node.
func (h *Hook) consume() {
	defer h.wg.Done()

	consumer := h.config.BrokerID
	for {
		select {
		case <-h.ctx.Done():
			return
		default:
		}

		res, err := h.db.XReadGroup(h.ctx, &redis.XReadGroupArgs{
			Group:    h.config.BrokerID,
			Consumer: consumer,
			Streams:  []string{h.config.Stream, ">"},
			Count:    defaultBatchSize,
			Block:    defaultBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || h.ctx.Err() != nil {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue // block timed out with no entries
			}
			h.Log.Error("bus XReadGroup failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range res {
			for _, msg := range stream.Messages {
				h.dispatch(msg)
			}
		}
	}
}

// dispatch handles a single bus message: skip our own, inject the rest.
func (h *Hook) dispatch(msg redis.XMessage) {
	// Ack regardless of outcome — the message has been seen by this node and a
	// failed re-injection should not be replayed forever.
	defer h.db.XAck(h.ctx, h.config.Stream, h.config.BrokerID, msg.ID)

	origin, _ := msg.Values["origin"].(string)
	if origin == h.config.BrokerID {
		// We originated it and already delivered to local subscribers at
		// ingress (OnPublish returned the packet). Skip to avoid a duplicate.
		return
	}

	topic, _ := msg.Values["topic"].(string)
	payload := toBytes(msg.Values["payload"])
	qos := parseByte(msg.Values["qos"])
	retain := parseBool(msg.Values["retain"])

	// server.Publish injects via an inline client, so OnPublish's inline guard
	// stops this from being re-broadcast onto the bus.
	if err := h.config.Server.Publish(topic, payload, retain, qos); err != nil {
		h.Log.Error("bus inject failed", "error", err, "topic", topic)
	}
}

func toBytes(v any) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return []byte(t)
	default:
		return nil
	}
}

func parseByte(v any) byte {
	s, _ := v.(string)
	n, _ := strconv.Atoi(s)
	return byte(n)
}

func parseBool(v any) bool {
	s, _ := v.(string)
	b, _ := strconv.ParseBool(s)
	return b
}
