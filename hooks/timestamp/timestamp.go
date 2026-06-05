// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 yusufmalikul
// SPDX-FileContributor: yusufmalikul

// Package timestamp provides a hook which appends a receive timestamp to the
// payload of published messages whose topic ends in "s". This mirrors the
// behaviour previously implemented in a custom EMQX fork (emqx_message.erl,
// make/4) being migrated to mochi-mqtt.
package timestamp

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Hook appends a millisecond receive timestamp to the payloads of messages
// published to topics ending with "s".
type Hook struct {
	mqtt.HookBase
	Log *slog.Logger
}

// ID returns the ID of the hook.
func (h *Hook) ID() string {
	return "timestamp"
}

// Provides indicates which hook methods this hook provides.
func (h *Hook) Provides(b byte) bool {
	return b == mqtt.OnPublish
}

// SetOpts is called when the hook receives inheritable server parameters.
func (h *Hook) SetOpts(l *slog.Logger, opts *mqtt.HookOptions) {
	h.Log = l
}

// OnPublish is called when a client publishes a message. When the topic ends
// with "s", the payload is rewritten to "<payload>:<unix-millis>".
func (h *Hook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if !strings.HasSuffix(pk.TopicName, "s") {
		return pk, nil
	}

	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)

	stamped := make([]byte, 0, len(pk.Payload)+1+len(ts))
	stamped = append(stamped, pk.Payload...)
	stamped = append(stamped, ':')
	stamped = append(stamped, ts...)
	pk.Payload = stamped

	return pk, nil
}
