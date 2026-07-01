// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt

// Package nowildcardsub provides an auth hook that blocks wildcard
// subscriptions. A client may only subscribe to a fully-specified topic
// (no "+" or "#"); any wildcard subscribe is refused. Publish and connect
// are always allowed.
//
// This reproduces the intent of an EMQX rule like:
//
//	{deny, all, subscribe, [{eq, "r/+/+/+/r"}, {eq, "#"}, ...]}.
//
// but generalized: instead of listing each wildcard filter, it rejects ANY
// subscribe that contains a wildcard. So a client that knows the exact topic
// (e.g. r/3/3/y@mail.com/r) can subscribe, while a client that fishes with
// r/+/+/+/r or # is blocked. Mochi's ledger ACL cannot express this because it
// has no exact-match ("eq") operator; a "+" ACL filter always matches a
// concrete value.
package nowildcardsub

import (
	"bytes"
	"strings"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Hook blocks any subscribe whose topic filter contains a wildcard.
type Hook struct {
	mqtt.HookBase
}

// ID returns the ID of the hook.
func (h *Hook) ID() string {
	return "no-wildcard-sub"
}

// Provides indicates which hook methods this hook provides.
func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
	}, []byte{b})
}

// OnConnectAuthenticate allows every client to connect.
func (h *Hook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	return true
}

// OnACLCheck allows all publishes and all fully-specified subscribes, but
// refuses a subscribe whose filter contains a single-level ("+") or
// multi-level ("#") wildcard.
//
// write == true  -> publish check: always allowed.
// write == false -> subscribe check: allowed only if the filter has no wildcard.
func (h *Hook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	if write {
		return true
	}

	if hasWildcard(topic) {
		h.Log.Debug("blocked wildcard subscribe",
			"client", cl.ID,
			"username", string(cl.Properties.Username),
			"filter", topic)
		return false
	}

	return true
}

// hasWildcard reports whether an MQTT topic filter uses a wildcard level.
// A level is a wildcard only when it is exactly "+" or "#"; a "+" or "#" that
// appears as part of a longer level (e.g. inside an email local-part) is a
// literal character, not a wildcard, per the MQTT spec.
func hasWildcard(filter string) bool {
	for _, level := range strings.Split(filter, "/") {
		if level == "+" || level == "#" {
			return true
		}
	}
	return false
}
