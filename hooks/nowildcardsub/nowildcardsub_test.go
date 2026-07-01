// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt

package nowildcardsub

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

func newHook() *Hook {
	h := new(Hook)
	h.SetOpts(logger, nil)
	return h
}

func TestID(t *testing.T) {
	require.Equal(t, "no-wildcard-sub", new(Hook).ID())
}

func TestProvides(t *testing.T) {
	h := new(Hook)
	require.True(t, h.Provides(mqtt.OnACLCheck))
	require.True(t, h.Provides(mqtt.OnConnectAuthenticate))
	require.False(t, h.Provides(mqtt.OnPublish))
}

func TestConnectAlwaysAllowed(t *testing.T) {
	h := newHook()
	require.True(t, h.OnConnectAuthenticate(&mqtt.Client{}, packets.Packet{}))
}

func TestSubscribeExactAllowedWildcardBlocked(t *testing.T) {
	h := newHook()
	cl := &mqtt.Client{}

	const subscribe = false
	const publish = true

	// The whole point: exact topic (client knows the email) is allowed,
	// the wildcard fishing version is blocked.
	require.True(t, h.OnACLCheck(cl, "r/3/3/y@mail.com/r", subscribe),
		"exact topic must be allowed on subscribe")
	require.False(t, h.OnACLCheck(cl, "r/+/+/+/r", subscribe),
		"wildcard fishing filter must be blocked on subscribe")

	// Other wildcard shapes are all blocked on subscribe.
	for _, f := range []string{"#", "+/#", "r/#", "u/#", "r/+/+/e", "+/+/#", "r/+/+/+/r"} {
		require.False(t, h.OnACLCheck(cl, f, subscribe), "wildcard %q must be blocked", f)
	}

	// Other exact topics are allowed on subscribe.
	for _, top := range []string{"app/foo", "r/3/3/y@mail.com/r", "u/123", "a/b/c/d/e"} {
		require.True(t, h.OnACLCheck(cl, top, subscribe), "exact topic %q must be allowed", top)
	}

	// Publish is always allowed, even to wildcard-looking targets (a real
	// publish can never contain a wildcard, but we assert the branch anyway).
	require.True(t, h.OnACLCheck(cl, "r/3/3/y@mail.com/r", publish))
	require.True(t, h.OnACLCheck(cl, "app/foo", publish))
}

func TestHasWildcard(t *testing.T) {
	// Whole-level "+"/"#" are wildcards.
	require.True(t, hasWildcard("#"))
	require.True(t, hasWildcard("+"))
	require.True(t, hasWildcard("r/+/+/+/r"))
	require.True(t, hasWildcard("a/#"))

	// No wildcard levels.
	require.False(t, hasWildcard("r/3/3/y@mail.com/r"))
	require.False(t, hasWildcard("app/foo/bar"))

	// A "+"/"#" that is only part of a level is a literal char, not a wildcard.
	require.False(t, hasWildcard("r/a+b/c"))
	require.False(t, hasWildcard("sensor#1/data"))
}
