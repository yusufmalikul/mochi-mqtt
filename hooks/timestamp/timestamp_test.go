// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 yusufmalikul
// SPDX-FileContributor: yusufmalikul

package timestamp

import (
	"strconv"
	"strings"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestProvides(t *testing.T) {
	h := new(Hook)
	require.True(t, h.Provides(mqtt.OnPublish))
	require.False(t, h.Provides(mqtt.OnConnect))
}

func TestStampsTopicWithFinalSegmentS(t *testing.T) {
	for _, topic := range []string{"test/s", "a/b/s", "s"} {
		h := new(Hook)
		pk, err := h.OnPublish(nil, packets.Packet{TopicName: topic, Payload: []byte("data")})
		require.NoError(t, err)

		parts := strings.SplitN(string(pk.Payload), ":", 2)
		require.Len(t, parts, 2, "topic %q should be stamped", topic)
		require.Equal(t, "data", parts[0])
		_, perr := strconv.ParseInt(parts[1], 10, 64)
		require.NoError(t, perr, "suffix should be a numeric timestamp")
	}
}

func TestLeavesOtherTopicsUnchanged(t *testing.T) {
	for _, topic := range []string{"sensors", "status", "bus", "sensor/temp"} {
		h := new(Hook)
		pk, err := h.OnPublish(nil, packets.Packet{TopicName: topic, Payload: []byte("data")})
		require.NoError(t, err)
		require.Equal(t, []byte("data"), pk.Payload, "topic %q should be unchanged", topic)
	}
}
