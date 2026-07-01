// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"crypto/tls"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/hooks/bus"
	"github.com/mochi-mqtt/server/v2/hooks/nowildcardsub"
	"github.com/mochi-mqtt/server/v2/hooks/timestamp"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/go-redis/redis/v8"
)

func main() {
	tcpAddr := flag.String("tcp", ":1883", "address for the plaintext MQTT (TCP) listener; empty to disable")
	mqttsAddr := flag.String("mqtts", "", "address for the TLS MQTT (mqtts) listener; requires -tls-cert-file/-tls-key-file; empty to disable")
	wsAddr := flag.String("ws", ":1882", "address for the plaintext WebSocket (ws) listener; empty to disable")
	wssAddr := flag.String("wss", "", "address for the TLS WebSocket (wss) listener; requires -tls-cert-file/-tls-key-file; empty to disable")
	infoAddr := flag.String("info", ":8080", "network address for web info dashboard listener; empty to disable")
	tlsCertFile := flag.String("tls-cert-file", "", "TLS certificate file (used by -mqtts and -wss)")
	tlsKeyFile := flag.String("tls-key-file", "", "TLS key file (used by -mqtts and -wss)")
	brokerID := flag.String("broker-id", "", "unique broker ID; enables the Redis Streams bus for multi-instance clustering when set")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address for the cluster bus")
	blockWildcardSub := flag.Bool("block-wildcard-sub", true, "block subscriptions that use + or # wildcards; clients must subscribe to a fully specified topic")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	// Load the TLS certificate once; it is shared by the mqtts and wss
	// listeners. TLS is applied per-listener, so plaintext and TLS ports can
	// run side by side (e.g. plain mqtt:// for backend, wss:// for mobile).
	var tlsConfig *tls.Config
	if *tlsCertFile != "" || *tlsKeyFile != "" {
		if *tlsCertFile == "" || *tlsKeyFile == "" {
			log.Fatal("both -tls-cert-file and -tls-key-file are required for TLS")
		}
		cert, err := tls.LoadX509KeyPair(*tlsCertFile, *tlsKeyFile)
		if err != nil {
			log.Fatalf("loading TLS keypair: %v", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	if (*mqttsAddr != "" || *wssAddr != "") && tlsConfig == nil {
		log.Fatal("-mqtts/-wss require -tls-cert-file and -tls-key-file")
	}

	// InlineClient must be enabled for the bus hook's consumer goroutine to
	// re-inject remote messages via server.Publish.
	server := mqtt.New(&mqtt.Options{InlineClient: *brokerID != ""})
	// An auth hook must be registered or the server denies every connection
	// ("Bad User Name or Password"). With -block-wildcard-sub (default) the
	// nowildcardsub hook allows connections but blocks "+"/"#" subscribes;
	// clients must subscribe to a fully specified topic (e.g. the exact email).
	// Set -block-wildcard-sub=false to allow all subscribes (AllowHook).
	if *blockWildcardSub {
		_ = server.AddHook(new(nowildcardsub.Hook), nil)
	} else {
		_ = server.AddHook(new(auth.AllowHook), nil)
	}
	_ = server.AddHook(new(timestamp.Hook), nil)

	add := func(l listeners.Listener) {
		if err := server.AddListener(l); err != nil {
			log.Fatal(err)
		}
	}

	// Plaintext MQTT (mqtt://) for trusted/internal clients.
	if *tcpAddr != "" {
		add(listeners.NewTCP(listeners.Config{ID: "tcp", Address: *tcpAddr}))
	}
	// TLS MQTT (mqtts://).
	if *mqttsAddr != "" {
		add(listeners.NewTCP(listeners.Config{ID: "mqtts", Address: *mqttsAddr, TLSConfig: tlsConfig}))
	}
	// Plaintext WebSocket (ws://).
	if *wsAddr != "" {
		add(listeners.NewWebsocket(listeners.Config{ID: "ws", Address: *wsAddr}))
	}
	// TLS WebSocket (wss://) for public/mobile clients.
	if *wssAddr != "" {
		add(listeners.NewWebsocket(listeners.Config{ID: "wss", Address: *wssAddr, TLSConfig: tlsConfig}))
	}
	if *infoAddr != "" {
		add(listeners.NewHTTPStats(listeners.Config{ID: "info", Address: *infoAddr}, server.Info))
	}

	if *brokerID != "" {
		if err := server.AddHook(new(bus.Hook), &bus.Options{
			RedisOptions: &redis.Options{Addr: *redisAddr},
			Server:       server,
			BrokerID:     *brokerID,
		}); err != nil {
			log.Fatal(err)
		}
	}

	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	<-done
	server.Log.Warn("caught signal, stopping...")
	_ = server.Close()
	server.Log.Info("mochi mqtt shutdown complete")
}
