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
	"github.com/mochi-mqtt/server/v2/hooks/timestamp"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/go-redis/redis/v8"
)

func main() {
	tcpAddr := flag.String("tcp", ":1883", "network address for TCP listener")
	wsAddr := flag.String("ws", ":1882", "network address for Websocket listener")
	infoAddr := flag.String("info", ":8080", "network address for web info dashboard listener")
	tlsCertFile := flag.String("tls-cert-file", "", "TLS certificate file")
	tlsKeyFile := flag.String("tls-key-file", "", "TLS key file")
	brokerID := flag.String("broker-id", "", "unique broker ID; enables the Redis Streams bus for multi-instance clustering when set")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address for the cluster bus")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	var tlsConfig *tls.Config

	if tlsCertFile != nil && tlsKeyFile != nil && *tlsCertFile != "" && *tlsKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCertFile, *tlsKeyFile)
		if err != nil {
			return
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	// InlineClient must be enabled for the bus hook's consumer goroutine to
	// re-inject remote messages via server.Publish.
	server := mqtt.New(&mqtt.Options{InlineClient: *brokerID != ""})
	_ = server.AddHook(new(auth.AllowHook), nil)
	_ = server.AddHook(new(timestamp.Hook), nil)

	tcp := listeners.NewTCP(listeners.Config{
		ID:        "t1",
		Address:   *tcpAddr,
		TLSConfig: tlsConfig,
	})
	err := server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	ws := listeners.NewWebsocket(listeners.Config{
		ID:      "ws1",
		Address: *wsAddr,
	})
	err = server.AddListener(ws)
	if err != nil {
		log.Fatal(err)
	}

	stats := listeners.NewHTTPStats(
		listeners.Config{
			ID:      "info",
			Address: *infoAddr,
		},
		server.Info,
	)
	err = server.AddListener(stats)
	if err != nil {
		log.Fatal(err)
	}

	if *brokerID != "" {
		err = server.AddHook(new(bus.Hook), &bus.Options{
			RedisOptions: &redis.Options{Addr: *redisAddr},
			Server:       server,
			BrokerID:     *brokerID,
		})
		if err != nil {
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
