// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt
// SPDX-FileContributor: dgduncan, mochi-co

package main

import (
	"flag"
	"github.com/mochi-mqtt/server/v2/config"
	"github.com/mochi-mqtt/server/v2/hooks/nowildcardsub"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/mochi-mqtt/server/v2"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil))) // set basic logger to ensure logs before configuration are in a consistent format

	configFile := flag.String("config", "config.yaml", "path to mochi config yaml or json file")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	configBytes, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	options, busCfg, err := config.FromBytesWithBus(configBytes)
	if err != nil {
		log.Fatal(err)
	}

	server := mqtt.New(options)

	// If a `bus:` block is present in the config, enable the Redis Streams
	// clustering bus so multiple broker instances share published messages.
	// The hook needs the *mqtt.Server pointer, so it is added here (after
	// mqtt.New) rather than inside config parsing. InlineClient was already set
	// to true by FromBytesWithBus, which the bus consumer requires.
	if busCfg != nil {
		hlc := busCfg.ToHook(server)
		if err := server.AddHook(hlc.Hook, hlc.Config); err != nil {
			log.Fatal(err)
		}
	}

	// Block wildcard subscriptions: a client may only subscribe to a fully
	// specified topic (must know the exact name, e.g. the email), never with
	// "+"/"#". This runs alongside any auth hook from config; a subscribe must
	// pass both, so wildcards are refused regardless of the config ACL.
	if err := server.AddHook(new(nowildcardsub.Hook), nil); err != nil {
		log.Fatal(err)
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
