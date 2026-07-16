// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package config

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/hooks/bus"
	"github.com/mochi-mqtt/server/v2/hooks/debug"
	"github.com/mochi-mqtt/server/v2/hooks/timestamp"
	"github.com/mochi-mqtt/server/v2/hooks/storage/badger"
	"github.com/mochi-mqtt/server/v2/hooks/storage/bolt"
	"github.com/mochi-mqtt/server/v2/hooks/storage/pebble"
	storageredis "github.com/mochi-mqtt/server/v2/hooks/storage/redis"
	"github.com/mochi-mqtt/server/v2/listeners"
	"gopkg.in/yaml.v3"

	"github.com/go-redis/redis/v8"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/rs/xid"
)

// config defines the structure of configuration data to be parsed from a config source.
type config struct {
	Options       mqtt.Options
	Listeners     []listeners.Config `yaml:"listeners" json:"listeners"`
	HookConfigs   HookConfigs        `yaml:"hooks" json:"hooks"`
	LoggingConfig LoggingConfig      `yaml:"logging" json:"logging"`
}

type LoggingConfig struct {
	Level string
}

func (lc LoggingConfig) ToLogger() *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(lc.Level)); err != nil {
		level = slog.LevelInfo
	}

	leveler := new(slog.LevelVar)
	leveler.Set(level)
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: leveler,
	}))
}

// HookConfigs contains configurations to enable individual hooks.
type HookConfigs struct {
	Auth    *HookAuthConfig    `yaml:"auth" json:"auth"`
	Storage *HookStorageConfig `yaml:"storage" json:"storage"`
	Debug   *debug.Options     `yaml:"debug" json:"debug"`
	Bus     *BusConfig         `yaml:"bus" json:"bus"`
}

// BusConfig configures the Redis Streams clustering bus that shares published
// messages between multiple broker instances. It is NOT turned into a hook by
// ToHooks: the bus hook needs the *mqtt.Server pointer, which is only known
// after mqtt.New. The caller (e.g. cmd/docker) must add the bus hook itself
// using the values parsed here — see BusConfig.ToHook.
type BusConfig struct {
	// RedisAddr is the Redis address for the bus, e.g. "redis:6379". Required.
	RedisAddr string `yaml:"redis_addr" json:"redis_addr"`
	// RedisPassword is the Redis password, if the bus Redis needs auth.
	RedisPassword string `yaml:"redis_password" json:"redis_password"`
	// RedisDB is the Redis database number for the bus.
	RedisDB int `yaml:"redis_db" json:"redis_db"`
	// BrokerID uniquely identifies this broker in the cluster and must be unique
	// per instance. Leave it EMPTY when autoscaling: ToHook then derives one
	// automatically (the container hostname, or a random id if the hostname is
	// unavailable), so every replica gets a distinct id from a single shared
	// config file. Set it explicitly only when you want a fixed, human-readable
	// id (e.g. "broker-a").
	BrokerID string `yaml:"broker_id" json:"broker_id"`
	// Stream is the Redis Stream key used as the bus. Optional; the hook
	// defaults it to "mochi:bus" when empty.
	Stream string `yaml:"stream" json:"stream"`
	// MaxLen approximately caps the stream length. Optional; the hook defaults
	// it to 10000 when zero.
	MaxLen int64 `yaml:"max_len" json:"max_len"`
}

// ToHook builds a bus hook load config for the given server. The server must be
// created with InlineClient=true (FromBytes does this automatically when a bus
// block is present) or the bus cannot re-inject remote messages.
//
// When BrokerID is empty a unique id is generated (see resolveBrokerID), so an
// autoscaled fleet can run from one shared config file.
func (bc *BusConfig) ToHook(server *mqtt.Server) mqtt.HookLoadConfig {
	return mqtt.HookLoadConfig{
		Hook: new(bus.Hook),
		Config: &bus.Options{
			RedisOptions: &redis.Options{
				Addr:     bc.RedisAddr,
				Password: bc.RedisPassword,
				DB:       bc.RedisDB,
			},
			Server:   server,
			BrokerID: resolveBrokerID(bc.BrokerID),
			Stream:   bc.Stream,
			MaxLen:   bc.MaxLen,
		},
	}
}

// resolveBrokerID returns id unchanged when set. When empty it derives a unique
// id for this instance: the container/pod hostname (unique per replica in
// Docker and Kubernetes), falling back to a random xid if the hostname cannot
// be read. This lets an autoscaled fleet share one config file with an empty
// broker_id and still give every instance a distinct bus consumer group.
func resolveBrokerID(id string) string {
	if id != "" {
		return id
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return xid.New().String()
}

// HookAuthConfig contains configurations for the auth hook.
type HookAuthConfig struct {
	Ledger   auth.Ledger `yaml:"ledger" json:"ledger"`
	AllowAll bool        `yaml:"allow_all" json:"allow_all"`
}

// HookStorageConfig contains configurations for the different storage hooks.
type HookStorageConfig struct {
	Badger *badger.Options `yaml:"badger" json:"badger"`
	Bolt   *bolt.Options   `yaml:"bolt" json:"bolt"`
	Pebble *pebble.Options `yaml:"pebble" json:"pebble"`
	Redis  *storageredis.Options `yaml:"redis" json:"redis"`
}

// ToHooks converts Hook file configurations into Hooks to be added to the server.
func (hc HookConfigs) ToHooks() []mqtt.HookLoadConfig {
	var hlc []mqtt.HookLoadConfig

	if hc.Auth != nil {
		hlc = append(hlc, hc.toHooksAuth()...)
	}

	if hc.Storage != nil {
		hlc = append(hlc, hc.toHooksStorage()...)
	}

	if hc.Debug != nil {
		hlc = append(hlc, mqtt.HookLoadConfig{
			Hook:   new(debug.Hook),
			Config: hc.Debug,
		})
	}

	// The timestamp hook is always enabled: it appends a receive timestamp to
	// the payload of messages published to topics ending in "s".
	hlc = append(hlc, mqtt.HookLoadConfig{
		Hook: new(timestamp.Hook),
	})

	return hlc
}

// toHooksAuth converts auth hook configurations into auth hooks.
func (hc HookConfigs) toHooksAuth() []mqtt.HookLoadConfig {
	var hlc []mqtt.HookLoadConfig
	if hc.Auth.AllowAll {
		hlc = append(hlc, mqtt.HookLoadConfig{
			Hook: new(auth.AllowHook),
		})
	} else {
		hlc = append(hlc, mqtt.HookLoadConfig{
			Hook: new(auth.Hook),
			Config: &auth.Options{
				Ledger: &auth.Ledger{ // avoid copying sync.Locker
					Users: hc.Auth.Ledger.Users,
					Auth:  hc.Auth.Ledger.Auth,
					ACL:   hc.Auth.Ledger.ACL,
				},
			},
		})
	}
	return hlc
}

// toHooksAuth converts storage hook configurations into storage hooks.
func (hc HookConfigs) toHooksStorage() []mqtt.HookLoadConfig {
	var hlc []mqtt.HookLoadConfig
	if hc.Storage.Badger != nil {
		hlc = append(hlc, mqtt.HookLoadConfig{
			Hook:   new(badger.Hook),
			Config: hc.Storage.Badger,
		})
	}

	if hc.Storage.Bolt != nil {
		hlc = append(hlc, mqtt.HookLoadConfig{
			Hook:   new(bolt.Hook),
			Config: hc.Storage.Bolt,
		})
	}

	if hc.Storage.Redis != nil {
		hlc = append(hlc, mqtt.HookLoadConfig{
			Hook:   new(storageredis.Hook),
			Config: hc.Storage.Redis,
		})
	}

	if hc.Storage.Pebble != nil {
		hlc = append(hlc, mqtt.HookLoadConfig{
			Hook:   new(pebble.Hook),
			Config: hc.Storage.Pebble,
		})
	}
	return hlc
}

// FromBytes unmarshals a byte slice of JSON or YAML config data into a valid server options value.
// Any hooks configurations are converted into Hooks using the toHooks methods in this package.
//
// Note: the clustering bus (hooks.bus) is NOT applied here, because the bus hook
// needs the *mqtt.Server pointer which does not exist until mqtt.New. Callers
// that want the bus should use FromBytesWithBus and add the returned bus hook
// after creating the server.
func FromBytes(b []byte) (*mqtt.Options, error) {
	o, _, err := FromBytesWithBus(b)
	return o, err
}

// FromBytesWithBus is like FromBytes but also returns the parsed clustering-bus
// config (nil when no `bus:` block is present). When a bus block IS present it
// sets Options.InlineClient=true, which the bus hook requires to re-inject
// remote messages. The caller must add the bus hook after mqtt.New, e.g.:
//
//	opts, busCfg, _ := config.FromBytesWithBus(b)
//	server := mqtt.New(opts)
//	if busCfg != nil {
//	    hlc := busCfg.ToHook(server)
//	    _ = server.AddHook(hlc.Hook, hlc.Config)
//	}
func FromBytesWithBus(b []byte) (*mqtt.Options, *BusConfig, error) {
	c := new(config)
	o := mqtt.Options{}

	if len(b) == 0 {
		return nil, nil, nil
	}

	if b[0] == '{' {
		err := json.Unmarshal(b, c)
		if err != nil {
			return nil, nil, err
		}
	} else {
		err := yaml.Unmarshal(b, c)
		if err != nil {
			return nil, nil, err
		}
	}

	o = c.Options
	o.Hooks = c.HookConfigs.ToHooks()
	listenerConfigs, err := loadListenerTLS(c.Listeners)
	if err != nil {
		return nil, nil, err
	}
	o.Listeners = listenerConfigs
	o.Logger = c.LoggingConfig.ToLogger()

	busCfg := c.HookConfigs.Bus
	if busCfg != nil {
		// The bus consumer re-injects remote messages via server.Publish, which
		// requires an inline client. Turn it on so the operator does not have to
		// remember a separate options flag.
		o.InlineClient = true
	}

	return &o, busCfg, nil
}

// loadListenerTLS loads the cert/key files configured on each listener into a
// tls.Config so the listener can serve TLS (e.g. wss://). Listeners without
// both tls_cert_file and tls_key_file are left as plaintext.
func loadListenerTLS(in []listeners.Config) ([]listeners.Config, error) {
	for i := range in {
		if in[i].TLSCertFile == "" && in[i].TLSKeyFile == "" {
			continue
		}
		if in[i].TLSCertFile == "" || in[i].TLSKeyFile == "" {
			return nil, fmt.Errorf("listener %q: both tls_cert_file and tls_key_file must be set", in[i].ID)
		}
		cert, err := tls.LoadX509KeyPair(in[i].TLSCertFile, in[i].TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("listener %q: loading TLS keypair: %w", in[i].ID, err)
		}
		in[i].TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	return in, nil
}
