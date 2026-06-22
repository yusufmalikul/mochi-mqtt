# timestamp hook

Appends a millisecond receive timestamp to the payload of published messages
whose **final topic segment is `s`** (e.g. `test/s`, `a/b/s`, or the bare topic
`s`). Topics that merely end in the letter `s` (`sensors`, `status`, `bus`) are
**not** modified.

A message published to `test/s` with payload `hello` is delivered to
subscribers as:

```
hello:1782128650648
```

where the suffix is `time.Now().UnixMilli()`. This mirrors behaviour migrated
from a custom EMQX fork (`emqx_message.erl`, `make/4`).

## Testing

### Unit test (fast, no broker)

Calls `OnPublish` directly with a fake packet and checks the rewritten payload:

```bash
go test ./hooks/timestamp/ -v
```

`TestStampsTopicWithFinalSegmentS` covers `test/s`, `a/b/s`, `s`.
`TestLeavesOtherTopicsUnchanged` covers `sensors`, `status`, `bus`, `sensor/temp`.

### Manual end-to-end test (real broker + client)

Tests the full path — broker startup, hook registration in `cmd/main.go`, and
delivery to a real MQTT client. Requires the [MQTTX CLI](https://mqttx.app/cli)
(`brew install emqx/mqttx/mqttx-cli`).

1. Start the broker (TCP listener on `:1883`):

   ```bash
   go run ./cmd -tcp :1883
   ```

2. In another terminal, subscribe to the topics under test (`-v` prints the
   topic alongside each payload):

   ```bash
   mqttx sub -h 127.0.0.1 -p 1883 -v \
     -t 'test/s' -t 'sensors' -t 'status' -t 'sensor/temp' -t 's'
   ```

3. In a third terminal, publish to each:

   ```bash
   for topic in 'test/s' 'sensors' 'status' 'sensor/temp' 's'; do
     mqttx pub -h 127.0.0.1 -p 1883 -t "$topic" -m 'hello'
   done
   ```

4. Expected output in the subscriber:

   | Topic         | Payload                | Stamped? |
   |---------------|------------------------|----------|
   | `test/s`      | `hello:<unix-millis>`  | yes      |
   | `sensors`     | `hello`                | no       |
   | `status`      | `hello`                | no       |
   | `sensor/temp` | `hello`                | no       |
   | `s`           | `hello:<unix-millis>`  | yes      |
