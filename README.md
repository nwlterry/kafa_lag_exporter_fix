# kafka-lag-exporter / Control Center Lag Fix

**Repository:** [nwlterry/kafa_lag_exporter_fix](https://github.com/nwlterry/kafa_lag_exporter_fix)  
**Upstream exporter:** [seglo/kafka-lag-exporter](https://github.com/seglo/kafka-lag-exporter) (archived 17 Mar 2024)

## Problem

Observed symptoms:

- `kafka_consumergroup_group_max_lag` and `kafka_consumergroup_group_sum_lag` appeared stuck / only increasing
- Confluent **Control Center** consumer offsets / lag were **not updating** (stale view)
- Need a reliable way to force correct lag visibility on Confluent Platform 7.7.2 (VM, self-hosted)

## Environment

| Item | Value |
|------|-------|
| Deployment | VM (self-hosted) |
| Kafka / Confluent | Confluent Platform **7.7.2** (Apache Kafka 3.7.x) |
| Exporter | seglo/kafka-lag-exporter (pre-archive) |
| Metrics / UI | Prometheus scrape of exporter; Confluent Control Center |

## Revised root cause (confirmed)

After following diagnosis steps, the primary issue was:

> **Control Center offsets / lag not updating**

On Confluent Platform ≥ 7.5, broker-side consumer lag emission is **disabled by default**. Without it, Control Center (and related monitoring) can show stale or incomplete lag/offset data.

Secondary / related issue:

- kafka-lag-exporter can also keep **stale high `max_lag` / `sum_lag`** in its in-memory registry (and optional Redis lookup table) even when real lag is zero. A process restart (and Redis flush if used) clears that.

**Source of truth for lag:** always cross-check with:

```bash
kafka-consumer-groups --bootstrap-server <brokers> \
  --command-config <client.properties> \
  --describe --group <group-name>
```

---

## Fix 1 — Control Center offsets / lag not updating (primary)

### 1. Enable the consumer lag emitter on every broker

This is **off by default**. Add to broker properties on **all brokers** (`server.properties` or your path under `$CONFLUENT_HOME/etc/kafka/`):

```properties
confluent.consumer.lag.emitter.enabled=true
confluent.consumer.lag.emitter.interval.ms=60000
```

- Default interval is 60000 ms (1 minute). Use a lower value (e.g. `30000`) for faster UI updates if needed.
- Apply on **all brokers**, then **rolling restart** brokers.

This emits lag as the `consumer-lag-offsets` MBean and feeds monitoring components properly.

### 2. Confirm Control Center Consumers view and mode

In Control Center properties (`control-center.properties` or production/dev variant under `$CONFLUENT_HOME/etc/confluent-control-center/`):

```properties
confluent.controlcenter.consumers.view.enable=true
```

- Control Center must run in **Normal mode** (not Reduced infrastructure mode). Reduced mode does not provide full metrics/lag views.
- Restart Control Center after changes:

```bash
# example – adjust to your install
sudo systemctl restart confluent-control-center
# or
$CONFLUENT_HOME/bin/control-center-stop
$CONFLUENT_HOME/bin/control-center-start $CONFLUENT_HOME/etc/confluent-control-center/control-center.properties
```

### 3. Check that Control Center itself is not lagging

If Control Center falls behind on its internal topics, the UI shows old offsets.

```bash
kafka-consumer-groups --bootstrap-server <brokers> \
  --describe --group _confluent-controlcenter-<c3-id>-1
```

Watch lag on topics such as `MetricsAggregateStore`, `aggregate-rekey`, and monitoring/metrics interceptor topics.

If lag is large and growing:

- Increase CPU/memory for Control Center
- Optionally skip old backlog so C3 catches up faster:

```properties
confluent.monitoring.interceptor.topic.skip.backlog.minutes=0
confluent.metrics.topic.skip.backlog.minutes=0
```

Then restart Control Center. (Use carefully on large histories.)

### 4. UI / data caveats

- After enabling the emitter or config changes, wait a few minutes for Control Center to refresh.
- Large numbers of groups/partitions can show placeholders (e.g. `-999` for currentOffset) until data is fully loaded; use CLI for accuracy.
- Groups with no active members, or groups using manual `assign()`, may display differently.
- Hard-refresh the browser if the page looks frozen.

### Minimal Control Center fix sequence

1. Set on **all brokers**:
   ```properties
   confluent.consumer.lag.emitter.enabled=true
   confluent.consumer.lag.emitter.interval.ms=60000
   ```
2. Rolling restart brokers.
3. Confirm `confluent.controlcenter.consumers.view.enable=true` and **Normal mode**.
4. Restart Control Center.
5. Wait 1–2 emitter intervals + a few minutes; recheck **Clients → Consumer Lag**.
6. Cross-check with `kafka-consumer-groups --describe`.
7. If UI still lags, inspect Control Center’s own consumer group lag and resources / skip-backlog as above.

### Key properties (quick reference)

| Component | Property | Purpose |
|-----------|----------|---------|
| Broker | `confluent.consumer.lag.emitter.enabled=true` | Emit lag for monitoring (default **false**) |
| Broker | `confluent.consumer.lag.emitter.interval.ms` | How often lag is calculated (default 60000) |
| Control Center | `confluent.controlcenter.consumers.view.enable=true` | Show Consumers / lag UI |
| Control Center | Normal mode (not Reduced) | Required for metrics/lag views |
| Control Center | `*.skip.backlog.minutes=0` | Help C3 catch up if behind |

---

## Fix 2 — kafka-lag-exporter stuck max_lag / sum_lag (secondary)

If CLI and Control Center show correct lag but exporter gauges stay high:

### Root cause (exporter)

Offset lag is:

```text
lag = latest_offset (AdminClient) − committed_offset (group metadata)
```

- `max_lag` = max across partitions  
- `sum_lag` = sum across partitions  

Gauges can stick because of:

1. In-memory metric registry not fully evicting old `client_id` / member series
2. Transient AdminClient / offset-fetch failures leaving last high value
3. Inactive / empty groups keeping previous max/sum
4. Dynamic topic changes sometimes needing process restart
5. Optional Redis lookup table holding stale history

Upstream project archived March 2024 — these edge cases were never fully fixed.

### Diagnosis

```bash
# Real lag
kafka-consumer-groups --bootstrap-server <brokers> \
  --command-config <client.properties> \
  --describe --group <group-name>

# Exporter metrics
curl -s http://<exporter-host>:8000/metrics | grep -E 'kafka_consumergroup_group_(max|sum)_lag'
```

Enable DEBUG on the collector package and compare “Received Offsets Snapshot” (earliest / latest / group offsets) with CLI. Exporter principal needs `DESCRIBE` on cluster, groups, and topics.

### Primary clean-up — restart exporter

```bash
# systemd
sudo systemctl restart kafka-lag-exporter

# Docker
docker restart <kafka-lag-exporter-container>
```

Wait 1–2 poll intervals (default 30 s), then re-check metrics.

### Redis (only if enabled)

**Check if Redis is installed / running:**

```bash
command -v redis-server
redis-server --version
redis-cli --version

systemctl status redis
systemctl status redis-server
ss -tlnp | grep 6379

redis-cli ping   # expect PONG
```

Quick one-liner:

```bash
command -v redis-server && redis-cli ping || echo "Redis not found or not running"
```

If Redis is not installed, skip Redis steps.

**Flush exporter keys:**

```bash
redis-cli -h <redis-host> -p <port>
KEYS kafka-lag-exporter*
EVAL "return redis.call('del', unpack(redis.call('keys', ARGV[1])))" 0 "kafka-lag-exporter*"
```

Then restart the exporter again.

### Minimal exporter clean-up

```bash
# Optional Redis flush
redis-cli KEYS "kafka-lag-exporter*" | xargs -r redis-cli DEL

# Restart exporter
sudo systemctl restart kafka-lag-exporter

# Wait ~60s and compare with CLI / Control Center
```

### Prometheus / Grafana

- Prefer current **gauge** values, not `increase()` / `rate()` / max-over-long-range.
- Old series disappear after the exporter stops emitting high values.

---

## Verification checklist

| Check | Action |
|-------|--------|
| Real lag | `kafka-consumer-groups --describe` |
| Control Center lag UI | Clients → Consumer Lag (after emitter + C3 restart) |
| Broker lag emitter | Properties set + brokers restarted |
| C3 Consumers view | `confluent.controlcenter.consumers.view.enable=true`, Normal mode |
| C3 own lag | Describe `_confluent-controlcenter-...` group |
| Exporter metrics | `/metrics` for `max_lag` / `sum_lag` after restart |
| Redis (if used) | Keys flushed; exporter restarted |
| ACLs | Exporter (and tools) have DESCRIBE on groups/topics |

---

## Native Confluent alternative (recommended for alerting)

With the lag emitter enabled, scrape JMX `consumer-lag-offsets` via JMX exporter / Prometheus. This avoids third-party exporter staleness.

```properties
# brokers
confluent.consumer.lag.emitter.enabled=true
confluent.consumer.lag.emitter.interval.ms=60000
```

---

## Long-term recommendations

| Option | Notes |
|--------|-------|
| Confluent native lag emitter + JMX | Official; no extra process |
| [themoah/klag](https://github.com/themoah/klag) | Inspired by kafka-lag-exporter |
| [softwaremill/klag-exporter](https://github.com/softwaremill/klag-exporter) | Rust, direct timestamp sampling |
| [danielqsj/kafka_exporter](https://github.com/danielqsj/kafka_exporter) | Simple lag model, widely used |
| kafka-lag-exporter | Archived; restart/Redis as band-aid only |

Prefer CLI + native emitter (or a maintained exporter) for production alerts. Control Center is useful for UI but can lag under load until the emitter and C3 capacity are correct.

---

## References

- Confluent monitor consumer lag: https://docs.confluent.io/platform/current/monitor/monitor-consumer-lag.html
- Control Center consumers / lag UI: https://docs.confluent.io/control-center/current/clients/consumers.html
- Control Center troubleshooting (C3 lagging): https://docs.confluent.io/platform/7.7/control-center/installation/troubleshooting.html
- Upstream kafka-lag-exporter (archived): https://github.com/seglo/kafka-lag-exporter
- Historical exporter issues: #25 (metrics not reset), #44 (inactive groups), #504 (restart for new topics), #154 (evict on failure)

---

*Updated Aug 2026 — VM + Confluent Platform 7.7.2. Primary fix: enable broker lag emitter + ensure Control Center Normal mode and healthy internal lag. Secondary: restart kafka-lag-exporter (and flush Redis if used).*
