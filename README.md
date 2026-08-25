# kafka-lag-exporter Stuck max_lag / sum_lag Fix

**Repository:** [nwlterry/kafa_lag_exporter_fix](https://github.com/nwlterry/kafa_lag_exporter_fix)  
**Upstream project:** [seglo/kafka-lag-exporter](https://github.com/seglo/kafka-lag-exporter) (archived 17 Mar 2024)

## Problem

`kafka_consumergroup_group_max_lag` and `kafka_consumergroup_group_sum_lag` metrics **only increase** (or stay stuck at high values) even when:

- Confluent Control Center shows **clean / zero lag**
- `kafka-consumer-groups --describe` reports zero lag

This makes Prometheus / Grafana alerts and dashboards unreliable.

## Environment

| Item | Value |
|------|-------|
| Deployment | VM (self-hosted) |
| Kafka / Confluent | Confluent Platform **7.7.2** (Apache Kafka 3.7.x) |
| Exporter | seglo/kafka-lag-exporter (any recent pre-archive version) |
| Metrics | Prometheus scrape of exporter `/metrics` endpoint |

## Root Cause

Offset lag is calculated as:

```text
lag = latest_offset (AdminClient) − committed_offset (consumer group metadata)
```

- `max_lag` = highest lag across the group’s partitions  
- `sum_lag` = sum of lags across the group’s partitions  

These are pure **gauges** and should drop to 0 when consumers catch up. In practice they become sticky because:

1. **In-memory metric registry** – old `client_id` / member series are not always fully evicted (historical issue #25, residual cases remain).
2. **After transient failures** (broker restart, offset-fetch errors) the collector can keep the last high value.
3. **Inactive / empty groups** can leave the previous max/sum visible for a period (issue #44).
4. **Dynamic changes** (new topics added to a group) sometimes require a full process restart to be rediscovered (issue #504).
5. Optional **Redis lookup table** (used for time-lag interpolation) can hold stale history.

Control Center and the official CLI read live group metadata directly from the brokers, so they show the real lag while the exporter still emits stale high values.

The project has been **archived since March 2024**, so these edge cases were never fully hardened.

## Diagnosis Steps

### 1. Confirm real lag is zero

```bash
kafka-consumer-groups --bootstrap-server <brokers> \
  --command-config <client.properties> \
  --describe --group <group-name>
```

Also check Confluent Control Center → Consumers.

### 2. Inspect exporter metrics

```bash
curl -s http://<exporter-host>:8000/metrics | grep -E 'kafka_consumergroup_group_(max|sum)_lag'
```

### 3. Enable DEBUG logging (recommended)

In `application.conf` / logback / environment set the collector package to DEBUG.  
Look for “Received Offsets Snapshot” (or equivalent) lines that show:

- earliest / latest offsets
- last group (committed) offsets

Compare those numbers with the CLI output. If the snapshot already shows lag = 0 but Prometheus still shows high values, the problem is downstream (scrape / Grafana query).

### 4. Check ACLs

The exporter principal needs `DESCRIBE` on the cluster, all relevant consumer groups and topics.

## Fix & Clean-up Steps (VM)

### Primary fix – Restart the exporter

This clears the in-memory metric registry and forces a full re-discovery + recalculation.

```bash
# systemd
sudo systemctl restart kafka-lag-exporter

# Docker
docker restart <kafka-lag-exporter-container>

# Bare process – stop cleanly then start again with the same config
```

Wait 1–2 poll intervals (default 30 s) then re-check the metrics.

### Redis lookup-table clean-up (only if enabled)

If `lookup-table.redis` is enabled in the exporter config, stale history can keep time-lag (and sometimes related state) dirty.

#### Check whether Redis is installed / running

```bash
# Binary present?
command -v redis-server
redis-server --version
redis-cli --version

# Service status
systemctl status redis
systemctl status redis-server
systemctl list-units | grep -i redis

# Port 6379 listening?
ss -tlnp | grep 6379
# or
netstat -tlnp | grep 6379

# Can we talk to it?
redis-cli ping          # expect PONG
# with host/password if needed:
redis-cli -h 127.0.0.1 -p 6379 ping
redis-cli -a <password> ping
```

Quick one-liner:

```bash
command -v redis-server && redis-cli ping || echo "Redis not found or not running"
```

If Redis is **not** installed, skip the Redis steps – just restart the exporter.

#### Flush Redis keys used by the exporter

```bash
redis-cli -h <redis-host> -p <port>

# List keys (default prefix is usually "kafka-lag-exporter")
KEYS kafka-lag-exporter*

# Delete everything under the prefix
EVAL "return redis.call('del', unpack(redis.call('keys', ARGV[1])))" 0 "kafka-lag-exporter*"
```

Then restart the exporter again so it rebuilds a clean lookup table.  
You can also temporarily disable Redis in the config, restart, and re-enable later.

### Minimal clean-up that usually works

```bash
# 1. (Optional) flush Redis if enabled
redis-cli KEYS "kafka-lag-exporter*" | xargs -r redis-cli DEL
# or the EVAL version above

# 2. Restart exporter
sudo systemctl restart kafka-lag-exporter   # or docker restart ...

# 3. Wait ~60 s then re-check metrics vs Control Center / CLI
```

### Prometheus / Grafana side (optional)

- Once the exporter stops emitting the old high values, the series will eventually disappear from Prometheus.
- Ensure Grafana panels show the **current gauge** value, not `increase()` / `rate()` / max-over-long-range.

## Verification Checklist

| Check | Action |
|-------|--------|
| Exporter process healthy | `systemctl status` / `docker ps` / metrics endpoint returns data |
| Real lag from Kafka | `kafka-consumer-groups --describe` + Control Center |
| Exporter’s view | DEBUG offset snapshot or `/metrics` for `max_lag` / `sum_lag` |
| Redis (if used) | Keys under the prefix are gone or freshly written |
| ACLs | Exporter principal has DESCRIBE on groups + topics |

## Confluent Platform 7.7.2 Native Alternative

Because you are on Confluent Platform ≥ 7.5 you can enable the official consumer-lag emitter on the brokers:

```properties
# broker properties
confluent.consumer.lag.emitter.enabled=true
confluent.consumer.lag.emitter.interval.ms=60000   # or lower
```

This exposes `consumer-lag-offsets` via JMX. It is often cleaner than a third-party exporter and does not suffer from the same stale-metric issues. Scrape it with the JMX exporter / Prometheus if desired.

## Long-term Recommendations

The upstream project is **archived**. Repeated restarts are a band-aid. Consider migrating to a maintained alternative:

| Project | Notes |
|---------|-------|
| [themoah/klag](https://github.com/themoah/klag) | Explicitly inspired by kafka-lag-exporter |
| [softwaremill/klag-exporter](https://github.com/softwaremill/klag-exporter) | Rust, direct timestamp sampling |
| [danielqsj/kafka_exporter](https://github.com/danielqsj/kafka_exporter) | Simpler lag model, widely used |
| Confluent native JMX lag metrics | Official, no extra process |

## References

- Upstream repo (archived): https://github.com/seglo/kafka-lag-exporter
- Key historical issues: #25 (metrics not reset after consumer restart), #44 (inactive groups keep max lag), #504 (restart needed for new topics), #154 (evict metrics on collector failure)
- Confluent consumer lag docs: https://docs.confluent.io/platform/current/monitor/monitor-consumer-lag.html

---

*Documented from operational diagnosis on VM + Confluent Platform 7.7.2 (Aug 2026).*
