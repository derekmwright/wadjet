# Network Analytics Workflow

This guide walks through the complete end-to-end pipeline: collecting network device logs, processing them through Bento, querying with Caelum, and integrating with custom applications.

## Pipeline Overview

```mermaid
graph TD
    subgraph DS ["DATA SOURCES"]
        R["Routers<br/><sub>Syslog UDP 514</sub>"]
        SW["Switches<br/><sub>SNMP Traps UDP 162</sub>"]
        FW["Firewalls<br/><sub>NetFlow UDP 2055</sub>"]
        LB["Load Balancers<br/><sub>JSON API</sub>"]
        WL["Wireless<br/><sub>SNMP Polling</sub>"]
    end

    subgraph BE ["BENTO (Stream Processing)"]
        IN["Input<br/><sub>UDP, Kafka, HTTP</sub>"]
        PA["Parse / Decode<br/><sub>syslog, JSON, NetFlow</sub>"]
        EN["Enrich / Transform<br/><sub>add fields, partition</sub>"]
        OUT["Output<br/><sub>Parquet to S3</sub>"]
        IN --> PA --> EN --> OUT
    end

    subgraph CA ["CAELUM (Query Engine)"]
        CAT["Catalog<br/><sub>table schemas, manifests</sub>"]
        SQL["SQL Engine<br/><sub>parse, plan, execute</sub>"]
        API["HTTP API<br/><sub>REST + Prometheus</sub>"]
    end

    subgraph APP ["YOUR APPLICATION"]
        DA["Dashboards"]
        AL["Alerting"]
        RE["Reports"]
        AU["Automation"]
        SI["SIEM"]
    end

    DS --> IN
    OUT -- "Parquet files on S3" --> CA
    API -- "JSON over HTTP" --> APP
```

## Step 1: Set Up Infrastructure

### MinIO (Object Storage)

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v /data/minio:/data \
  minio/minio server /data --console-address ":9001"

# Create the bucket
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/caelum
```

### Caelum Server

```bash
./caelum serve \
  --mode standalone \
  --http-addr :8080 \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket caelum \
  --config caelum.yaml
```

## Step 2: Configure Bento Pipelines

### Firewall Syslog Pipeline

Collects syslog from firewalls, parses structured fields, writes Parquet to S3.

```yaml
# bento-firewall-syslog.yaml
input:
  socket_server:
    network: udp
    address: 0.0.0.0:5514
    codec: lines

pipeline:
  processors:
    # Parse syslog header
    - mapping: |
        let parsed = this.parse_log("syslog_rfc5424")
        root.timestamp = $parsed.timestamp
        root.device = $parsed.hostname
        root.app = $parsed.app_name
        root.severity = $parsed.severity
        root.raw_message = $parsed.message
        root.date = $parsed.timestamp.format_timestamp("2006-01-02")

    # Extract firewall-specific fields from the message body
    # Example: "SRC=10.0.1.5 DST=192.168.1.100 PROTO=TCP SPT=54321 DPT=443 ACTION=ALLOW"
    - mapping: |
        root = this
        let msg = this.raw_message
        root.src_ip = $msg.re_find_all("SRC=([0-9.]+)").index(0).or("0.0.0.0")
        root.dst_ip = $msg.re_find_all("DST=([0-9.]+)").index(0).or("0.0.0.0")
        root.protocol = $msg.re_find_all("PROTO=(\\w+)").index(0).or("UNKNOWN")
        root.src_port = $msg.re_find_all("SPT=(\\d+)").index(0).or("0").number()
        root.dst_port = $msg.re_find_all("DPT=(\\d+)").index(0).or("0").number()
        root.action = $msg.re_find_all("ACTION=(\\w+)").index(0).or("UNKNOWN")

    # Classify traffic
    - mapping: |
        root = this
        root.traffic_class = match this.dst_port {
          443 => "HTTPS",
          80 => "HTTP",
          53 => "DNS",
          22 => "SSH",
          25 => "SMTP",
          _ => "OTHER",
        }

output:
  aws_s3:
    bucket: caelum
    path: 'data/firewall_logs/date=${! this.date }/device=${! this.device }/part-${! uuid_v4() }.parquet'
    batching:
      count: 100000
      period: 30s
    codec: parquet
    parquet_encoding:
      schema:
        - name: timestamp
          type: INT64
          annotation: TIMESTAMP_MILLIS
        - name: device
          type: UTF8
        - name: severity
          type: UTF8
        - name: src_ip
          type: UTF8
        - name: dst_ip
          type: UTF8
        - name: protocol
          type: UTF8
        - name: src_port
          type: INT32
        - name: dst_port
          type: INT32
        - name: action
          type: UTF8
        - name: traffic_class
          type: UTF8
        - name: raw_message
          type: UTF8
        - name: date
          type: UTF8
    region: us-east-1
    endpoint: http://localhost:9000
    credentials:
      id: minioadmin
      secret: minioadmin
    force_path_style_urls: true
```

### Switch/Router NetFlow Pipeline

```yaml
# bento-netflow.yaml
input:
  kafka:
    addresses:
      - kafka.internal:9092
    topics:
      - netflow-v9
    consumer_group: caelum-netflow

pipeline:
  processors:
    - mapping: |
        root.timestamp = this.TimeReceived
        root.date = (this.TimeReceived / 1000).format_timestamp("2006-01-02")
        root.src_ip = this.SrcAddr
        root.dst_ip = this.DstAddr
        root.src_port = this.SrcPort
        root.dst_port = this.DstPort
        root.protocol = match this.Proto {
          6 => "TCP", 17 => "UDP", 1 => "ICMP", 47 => "GRE", _ => this.Proto.string()
        }
        root.bytes = this.Bytes
        root.packets = this.Packets
        root.tcp_flags = this.TCPFlags
        root.tos = this.Tos
        root.input_snmp = this.InIf
        root.output_snmp = this.OutIf
        root.exporter = this.SamplerAddress
        root.next_hop = this.NextHop
        root.src_as = this.SrcAS
        root.dst_as = this.DstAS

output:
  aws_s3:
    bucket: caelum
    path: 'data/netflow/date=${! this.date }/exporter=${! this.exporter }/part-${! uuid_v4() }.parquet'
    batching:
      count: 250000
      period: 30s
    codec: parquet
    parquet_encoding:
      schema:
        - name: timestamp
          type: INT64
          annotation: TIMESTAMP_MILLIS
        - name: src_ip
          type: UTF8
        - name: dst_ip
          type: UTF8
        - name: src_port
          type: INT32
        - name: dst_port
          type: INT32
        - name: protocol
          type: UTF8
        - name: bytes
          type: INT64
        - name: packets
          type: INT64
        - name: tcp_flags
          type: INT32
        - name: tos
          type: INT32
        - name: input_snmp
          type: INT32
        - name: output_snmp
          type: INT32
        - name: exporter
          type: UTF8
        - name: next_hop
          type: UTF8
        - name: src_as
          type: INT64
        - name: dst_as
          type: INT64
        - name: date
          type: UTF8
    region: us-east-1
    endpoint: http://localhost:9000
    credentials:
      id: minioadmin
      secret: minioadmin
    force_path_style_urls: true
```

### Device Inventory Sync

Periodically pull device inventory from your CMDB/NMS and load it as a lookup table:

```yaml
# bento-device-inventory.yaml
input:
  generate:
    interval: 1h
    mapping: 'root = ""'

pipeline:
  processors:
    # Fetch device inventory from your NMS/CMDB API
    - http:
        url: http://nms.internal/api/v1/devices
        verb: GET
        headers:
          Authorization: "Bearer ${NMS_API_KEY}"
    - unarchive:
        format: json_array
    - mapping: |
        root.ip_address = this.management_ip
        root.hostname = this.hostname
        root.vendor = this.vendor
        root.model = this.model
        root.os_version = this.os_version
        root.location = this.site_name
        root.role = this.device_role
        root.region = this.region
        root.environment = this.environment
        root.last_seen = now()

output:
  aws_s3:
    bucket: caelum
    path: 'data/device_inventory/snapshot-${! now().format_timestamp("2006-01-02T15-04-05") }.parquet'
    codec: parquet
    parquet_encoding:
      schema:
        - name: ip_address
          type: UTF8
        - name: hostname
          type: UTF8
        - name: vendor
          type: UTF8
        - name: model
          type: UTF8
        - name: os_version
          type: UTF8
        - name: location
          type: UTF8
        - name: role
          type: UTF8
        - name: region
          type: UTF8
        - name: environment
          type: UTF8
        - name: last_seen
          type: INT64
          annotation: TIMESTAMP_MILLIS
    region: us-east-1
    endpoint: http://localhost:9000
    credentials:
      id: minioadmin
      secret: minioadmin
    force_path_style_urls: true
```

## Step 3: Register Tables in Caelum

After Bento starts writing data, register the tables so Caelum can query them.

```go
// register_tables.go
package main

import (
    "context"
    "log"

    "github.com/derekmwright/caelum/internal/storage/objstore"
    "github.com/derekmwright/caelum/internal/storage/parquet"
    "github.com/derekmwright/caelum/caelum"
)

func main() {
    ctx := context.Background()

    store, err := objstore.NewMinIOStore(ctx, objstore.MinIOConfig{
        Endpoint:  "localhost:9000",
        AccessKey: "minioadmin",
        SecretKey: "minioadmin",
    })
    if err != nil {
        log.Fatal(err)
    }

    db, err := caelum.Open(ctx, caelum.Config{
        Store:  store,
        Bucket: "caelum",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Firewall logs
    db.CreateTable(ctx, "firewall_logs", parquet.Schema{
        Columns: []parquet.Column{
            {Name: "timestamp", Type: parquet.TypeTimestamp},
            {Name: "device", Type: parquet.TypeString},
            {Name: "severity", Type: parquet.TypeString},
            {Name: "src_ip", Type: parquet.TypeString},
            {Name: "dst_ip", Type: parquet.TypeString},
            {Name: "protocol", Type: parquet.TypeString},
            {Name: "src_port", Type: parquet.TypeInt32},
            {Name: "dst_port", Type: parquet.TypeInt32},
            {Name: "action", Type: parquet.TypeString},
            {Name: "traffic_class", Type: parquet.TypeString},
            {Name: "raw_message", Type: parquet.TypeString},
        },
    }, []string{"date", "device"})

    // NetFlow
    db.CreateTable(ctx, "netflow", parquet.Schema{
        Columns: []parquet.Column{
            {Name: "timestamp", Type: parquet.TypeTimestamp},
            {Name: "src_ip", Type: parquet.TypeString},
            {Name: "dst_ip", Type: parquet.TypeString},
            {Name: "src_port", Type: parquet.TypeInt32},
            {Name: "dst_port", Type: parquet.TypeInt32},
            {Name: "protocol", Type: parquet.TypeString},
            {Name: "bytes", Type: parquet.TypeInt64},
            {Name: "packets", Type: parquet.TypeInt64},
            {Name: "tcp_flags", Type: parquet.TypeInt32},
            {Name: "tos", Type: parquet.TypeInt32},
            {Name: "input_snmp", Type: parquet.TypeInt32},
            {Name: "output_snmp", Type: parquet.TypeInt32},
            {Name: "exporter", Type: parquet.TypeString},
            {Name: "next_hop", Type: parquet.TypeString},
            {Name: "src_as", Type: parquet.TypeInt64},
            {Name: "dst_as", Type: parquet.TypeInt64},
        },
    }, []string{"date", "exporter"})

    // Device inventory
    db.CreateTable(ctx, "device_inventory", parquet.Schema{
        Columns: []parquet.Column{
            {Name: "ip_address", Type: parquet.TypeString},
            {Name: "hostname", Type: parquet.TypeString},
            {Name: "vendor", Type: parquet.TypeString},
            {Name: "model", Type: parquet.TypeString},
            {Name: "os_version", Type: parquet.TypeString},
            {Name: "location", Type: parquet.TypeString},
            {Name: "role", Type: parquet.TypeString},
            {Name: "region", Type: parquet.TypeString},
            {Name: "environment", Type: parquet.TypeString},
            {Name: "last_seen", Type: parquet.TypeTimestamp},
        },
    }, nil)

    log.Println("Tables registered successfully")
}
```

## Step 4: Query Network Data

### Via the Interactive Shell

```bash
./caelum shell --endpoint localhost:9000 --access-key minioadmin --secret-key minioadmin --bucket caelum
```

```sql
-- Top denied connections by source
caelum> SELECT src_ip, COUNT(*) AS denied
        FROM firewall_logs
        WHERE date = '2026-03-15' AND action = 'DENY'
        GROUP BY src_ip
        ORDER BY denied DESC
        LIMIT 20;

-- Bandwidth by AS path
caelum> SELECT src_as, dst_as, SUM(bytes) AS total_bytes, SUM(packets) AS total_pkts
        FROM netflow
        WHERE date = '2026-03-15'
        GROUP BY src_as, dst_as
        ORDER BY total_bytes DESC
        LIMIT 25;

-- Join flow data with device inventory
caelum> SELECT d.hostname, d.location, d.role,
               SUM(n.bytes) AS total_bytes,
               COUNT(*) AS flow_count
        FROM netflow n
        JOIN device_inventory d ON n.exporter = d.ip_address
        WHERE n.date = '2026-03-15'
        GROUP BY d.hostname, d.location, d.role
        ORDER BY total_bytes DESC;
```

### Via HTTP API

```bash
# Firewall deny rate by hour
curl -s -X POST http://localhost:8080/v1/queries \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT CAST(timestamp / 3600000 * 3600000 AS Timestamp) AS hour, COUNT(*) AS denies FROM firewall_logs WHERE date = '\''2026-03-15'\'' AND action = '\''DENY'\'' GROUP BY CAST(timestamp / 3600000 * 3600000 AS Timestamp) ORDER BY hour"
  }' | jq .
```

## Step 5: Build Application Integrations

### Python Dashboard Backend

```python
import requests
from datetime import date, timedelta
from flask import Flask, jsonify

app = Flask(__name__)
CAELUM = "http://localhost:8080"

def caelum_query(sql: str) -> list:
    resp = requests.post(f"{CAELUM}/v1/queries", json={"sql": sql})
    resp.raise_for_status()
    return resp.json()["rows"]

@app.route("/api/dashboard/overview")
def dashboard_overview():
    today = date.today().isoformat()

    # Run multiple queries for the dashboard
    top_talkers = caelum_query(f"""
        SELECT src_ip, SUM(bytes) AS total_bytes, COUNT(*) AS flows
        FROM netflow WHERE date = '{today}'
        GROUP BY src_ip ORDER BY total_bytes DESC LIMIT 10
    """)

    denied_connections = caelum_query(f"""
        SELECT src_ip, dst_ip, dst_port, COUNT(*) AS attempts
        FROM firewall_logs
        WHERE date = '{today}' AND action = 'DENY'
        GROUP BY src_ip, dst_ip, dst_port
        ORDER BY attempts DESC LIMIT 10
    """)

    traffic_by_protocol = caelum_query(f"""
        SELECT protocol, SUM(bytes) AS total_bytes, COUNT(*) AS flows
        FROM netflow WHERE date = '{today}'
        GROUP BY protocol ORDER BY total_bytes DESC
    """)

    return jsonify({
        "date": today,
        "top_talkers": top_talkers,
        "denied_connections": denied_connections,
        "traffic_by_protocol": traffic_by_protocol,
    })

@app.route("/api/dashboard/device/<hostname>")
def device_detail(hostname):
    today = date.today().isoformat()

    device_info = caelum_query(f"""
        SELECT * FROM device_inventory WHERE hostname = '{hostname}' LIMIT 1
    """)

    device_traffic = caelum_query(f"""
        SELECT
            n.dst_ip, n.dst_port, n.protocol,
            SUM(n.bytes) AS total_bytes, COUNT(*) AS flows
        FROM netflow n
        JOIN device_inventory d ON n.exporter = d.ip_address
        WHERE d.hostname = '{hostname}' AND n.date = '{today}'
        GROUP BY n.dst_ip, n.dst_port, n.protocol
        ORDER BY total_bytes DESC LIMIT 50
    """)

    return jsonify({
        "device": device_info[0] if device_info else None,
        "traffic": device_traffic,
    })

if __name__ == "__main__":
    app.run(port=9090)
```

### Go Alerting Service

```go
// alerting.go — monitors Caelum for anomalous patterns and sends alerts
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/derekmwright/caelum/internal/storage/objstore"
    "github.com/derekmwright/caelum/caelum"
)

type Alert struct {
    Severity string
    Title    string
    Detail   string
    Time     time.Time
}

func main() {
    ctx := context.Background()
    store, _ := objstore.NewMinIOStore(ctx, objstore.MinIOConfig{
        Endpoint:  "localhost:9000",
        AccessKey: "minioadmin",
        SecretKey: "minioadmin",
    })
    db, _ := caelum.Open(ctx, caelum.Config{
        Store:  store,
        Bucket: "caelum",
    })

    ticker := time.NewTicker(5 * time.Minute)
    for range ticker.C {
        alerts := runChecks(ctx, db)
        for _, alert := range alerts {
            sendAlert(alert) // Implement: Slack, PagerDuty, email, etc.
        }
    }
}

func runChecks(ctx context.Context, db *caelum.DB) []Alert {
    var alerts []Alert
    today := time.Now().Format("2006-01-02")

    // Check 1: Port scan detection
    result, _ := db.Query(ctx, fmt.Sprintf(`
        SELECT src_ip, COUNT(DISTINCT dst_port) AS ports, COUNT(*) AS flows
        FROM firewall_logs
        WHERE date = '%s' AND action = 'DENY'
        GROUP BY src_ip
        HAVING COUNT(DISTINCT dst_port) > 100
    `, today))
    for _, row := range result.Rows {
        alerts = append(alerts, Alert{
            Severity: "HIGH",
            Title:    fmt.Sprintf("Port scan detected from %s", row["src_ip"]),
            Detail:   fmt.Sprintf("%v unique ports probed, %v total flows", row["ports"], row["flows"]),
            Time:     time.Now(),
        })
    }

    // Check 2: Unusual outbound traffic volume
    result, _ = db.Query(ctx, fmt.Sprintf(`
        SELECT src_ip, SUM(bytes) AS total_bytes
        FROM netflow
        WHERE date = '%s'
        GROUP BY src_ip
        HAVING SUM(bytes) > 10737418240
    `, today)) // > 10 GB
    for _, row := range result.Rows {
        alerts = append(alerts, Alert{
            Severity: "MEDIUM",
            Title:    fmt.Sprintf("High traffic volume from %s", row["src_ip"]),
            Detail:   fmt.Sprintf("%v bytes transferred today", row["total_bytes"]),
            Time:     time.Now(),
        })
    }

    // Check 3: Denied connections spike
    result, _ = db.Query(ctx, fmt.Sprintf(`
        SELECT COUNT(*) AS deny_count
        FROM firewall_logs
        WHERE date = '%s' AND action = 'DENY'
    `, today))
    if len(result.Rows) > 0 {
        if count, ok := result.Rows[0]["deny_count"].(int64); ok && count > 100000 {
            alerts = append(alerts, Alert{
                Severity: "HIGH",
                Title:    "Firewall deny spike",
                Detail:   fmt.Sprintf("%d denied connections today (threshold: 100,000)", count),
                Time:     time.Now(),
            })
        }
    }

    return alerts
}

func sendAlert(a Alert) {
    log.Printf("[%s] %s: %s — %s", a.Severity, a.Time.Format(time.RFC3339), a.Title, a.Detail)
    // TODO: integrate with Slack, PagerDuty, OpsGenie, etc.
}
```

### Grafana Integration

Caelum's HTTP API can be used as a JSON data source in Grafana via the [JSON API plugin](https://grafana.com/grafana/plugins/marcusolsson-json-datasource/):

1. Install the Grafana JSON API data source plugin
2. Add a new data source:
   - **URL**: `http://caelum.internal:8080`
   - **Custom headers**: `Authorization: Bearer <api-key>`
3. Create a dashboard panel with a query like:

```
POST /v1/queries
{
  "sql": "SELECT CAST(timestamp / 3600000 * 3600000 AS Timestamp) AS time, SUM(bytes) AS bytes FROM netflow WHERE date = '${__from:date:YYYY-MM-DD}' GROUP BY CAST(timestamp / 3600000 * 3600000 AS Timestamp) ORDER BY time"
}
```

4. Map the `time` field to the time axis and `bytes` to the value axis

## Step 6: Production Checklist

### Infrastructure
- [ ] S3 storage with replication enabled (MinIO erasure coding or AWS S3 standard)
- [ ] Caelum deployed in distributed mode with 3+ workers
- [ ] Bento pipelines deployed as systemd services or Kubernetes deployments
- [ ] Reverse proxy (nginx/Caddy) in front of Caelum with TLS termination

### Data Pipeline
- [ ] Syslog collection configured on all network devices
- [ ] NetFlow/IPFIX export enabled on core routers and switches
- [ ] Bento batching tuned for 64–256 MB Parquet files
- [ ] Partition keys chosen (typically `date` + one device/region dimension)
- [ ] All tables registered in the Caelum catalog

### Security
- [ ] Authentication configured (JWT or mTLS for production)
- [ ] Roles defined with least-privilege access
- [ ] Cell-level policies for sensitive fields (source IPs, raw messages)
- [ ] API keys rotated on schedule

### Monitoring
- [ ] Prometheus scraping Caelum `/metrics` endpoint
- [ ] Grafana dashboards for query latency, rows scanned, error rate
- [ ] Alerting on query failures and high latency
- [ ] Bento pipeline health monitoring

### Operations
- [ ] S3 lifecycle policies for data retention (e.g., 90-day expiry)
- [ ] Orphaned Parquet file cleanup job
- [ ] Backup strategy for the NATS JetStream store (catalog KV)
- [ ] Runbook for common failure scenarios
