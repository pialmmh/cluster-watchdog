# SIGTRAN Health Check REST API

REST API for checking SIGTRAN link health status. Use this to determine if a link is ready to handle SMS traffic.

## Endpoint

```
GET http://{host}:{port}/health
```

## Link Endpoints (BDCOM)

| Link | Endpoint |
|------|----------|
| borak-1 | `http://10.10.199.1:8282/health` |
| borak-2 | `http://10.10.199.1:8283/health` |
| khawaja-1 | `http://10.10.199.1:8284/health` |
| khawaja-2 | `http://10.10.199.1:8285/health` |

## Response

### Success (200 OK) - Link Ready

```json
{
    "link": "borak-1",
    "timestamp": "2025/12/21 03:15:59",
    "m3ua": {
        "sctpUp": true,
        "aspUp": true,
        "asActive": true
    },
    "ready": true
}
```

### Failure (503 Service Unavailable) - Link Not Ready

```json
{
    "link": "borak-1",
    "timestamp": "2025/12/21 03:15:59",
    "m3ua": {
        "sctpUp": false,
        "aspUp": false,
        "asActive": false
    },
    "ready": false
}
```

## Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `link` | string | Link name identifier |
| `timestamp` | string | Server timestamp (yyyy/MM/dd HH:mm:ss) |
| `m3ua.sctpUp` | boolean | SCTP transport layer connected |
| `m3ua.aspUp` | boolean | M3UA ASP is UP (handshake complete) |
| `m3ua.asActive` | boolean | M3UA AS is ACTIVE (ready for traffic) |
| `ready` | boolean | Overall status: `sctpUp && aspUp && asActive` |

## Ready Condition

A link is **ready** (`ready: true`) when ALL conditions are met:
- `sctpUp: true` - SCTP association is connected
- `aspUp: true` - ASP completed handshake with remote STP
- `asActive: true` - AS is in ACTIVE state

## Usage Examples

### curl

```bash
# Check single link
curl -s http://10.10.199.1:8282/health | jq .

# Check if ready (exit code 0 if ready, 22 if not)
curl -sf http://10.10.199.1:8282/health > /dev/null && echo "READY" || echo "NOT READY"

# Check all links
for port in 8282 8283 8284 8285; do
    echo -n "Port $port: "
    curl -s http://10.10.199.1:$port/health | jq -r '.ready'
done
```

### Java (HttpClient)

```java
import java.net.http.*;
import java.net.URI;
import com.google.gson.*;

public boolean isLinkReady(String host, int port) {
    try {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://" + host + ":" + port + "/health"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();

        HttpResponse<String> response = client.send(request,
            HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
            return json.get("ready").getAsBoolean();
        }
        return false;
    } catch (Exception e) {
        return false;
    }
}
```

### Python

```python
import requests

def is_link_ready(host, port, timeout=5):
    try:
        resp = requests.get(f"http://{host}:{port}/health", timeout=timeout)
        if resp.status_code == 200:
            return resp.json().get("ready", False)
        return False
    except:
        return False

# Check all BDCOM links
links = [
    ("borak-1", "10.10.199.1", 8282),
    ("borak-2", "10.10.199.1", 8283),
    ("khawaja-1", "10.10.199.1", 8284),
    ("khawaja-2", "10.10.199.1", 8285),
]

for name, host, port in links:
    ready = is_link_ready(host, port)
    print(f"{name}: {'READY' if ready else 'NOT READY'}")
```

## Failure Scenarios

| Scenario | sctpUp | aspUp | asActive | ready |
|----------|--------|-------|----------|-------|
| Normal operation | true | true | true | true |
| SCTP disconnected | false | false | false | false |
| ASP handshake pending | true | false | false | false |
| AS not active yet | true | true | false | false |
| Link process dead | Connection refused | | | |

## Polling Recommendations

| Use Case | Interval | Notes |
|----------|----------|-------|
| Load balancer health | 5-10s | Standard health check |
| Routing decisions | 10-30s | Cache result briefly |
| Alerting/monitoring | 30-60s | Avoid alert storms |

Polling faster than 1 second is generally unnecessary - M3UA state changes are infrequent.

## Error Handling

| HTTP Status | Meaning | Action |
|-------------|---------|--------|
| 200 | Link ready | Route traffic to this link |
| 503 | Link not ready | Do not route traffic |
| Connection refused | Process not running | Mark link as dead |
| Timeout | Network issue | Retry or mark unavailable |

## Integration with Routesphere

```java
// Before routing SMS, check link health
String[] links = {"borak-1", "borak-2", "khawaja-1", "khawaja-2"};
int[] ports = {8282, 8283, 8284, 8285};

List<String> availableLinks = new ArrayList<>();
for (int i = 0; i < links.length; i++) {
    if (isLinkReady("10.10.199.1", ports[i])) {
        availableLinks.add(links[i]);
    }
}

if (availableLinks.isEmpty()) {
    // No links available - queue or reject SMS
} else {
    // Route to one of the available links
    String selectedLink = availableLinks.get(random.nextInt(availableLinks.size()));
}
```
