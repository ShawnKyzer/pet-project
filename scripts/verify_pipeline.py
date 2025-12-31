import socket
import urllib.request
import urllib.error
import time
import sys

def check_tcp(host, port, name):
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        print(f"✅ {name} (TCP {host}:{port}) is reachable")
        return True
    except Exception as e:
        print(f"❌ {name} (TCP {host}:{port}) failed: {e}")
        return False

def check_http(url, name):
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            if response.status == 200:
                print(f"✅ {name} (HTTP {url}) is healthy")
                return True
            else:
                print(f"⚠️ {name} (HTTP {url}) returned status {response.status}")
                return False
    except urllib.error.HTTPError as e:
        # Some services might return 401/403 which means they are running but auth required
        if e.code in [401, 403]:
             print(f"✅ {name} (HTTP {url}) is reachable (Auth Required: {e.code})")
             return True
        print(f"❌ {name} (HTTP {url}) failed: {e}")
        return False
    except Exception as e:
        print(f"❌ {name} (HTTP {url}) failed: {e}")
        return False

checks = [
    # Data Sources
    ("tcp", "localhost", 5433, "Pet Postgres"),
    ("tcp", "localhost", 5434, "Output Postgres"),
    
    # Prefect
    ("http", "http://localhost:4200/api/health", None, "Prefect Server Health"),
    
    # Kafka
    ("tcp", "localhost", 19092, "Kafka Broker"),
    ("http", "http://localhost:8081", None, "Schema Registry"),
    ("http", "http://localhost:9021", None, "Control Center"),
    
    # Spark
    ("http", "http://localhost:8082", None, "Spark Master UI"),
    # Spark Master Port 7077 is binary, stick to UI check or TCP
    ("tcp", "localhost", 7077, "Spark Master Port"),
    
    # OpenMetadata
    # API might be protected, check version
    ("http", "http://localhost:8585/api/v1/system/version", None, "OpenMetadata API"),
    
    # Monitoring
    ("http", "http://localhost:19090/-/healthy", None, "Prometheus"),
    ("http", "http://localhost:3000/api/health", None, "Grafana"),
]

def main():
    print("🚀 Starting Pipeline Verification...")
    failed = 0
    for check in checks:
        if check[0] == "tcp":
            if not check_tcp(check[1], check[2], check[3]):
                failed += 1
        elif check[0] == "http":
            if not check_http(check[1], check[3]):
                failed += 1
    
    print("-" * 40)
    if failed == 0:
        print("🎉 All systems go!")
        sys.exit(0)
    else:
        print(f"⚠️ {failed} checks failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
