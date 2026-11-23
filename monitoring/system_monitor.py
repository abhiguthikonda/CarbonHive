import psutil
import platform
import subprocess
import json
from pathlib import Path
from datetime import datetime, timezone


def get_system_metrics():
    """Collect current system CPU and memory utilization."""

    cpu_percent = psutil.cpu_percent(interval=1)

    memory = psutil.virtual_memory()

    metrics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hostname": platform.node(),
        "platform": platform.system(),
        "cpu_percent": cpu_percent,
        "ram_total_gb": round(memory.total / (1024 ** 3), 2),
        "ram_used_gb": round(memory.used / (1024 ** 3), 2),
        "ram_available_gb": round(memory.available / (1024 ** 3), 2),
        "ram_percent": memory.percent
    }

    return metrics

def get_docker_metrics():
    """Collect resource utilization of running Docker containers."""

    try:
        result = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}"
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=20
        )

        containers = []

        for line in result.stdout.strip().splitlines():

            if not line.strip():
                continue

            data = json.loads(line)

            containers.append({
                "container_name": data.get("Name"),
                "status": "running",
                "cpu_percent": float(
                    data.get("CPUPerc", "0%").replace("%", "")
                ),
                "memory_usage": data.get("MemUsage"),
                "memory_percent": float(
                    data.get("MemPerc", "0%").replace("%", "")
                ),
                "network_io": data.get("NetIO"),
                "block_io": data.get("BlockIO"),
                "process_count": int(data.get("PIDs", 0))
            })

        return containers

    except subprocess.TimeoutExpired:
        print("Docker monitoring timed out.")
        return []

    except subprocess.CalledProcessError as error:
        print(f"Docker monitoring failed: {error.stderr}")
        return []

    except (ValueError, json.JSONDecodeError) as error:
        print(f"Unable to parse Docker metrics: {error}")
        return []

def collect_all_metrics():
    """Collect host and Docker metrics in one monitoring record."""

    host_metrics = get_system_metrics()
    docker_metrics = get_docker_metrics()

    return {
        "timestamp": host_metrics["timestamp"],
        "host": host_metrics,
        "docker_containers": docker_metrics
    }

if __name__ == "__main__":

    metrics = collect_all_metrics()

    # Create results directory if it does not exist.
    results_dir = Path(__file__).resolve().parent.parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Append monitoring snapshot to JSONL history.
    history_file = results_dir / "system_metrics.jsonl"

    with open(history_file, "a", encoding="utf-8") as file:
        file.write(json.dumps(metrics) + "\n")

    print("\n========== CARBONHIVE SYSTEM MONITOR ==========")

    print(f"Timestamp: {metrics['timestamp']}")

    print("\nHOST METRICS:")
    for key, value in metrics["host"].items():
        print(f"{key}: {value}")

    print("\nDOCKER CONTAINER METRICS:")
    for container in metrics["docker_containers"]:
        print(f"\nContainer: {container['container_name']}")

        for key, value in container.items():
            if key != "container_name":
                print(f"{key}: {value}")

    print(f"\nHistory saved to: {history_file}")
    print("================================================")