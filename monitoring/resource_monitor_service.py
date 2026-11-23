import time
from datetime import datetime, timezone

from system_monitor import collect_all_metrics

INTERVAL_SECONDS = 30


def run_monitoring_service():

    print("CarbonHive Resource Monitoring Service Started")
    print(f"Collection interval: {INTERVAL_SECONDS} seconds")

    try:
        while True:

            metrics = collect_all_metrics()

            timestamp = datetime.now(timezone.utc).isoformat()

            print(
                f"[{timestamp}] "
                f"CPU: {metrics['host']['cpu_percent']}% | "
                f"RAM: {metrics['host']['ram_percent']}%"
            )

            for container in metrics["docker_containers"]:
                print(
                    f"{container['container_name']} | "
                    f"CPU: {container['cpu_percent']}% | "
                    f"RAM: {container['memory_percent']}%"
                )

            # Save monitoring history.
            from pathlib import Path
            import json

            results_dir = (
                Path(__file__).resolve().parent.parent / "results"
            )
            results_dir.mkdir(parents=True, exist_ok=True)

            history_file = results_dir / "system_metrics.jsonl"

            with open(history_file, "a", encoding="utf-8") as file:
                file.write(json.dumps(metrics) + "\n")

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nResource monitoring service stopped.")


if __name__ == "__main__":
    run_monitoring_service()