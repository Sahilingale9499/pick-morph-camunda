import docker
import time
import subprocess
import os

THRESHOLD_BYTES  = 5 * 1024 ** 3   # 5 GB
SERVICE_NAME     = "spring-temporal"
MAX_INSTANCES    = 10
COOLDOWN_SEC     = 60               # min seconds between scale-up events
CHECK_INTERVAL   = 15               # seconds between memory checks
COMPOSE_FILE     = "/app/docker-compose.yml"
PROJECT_NAME     = os.getenv("COMPOSE_PROJECT_NAME", "temporalprojectworkflowmanagement")


def get_total_memory(client):
    total = 0
    for container in client.containers.list():
        if SERVICE_NAME in container.name:
            stats = container.stats(stream=False)
            total += stats["memory_stats"].get("usage", 0)
    return total


def get_current_scale(client):
    return sum(1 for c in client.containers.list() if SERVICE_NAME in c.name)


def scale_up(current):
    new = current + 1
    print(f"[autoscaler] Scaling {SERVICE_NAME}: {current} → {new} instances", flush=True)
    subprocess.run(
        [
            "docker", "compose",
            "-f", COMPOSE_FILE,
            "-p", PROJECT_NAME,
            "up",
            "--scale", f"{SERVICE_NAME}={new}",
            "--no-recreate",
            "-d",
        ],
        check=True,
    )


if __name__ == "__main__":
    print(
        f"[autoscaler] Started. "
        f"Threshold={THRESHOLD_BYTES / 1024**3:.0f} GB  "
        f"Max={MAX_INSTANCES}  "
        f"Cooldown={COOLDOWN_SEC}s  "
        f"Check={CHECK_INTERVAL}s",
        flush=True,
    )
    last_scale_time = 0.0
    client = docker.from_env()

    while True:
        try:
            total_mem = get_total_memory(client)
            total_gb  = total_mem / 1024 ** 3
            current   = get_current_scale(client)
            print(f"[autoscaler] Memory={total_gb:.2f} GB  Instances={current}", flush=True)

            if total_mem > THRESHOLD_BYTES:
                now = time.time()
                if now - last_scale_time >= COOLDOWN_SEC:
                    if current < MAX_INSTANCES:
                        scale_up(current)
                        last_scale_time = now
                    else:
                        print(f"[autoscaler] At max ({MAX_INSTANCES}), cannot scale further", flush=True)
                else:
                    remaining = int(COOLDOWN_SEC - (now - last_scale_time))
                    print(f"[autoscaler] Cooldown active — {remaining}s remaining", flush=True)

        except Exception as e:
            print(f"[autoscaler] Error: {e}", flush=True)

        time.sleep(CHECK_INTERVAL)
