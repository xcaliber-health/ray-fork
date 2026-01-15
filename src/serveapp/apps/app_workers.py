import asyncio
from ray import serve
from ray.serve.config import AutoscalingConfig

@serve.deployment(
    autoscaling_config=AutoscalingConfig(min_replicas=1, max_replicas=10, target_ongoing_requests=5),
    max_ongoing_requests=10,
)
class Worker:
    async def __call__(self, payload: dict) -> dict:
        await asyncio.sleep(payload.get("sleep_s", 0.2))
        return {"worker": "ok", "echo": payload}

app = Worker.bind()
