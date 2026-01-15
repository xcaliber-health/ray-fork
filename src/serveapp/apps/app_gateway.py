from ray import serve
from ray.serve.handle import DeploymentHandle
import starlette.requests

@serve.deployment
class Validator:
    def __call__(self, payload: dict) -> dict:
        payload.setdefault("sleep_s", 0.2)
        return payload

@serve.deployment
class Gateway:
    def __init__(self, validator: DeploymentHandle):
        self.validator = validator
        self.worker = serve.get_deployment_handle("Worker", app_name="workers")

    async def __call__(self, request: starlette.requests.Request):
        payload = await request.json()
        normalized = await self.validator.remote(payload)

        result = await self.worker.remote(normalized)

        return {"gateway": "ok", "worker_result": result}

app = Gateway.bind(Validator.bind())
