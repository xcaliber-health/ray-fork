from ray import serve
import ray
import traceback
from agentx_adk.agent._runtime import Agent, serve as serve_abs, get_agent, call_agent



@Agent(name="Agent_1", namespace="agents")
class Agent_1:

 
    async def run(self, message: str) -> str:
        results = []


        payload1 = {"message": f"{message}+agent1-check1-fn-call-agent"}
        try:
            message1 = await call_agent("Agent_2", payload1)
            results.append(
                f"""
    [call_agent ✅]
    input  : {payload1}
    output : {message1}
    """
            )
        except Exception as e:
            results.append(
                f"""
    [call_agent ❌ ERROR]
    input   : {payload1}
    message : {e}

    traceback:
    {traceback.format_exc()}
    """
            )


        payload2 = {"message": "agent1-check2-fn-get-agent"}
        try:
            agent2 = get_agent("Agent_2")
            message2 = await agent2.run(payload2)
            results.append(
                f"""
    [get_agent ✅]
    input  : {payload2}
    output : {message2}
    """
            )
        except Exception as e:
            results.append(
                f"""
    [get_agent ❌ ERROR]
    input   : {payload2}
    message : {e}

    traceback:
    {traceback.format_exc()}
    """
            )


        payload3 = {"message": "agent1-check3-fn-handle_get_deployment"}
        try:
            handle = serve.get_deployment_handle("Agent_2_API", app_name="agents-app-2")
            resp = await handle.remote(payload3)
            message3 = resp.get("result", "MISSING_RESULT")
            results.append(
                f"""
    [serve.get_deployment_handle ✅]
    input  : {payload3}
    output : {message3}
    """
            )
        except Exception as e:
            results.append(
                f"""
    [serve.get_deployment_handle ❌ ERROR]
    input   : {payload3}
    message : {e}

    traceback:
    {traceback.format_exc()}
    """
            )


        payload4 = {"message": "agent1-check4-fn-ray-get-actor"}
        try:
            actor = ray.get_actor("Agent_2", namespace="agents")
            message4 = await actor.run.remote(payload4)
            results.append(
                f"""
    [ray.get_actor ✅]
    input  : {payload4}
    output : {message4}
    """
            )
        except Exception as e:
            results.append(
                f"""
    [ray.get_actor ❌ ERROR]
    input   : {payload4}
    message : {e}

    traceback:
    {traceback.format_exc()}
    """
            )

        return "\n".join(results)


def create_Agent_1():
    Agent_1()

@serve_abs(name="Agent_1_API")
class Agent_1_API:
    def __init__(self):
        create_Agent_1()
        self.agent = get_agent("Agent_1", namespace="agents")

    async def __call__(self, request):
        if isinstance(request, dict):
            payload = request
        else:
            payload = await request.json()
        message = payload.get("message")

        if not message:
            return {"error": "message is required"}

        result = await self.agent.run(message=message)
        return {"result": result}



def create_Agent_1_api(config: dict):
    return Agent_1_API.options().bind()


# if __name__ == "__main__":
#     ray.init(address="127.0.0.1:6379", namespace="agents")
#     serve.start(detached=True)
#     serve.run(Agent_1_API.bind())