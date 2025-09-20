import asyncio
import json
from typing import Optional

# from src.config.logger import logging

from fastmcp import Client as MCPClient
from langgraph.graph import StateGraph, END
from langchain_ollama import OllamaLLM

# logger = logging.getLogger(__name__)

class AgentState(dict):
    query: str
    action: dict
    result: str

ALLOWED_TOOLS = {
    "csv_rag",
    "weather",
    "health.ping",
}


# Utility: extract JSON object from potentially noisy text using balanced-brace scan
def extract_json_object_from_text(text: str) -> Optional[dict]:
    if not text:
        return None
    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass


    start = text.find("{")
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    continue
    return None



llm = OllamaLLM(model="qwen2.5:3b", base_url="http://localhost:11434")



async def llm_node(state: AgentState):
    """Decides which tool to call based on query and returns updated state."""
    prompt = f"""
    You are an agent. Decide which tool to call based on the query.

    Available tools:
    - csv_rag(query: str, top_k: int)
    - weather(city: str)
    - health.ping()

    User query: {state['query']}

    Return JSON with exact arguments required by the tool.
    Examples:
    - For 'What's the weather in Tehran?', output:
    {{ "tool": "weather", "args": {{ "city": "Tehran" }} }}
    - For 'Are you alive?', output:
    {{ "tool": "health.ping", "args": {{}} }}

    Always ensure all required fields are present.
    """
    response = await llm.ainvoke(prompt)
    print("res: ",response)
    print(type(response))
    try:
        action = json.loads(response.strip())
    except json.JSONDecodeError:
        action = {"tool": "health.ping", "args": {}}

    state["action"] = action
    return state   



async def tool_node(state: AgentState):
    if "action" not in state:
        state["result"] = None
        return state

    async with MCPClient("http://127.0.0.1:8001/mcp") as client:
        action = state["action"]
        tool = action["tool"]
        args = action.get("args", {})


        if not isinstance(args, dict):
            args = {}
        mcp_args = {"args": args}

        result = await client.call_tool(tool, mcp_args)
        state["result"] = result.data

    return state

# 4. Build LangGraph workflow
def build_graph():
    workflow = StateGraph(AgentState)

    workflow.add_node("llm", llm_node)
    workflow.add_node("tool", tool_node)

    workflow.add_edge("llm", "tool")
    workflow.add_edge("tool", END)

    workflow.set_entry_point("llm")
    return workflow


# 5. Run it
async def main():
    graph = build_graph().compile()

    for q in [
        "What's the weather in Tehran?",
    ]:
        print(f"\n=== Running query: {q} ===")
        result = await graph.ainvoke({"query": q})
        # print entire state so you can see action/result for debugging
        print("Final state:", result)
        print("Final result:", result.get("result"))


if __name__ == "__main__":
    asyncio.run(main())



# from fastmcp import Client as MCPClient
# import asyncio

# async def list_tools():
#     async with MCPClient("http://127.0.0.1:8000/mcp") as client:
#         tools = await client.list_tools()
#         print("Registered tools:", tools)        

# asyncio.run(list_tools())
