import asyncio
import json

# from src.config.logger import logging

from fastmcp import Client as MCPClient
from langgraph.graph import StateGraph, END
from langchain_ollama import OllamaLLM

from typing import Optional, List, Dict

# logger = logging.getLogger(__name__)


class AgentState(dict):
    query: str
    action: dict
    result: str


ADMIN_NAME_PATTERNS = (".ingest_folder", ".ingest", ".admin")  # things not for LLM
# Optional explicit allow list per tool prefix
ALLOWED_PREFIXES = ("csv_rag", "weather", "health")


def is_public_tool(tool_name: str) -> bool:
    ln = tool_name.lower()
    # filter by explicit patterns first
    if any(pat in ln for pat in ADMIN_NAME_PATTERNS):
        return False
    # optionally require allowed prefix
    if not any(ln.startswith(pref) for pref in ALLOWED_PREFIXES):
        # you can be stricter here or allow more prefixes
        return False
    return True


def simple_score(query: str, text: str) -> int:
    """Simple deterministic scoring for remapping. Count keyword overlap."""
    q_tokens = set([t.lower() for t in query.split() if len(t) > 2])
    t_tokens = set([t.lower() for t in text.split() if len(t) > 2])
    return len(q_tokens & t_tokens)


async def fetch_public_tools() -> List[Dict]:
    async with MCPClient("http://127.0.0.1:8001/mcp") as client:
        tools = await client.list_tools()
    # Convert to simple dicts: {'name': ..., 'description': ...}
    tool_list = []
    for t in tools:
        name = t.name if hasattr(t, "name") else getattr(t, "tool", None)
        desc = getattr(t, "description", "") or ""
        tool_list.append({"name": name, "description": desc})
    # filter public tools
    return [t for t in tool_list if t["name"] and is_public_tool(t["name"])]


def build_prompt(query: str, tools: List[Dict]) -> str:
    tool_lines = "\n".join([f"- {t['name']}: {t['description']}" for t in tools])
    return f"""
You are an intelligent agent deciding which tool to call for a user's query.
Only choose from the exact tool names listed below (case-sensitive).
DO NOT choose any maintenance or ingestion tools.

Available tools:
{tool_lines}

User query:
\"{query}\"

Return EXACTLY one JSON object, nothing else, with format:
{{ "tool": "<tool_name>", "args": {{ ... }} }}

Example:
{{ "tool": "weather", "args": {{ "city": "Tehran" }} }}

If unsure, choose the best matching tool by name and description.
"""


# attempt robust JSON extraction - kept from your code
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
    # 1) fetch public tools
    tools = await fetch_public_tools()

    # 2) build prompt and call LLM
    prompt = build_prompt(state["query"], tools)
    response = await llm.ainvoke(prompt)
    print("res: ", response)

    # 3) parse JSON; robust fallback
    action = extract_json_object_from_text(response)
    if not action:
        print("⚠️ LLM returned invalid JSON. Response:", response)
        action = {"tool": "health.ping", "args": {}}

    # 4) validate selected tool exists and is public
    selected = action.get("tool")
    tool_names = [t["name"] for t in tools]
    if selected not in tool_names:
        print(
            f"⚠️ LLM chose invalid or disallowed tool '{selected}'. Attempting remap..."
        )
        # deterministic rerank: score query against name+description
        best = None
        best_score = -1
        for t in tools:
            text = f"{t['name']} {t['description']}"
            s = simple_score(state["query"], text)
            if s > best_score:
                best_score = s
                best = t
        if best and best_score >= 1:
            print(f"↪ Remapped to best candidate: {best['name']} (score {best_score})")
            action = {"tool": best["name"], "args": {"query": state["query"]}}
        else:
            print("↪ No good public candidate found. Fallback to `health.ping`.")
            action = {"tool": "health.ping", "args": {}}

    state["action"] = action
    return state


async def tool_node(state: AgentState):
    if "action" not in state:
        state["result"] = None
        return state

    action = state["action"]
    tool = action.get("tool")
    args = action.get("args", {})
    if not isinstance(args, dict):
        args = {}

    # Validate again before calling MCP
    public_tools = await fetch_public_tools()
    public_names = {t["name"] for t in public_tools}
    if tool not in public_names:
        # never call disallowed tools
        state["result"] = {"error": f"Tool '{tool}' is not allowed or not registered."}
        return state

    # call the tool
    async with MCPClient("http://127.0.0.1:8001/mcp") as client:
        pass_args = {"args": args}
        result = await client.call_tool(tool, pass_args)
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

    for q in ["give me a list of restaurant in Isfahan"]:
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
#     async with MCPClient("http://127.0.0.1:8001/mcp") as client:
#         tools = await client.list_tools()
#         print("Registered tools:", tools)

# asyncio.run(list_tools())
