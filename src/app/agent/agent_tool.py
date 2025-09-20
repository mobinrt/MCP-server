from typing import TypedDict, Optional
from langgraph.graph import StateGraph, END

from src.app.agent.local_client import QwenOllamaLLM


class ChatState(TypedDict):
    user_input: str
    response: str


class GraphAgent:
    """
    - initialize() prepares LLM + compiled graph (lazy)
    - run(user_input) executes the compiled graph and returns the response string
    """

    def __init__(self, llm: Optional[QwenOllamaLLM] = None):
        self._llm = llm
        self._compiled = None

    async def initialize(self) -> None:
        if self._compiled is not None:
            return

        if self._llm is None:
            self._llm = QwenOllamaLLM()

        async def llm_node(state: ChatState) -> ChatState:
            try:
                resp = await self._llm.chat(state["user_input"])
                return {"response": resp}
            except Exception as e:
                return {"response": f"[Agent Error] {str(e)}"}

        graph = StateGraph(ChatState)
        graph.add_node("qwen_chat", llm_node)
        graph.set_entry_point("qwen_chat")
        graph.add_edge("qwen_chat", END)

        self._compiled = graph.compile()

    async def run(self, user_input: str) -> str:
        if self._compiled is None:
            await self.initialize()
        state = {"user_input": user_input}
        result = await self._compiled.ainvoke(state)

        if isinstance(result, dict):
            return result.get("response", "")

        return str(result)
