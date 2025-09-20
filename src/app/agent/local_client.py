import os
from typing import Optional
from langchain_community.chat_models import ChatOllama
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
from src.config.settings import settings

class QwenOllamaLLM:
    def __init__(self, model: str = settings.llm_model, temperature: float = 0.3):
        self.client = ChatOllama(
            model=model,
            temperature=temperature,
            base_url=settings.llm_url,
        )

    async def chat(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """Send a prompt to Qwen (Ollama) and return text response."""
        messages: list[BaseMessage] = []
        if system_prompt:
            messages.append(HumanMessage(content=system_prompt))
        messages.append(HumanMessage(content=prompt))

        try:
            response = await self.client.ainvoke(messages)
        except Exception as e:
            return f"[Error contacting Qwen via Ollama: {e}]"

        if isinstance(response, AIMessage):
            return response.content
        return getattr(response, "content", str(response))
