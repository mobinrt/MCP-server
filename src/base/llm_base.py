# src/app/agent/llm_base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class LLMClient(ABC):
    """
    Small interface for the agent to interact with an LLM.
    Implementations must be async.
    """

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        *,
        max_tokens: int = 512,
        temperature: float = 0.0,
        stop: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Generate model output for the prompt.
        Return shape is a dict with at least {"text": "<generated string>"}.
        """
        raise NotImplementedError()
