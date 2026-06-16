"""Single provider-agnostic LLM client (Anthropic by default, OpenAI optional).

All LLM calls in Shift.ai go through this one class so the rest of the AI layer
never imports a vendor SDK directly. Picks the provider from LLM_PROVIDER.
"""
from __future__ import annotations

import json
import os
import re


class LLMClient:
    def __init__(self, provider: str | None = None, model: str | None = None):
        self.provider = (provider or os.environ.get("LLM_PROVIDER", "anthropic")).lower()
        if self.provider == "anthropic":
            import anthropic

            self._client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY
            self.model = model or os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-8")
        elif self.provider == "openai":
            from openai import OpenAI

            self._client = OpenAI()  # reads OPENAI_API_KEY
            self.model = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        else:
            raise ValueError(f"Unknown LLM_PROVIDER: {self.provider}")

    def complete(self, system: str, user: str, *, max_tokens: int = 1500) -> str:
        if self.provider == "anthropic":
            msg = self._client.messages.create(
                model=self.model, max_tokens=max_tokens,
                system=system, messages=[{"role": "user", "content": user}],
            )
            return "".join(b.text for b in msg.content if b.type == "text")
        resp = self._client.chat.completions.create(
            model=self.model, max_tokens=max_tokens,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        )
        return resp.choices[0].message.content or ""

    def complete_json(self, system: str, user: str, *, max_tokens: int = 1500) -> dict:
        """Ask for JSON and parse it leniently (provider-agnostic — no vendor
        structured-output features, so we instruct + extract)."""
        text = self.complete(
            system,
            user + "\n\nRespond with ONLY a single valid JSON object — no prose, no markdown fences.",
            max_tokens=max_tokens,
        )
        return extract_json(text)


def extract_json(text: str) -> dict:
    """Pull the first JSON object out of an LLM response, fences or not."""
    text = text.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        text = fenced.group(1)
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"No JSON object found in LLM response: {text[:200]!r}")
    return json.loads(text[start : end + 1])
