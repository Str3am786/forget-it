import random
import time
from typing import Iterable
from openai import OpenAI, RateLimitError
from .base import LLM

class OpenAiLLM(LLM):
    def __init__(
        self,
        base_url: str = "http://localhost:8000/v1",
        api_key: str = "sk-noauth",
        model: str = "local",          # or whatever alias you used
    ):
        self.client = OpenAI(
            base_url=base_url,
            api_key=api_key,
        )
        self.model = model

    def generate(
        self,
        prompt: str,
        *,
        max_tokens: int = 512,
        temperature: float = 0.0,
        max_retries: int = 6,
        base_delay:int = 0.5
    ) -> str:
        max_retries = 6
        base_delay = 0.5
        last_err = None
        for attempt in range(max_retries):
            try:
                ...
                r = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                return r.choices[0].message.content
            
            except RateLimitError as e:
                last_err = e
                # exponential backoff with jitter
                sleep_s = base_delay * (2 ** attempt) * (1.0 + random.random() * 0.2)
                time.sleep(sleep_s)

        raise last_err


        

    def stream(
        self,
        prompt: str,
        *,
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> Iterable[str]:
        # streaming style for openai>=1.0
        stream = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
        )
        for chunk in stream:
            delta = chunk.choices[0].delta
            if delta.content:
                yield delta.content