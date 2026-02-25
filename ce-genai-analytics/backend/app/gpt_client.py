from openai import AsyncOpenAI
import os

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))


async def stream_chat_completion(prompt: str, system_prompt: str | None = None):
    """
    TRUE streaming from OpenAI.
    Yields tokens as they are generated.
    """

    effective_system_prompt = system_prompt or "You are an executive analytics assistant. Keep responses very brief."

    stream = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": effective_system_prompt},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
        max_tokens=600,
        stream=True
    )

    async for event in stream:
        if not event.choices:
            continue

        delta = event.choices[0].delta
        if delta and delta.content:
            yield delta.content
