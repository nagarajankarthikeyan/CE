from openai import AsyncOpenAI
import os

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))


async def stream_chat_completion(prompt: str):
    """
    TRUE streaming from OpenAI.
    Yields tokens as they are generated.
    """

    stream = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an executive analytics assistant. Keep responses very brief."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
        max_tokens=90,
        stream=True
    )

    async for event in stream:
        if not event.choices:
            continue

        delta = event.choices[0].delta
        if delta and delta.content:
            yield delta.content
