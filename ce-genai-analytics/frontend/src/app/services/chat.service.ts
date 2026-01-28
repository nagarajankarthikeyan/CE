import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class ChatService {

  async streamChat(message: string, onToken: (chunk: string) => void) {
    const response = await fetch('http://localhost:8000/chat/stream', {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message })
    });

    if (!response.body) return;

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value, { stream: true });
      onToken(chunk);
    }
  }

  streamChatSSE(message: string, onToken: (chunk: string) => void) {
  const url = `http://localhost:8000/chat/stream?message=${encodeURIComponent(message)}`;
  const es = new EventSource(url);

  es.onmessage = (event) => {
    onToken(event.data);
  };

  es.addEventListener('done', () => {
    es.close();
  });

  es.onerror = (err) => {
    console.error('SSE error', err);
    es.close();
  };

  return es;
}

}
