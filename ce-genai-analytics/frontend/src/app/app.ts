import {
  Component,
  ElementRef,
  ViewChild,
  NgZone,
  ChangeDetectorRef
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.html',
  styleUrls: ['./app.css']
})
export class App {

  message = '';
  hasStarted = false;
  isStreaming = false;
  streamingText = '';

  conversationId = this.generateConversationId();

  messages: any[] = [];

  @ViewChild('chatWindow') chatWindow!: ElementRef;

  constructor(
    private zone: NgZone,
    private cd: ChangeDetectorRef
  ) {}

  // =========================
  // SEND MESSAGE
  // =========================
  send() {
    if (!this.message.trim() || this.isStreaming) return;

    if (!this.hasStarted) this.hasStarted = true;

    const userMsg = this.message.trim();

    this.messages.push({
      role: 'user',
      text: userMsg
    });

    this.message = '';
    this.scrollToBottom();

    this.isStreaming = true;
    this.streamingText = '';
    this.cd.detectChanges();

    const params = new URLSearchParams({
      message: userMsg,
      conversation_id: this.conversationId
    });

    const url = `http://localhost:8000/chat/stream?${params.toString()}`;
    const es = new EventSource(url);

    // =========================
    // RENDER EVENT
    // =========================
    es.addEventListener('render', (e: any) => {
      setTimeout(() => {
        this.zone.run(() => {
          try {
            const renderSpec = JSON.parse(e.data);

            this.messages.push({
              role: 'bot',
              render: renderSpec
            });

            this.isStreaming = false;

            this.cd.detectChanges();
            this.scrollToBottom();

          } catch (err) {
            console.error('Failed to parse render spec:', err, e.data);
          }
        });
      }, 0);
    });

    // =========================
    // STREAMING TOKENS
    // =========================
    es.onmessage = (e) => {
      if (!e.data) return;

      setTimeout(() => {
        this.zone.run(() => {
          this.streamingText += e.data;
          this.cd.detectChanges();
          this.scrollToBottom();
        });
      }, 0);
    };

    // =========================
    // DONE
    // =========================
    es.addEventListener('done', () => {
      setTimeout(() => {
        this.zone.run(() => {
          this.finishStreaming(es);
        });
      }, 0);
    });

    // =========================
    // ERROR
    // =========================
    es.onerror = (err) => {
      console.error('SSE error:', err);
      setTimeout(() => {
        this.zone.run(() => {
          this.finishStreaming(es);
        });
      }, 0);
    };
  }

  // =========================
  // FINISH STREAMING
  // =========================
  private finishStreaming(es: EventSource) {
    es.close();

    if (this.streamingText.trim()) {
      this.messages.push({
        role: 'bot',
        text: this.streamingText
      });
    }

    this.streamingText = '';
    this.isStreaming = false;

    this.cd.detectChanges();
    this.scrollToBottom();
  }

  // =========================
  // AUTO SCROLL
  // =========================
  private scrollToBottom() {
    setTimeout(() => {
      if (this.chatWindow) {
        const el = this.chatWindow.nativeElement;
        el.scrollTop = el.scrollHeight;
      }
    }, 20);
  }

  // =========================
  // KPI FORMATTER
  // =========================
  formatKpiValue(k: any): string {
    if (k.format === 'currency') {
      return '$' + Number(k.value).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      });
    }

    if (k.format === 'percent') {
      return Number(k.value).toFixed(2) + '%';
    }

    if (typeof k.value === 'number') {
      return k.value.toLocaleString();
    }

    return k.value;
  }

  // =========================
  // CONVERSATION ID
  // =========================
  private generateConversationId(): string {
    return 'conv-' + Math.random().toString(36).substring(2, 12);
  }

  maxChartValue(arr: number[]): number {
  if (!arr || !arr.length) return 1;
  return Math.max(...arr.map(v => Number(v) || 0), 1);
}

}
