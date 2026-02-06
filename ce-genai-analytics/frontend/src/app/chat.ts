import {
  Component,
  ElementRef,
  ViewChild,
  NgZone,
  ChangeDetectorRef,
  OnInit
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AuthService } from './services/auth.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-chat',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chat.html',
  styleUrls: ['./chat.css']
})
export class ChatComponent implements OnInit {
  // 🔁 MOVE EVERYTHING from your App class here
  // (username, messages, send(), logout(), etc)
  // AUTH

  password = '';
  authHeader = '';
  loginError = false;

  message = '';
  hasStarted = false;
  isStreaming = false;
  streamingText = '';

  conversationId = this.generateConversationId();

  showProfileMenu = false;

  messages: any[] = [];
  currentRender: any = null;

  @ViewChild('chatWindow') chatWindow!: ElementRef;

  isLoggedIn = false;
  username = '';
  authToken = '';
  toggleProfileMenu() {
    this.showProfileMenu = !this.showProfileMenu;
  }

  constructor(
    private zone: NgZone,
    private cd: ChangeDetectorRef,
    private auth: AuthService,
    private router: Router
  ) { }

  ngOnInit() {
    this.username = this.auth.getUsername();
    this.authToken = this.auth.getToken();

    if (!this.auth.isLoggedIn()) {
      this.router.navigate(['/login']);
    }
  }

  logout() {
  this.showProfileMenu = false;
  this.auth.clear();          // ✅ CLEAR STORAGE
  this.router.navigate(['/login']);
}

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
      conversation_id: this.conversationId,
      auth: this.authToken
    });
    

    const url = `http://localhost:8000/chat/stream?${params.toString()}`;
    const es = new EventSource(url);

    let hasReceivedValidEvent = false;

    // =========================
    // VALIDATION ERROR EVENT - Handle forbidden operations
    // =========================
    es.addEventListener('validation_error', (e: any) => {
      hasReceivedValidEvent = true;
      setTimeout(() => {
        this.zone.run(() => {
          try {
            const errorData = JSON.parse(e.data);

            this.messages.push({
              role: 'bot',
              error: true,
              errorMessage: errorData.message,
              errorType: errorData.type
            });

            this.isStreaming = false;
            es.close();

            this.cd.detectChanges();
            this.scrollToBottom();

          } catch (err) {
            console.error('Failed to parse validation error:', err, e.data);
            this.messages.push({
              role: 'bot',
              error: true,
              errorMessage: 'Operation blocked: Invalid request.',
              errorType: 'validation_error'
            });
            this.isStreaming = false;
            es.close();
            this.cd.detectChanges();
          }
        });
      }, 0);
    });

    // =========================
    // EXECUTION ERROR EVENT
    // =========================
    es.addEventListener('execution_error', (e: any) => {
      hasReceivedValidEvent = true;
      setTimeout(() => {
        this.zone.run(() => {
          try {
            const errorData = JSON.parse(e.data);

            this.messages.push({
              role: 'bot',
              error: true,
              errorMessage: errorData.message,
              errorType: errorData.type
            });

            this.isStreaming = false;
            es.close();

            this.cd.detectChanges();
            this.scrollToBottom();

          } catch (err) {
            console.error('Failed to parse execution error:', err, e.data);
            this.messages.push({
              role: 'bot',
              error: true,
              errorMessage: 'An error occurred while executing your query.',
              errorType: 'execution_error'
            });
            this.isStreaming = false;
            es.close();
            this.cd.detectChanges();
          }
        });
      }, 0);
    });

    // =========================
    // RENDER EVENT
    // =========================
    es.addEventListener('render', (e: any) => {
      hasReceivedValidEvent = true;
      this.zone.run(() => {
        try {
          const renderSpec = JSON.parse(e.data);
          this.currentRender = renderSpec; // 🔥 IMPORTANT
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
    });

    // =========================
    // STREAMING TOKENS
    // =========================
    es.onmessage = (e) => {
      hasReceivedValidEvent = true;
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
      hasReceivedValidEvent = true;
      setTimeout(() => {
        this.zone.run(() => {
          this.finishStreaming(es);
        });
      }, 0);
    });

    // =========================
    // CONNECTION ERROR
    // =========================
    es.onerror = (err) => {
      // Only show error if no valid events were received
      if (!hasReceivedValidEvent) {
        console.error('SSE connection error:', err);
        setTimeout(() => {
          this.zone.run(() => {
            this.messages.push({
              role: 'bot',
              error: true,
              errorMessage: 'Connection lost. Please try again.',
              errorType: 'connection_error'
            });
            this.isStreaming = false;
            this.cd.detectChanges();
          });
        }, 0);
      }
      es.close();
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

  getLineChartOptions() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        tooltip: {
          callbacks: {
            label: (ctx: any) => {
              const index = ctx.dataIndex;
              const formatted =
                this.currentRender?.chart?.y_formatted?.[index];
              return formatted ?? ctx.raw;
            }
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: (_value: any, index: number) => {
              const formatted =
                this.currentRender?.chart?.y_formatted?.[index];
              return formatted ?? _value;
            }
          }
        }
      }
    };
  }
}
