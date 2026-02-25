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
import { marked } from 'marked';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration, ChartOptions, ChartType } from 'chart.js';
import { AuthService } from './services/auth.service';
import { Router } from '@angular/router';

export interface ChartSpec {
  type: 'bar' | 'line' | 'pie';
  title?: string;
  data: Record<string, any>[];
  xKey: string;
  yKeys: { key: string; label?: string; color?: string }[];
}

interface UiChartModel {
  type: ChartType;
  title?: string;
  data: ChartConfiguration['data'];
  options: ChartOptions;
}

const CHART_COLORS = [
  '#7EC8FF',
  '#64B5F6',
  '#90CAF9',
  '#4FC3F7',
  '#81D4FA'
];

marked.setOptions({
  gfm: true,
  breaks: true
});

@Component({
  selector: 'app-chat',
  standalone: true,
  imports: [CommonModule, FormsModule, BaseChartDirective],
  templateUrl: './chat.html',
  styleUrls: ['./chat.css']
})
export class ChatComponent implements OnInit {
  password = '';
  authHeader = '';
  loginError = false;

  message = '';
  hasStarted = false;
  isStreaming = false;
  streamingText = '';
  streamingHtml = '';
  lastUserMessage = '';
  conversationId = this.generateConversationId();

  showProfileMenu = false;

  messages: any[] = [];
  role = '';
  currentRender: any = null;
  lastRenderSpecForNarrative: any = null;

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
  ) {}

  ngOnInit() {
    this.username = this.auth.getUsername();
    this.authToken = this.auth.getToken();
    this.role = this.auth.getRole();

    if (!this.auth.isLoggedIn()) {
      this.router.navigate(['/login']);
    }
  }

  goToAdmin() {
    this.router.navigate(['/admin']);
    this.showProfileMenu = false;
  }

  isAdmin(): boolean {
    return this.role === 'admin';
  }

  logout() {
    this.showProfileMenu = false;
    this.auth.clear();
    this.router.navigate(['/login']);
  }

  send() {
    if (!this.message.trim() || this.isStreaming) return;

    if (!this.hasStarted) this.hasStarted = true;

    const userMsg = this.message.trim();
    this.lastUserMessage = userMsg;
    this.messages.push({
      role: 'user',
      text: userMsg
    });

    this.message = '';
    this.scrollToBottom();

    this.isStreaming = true;
    this.streamingText = '';
    this.streamingHtml = '';
    this.cd.detectChanges();

    const params = new URLSearchParams({
      message: userMsg,
      conversation_id: this.conversationId,
      auth: this.authToken
    });

    const url = `/api/chat/stream?${params.toString()}`;
    const es = new EventSource(url);

    let hasReceivedValidEvent = false;

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

    es.addEventListener('render', (e: any) => {
      hasReceivedValidEvent = true;
      this.zone.run(() => {
        try {
          const renderSpec = JSON.parse(e.data);
          const title = typeof renderSpec?.title === 'string' ? renderSpec.title.trim() : '';
          const last = this.lastUserMessage.trim();
          if (title && last && title.toLowerCase() === last.toLowerCase()) {
            delete renderSpec.title;
          }
          const wantsTable = this.shouldRenderTable(last);
          if (!wantsTable) {
            if (renderSpec?.render_type === 'table') {
              renderSpec.render_type = 'narrative';
              renderSpec.table = { columns: [], rows: [] };
            }
            if (renderSpec?.render_type === 'mixed') {
              renderSpec.table = { columns: [], rows: [] };
            }
          }
          this.lastRenderSpecForNarrative = renderSpec;
          const hasRenderableStructured =
            (Array.isArray(renderSpec?.kpis) && renderSpec.kpis.length > 0) ||
            (Array.isArray(renderSpec?.ranked_list) && renderSpec.ranked_list.length > 0) ||
            (Array.isArray(renderSpec?.bullets) && renderSpec.bullets.length > 0) ||
            (Array.isArray(renderSpec?.table?.rows) && renderSpec.table.rows.length > 0 && this.shouldRenderTable(last)) ||
            (typeof renderSpec?.narrative === 'string' && renderSpec.narrative.trim().length > 0);

          if (hasRenderableStructured) {
            this.messages.push({
              role: 'bot',
              render: renderSpec,
              queryText: last
            });
          }
          this.isStreaming = false;
          this.cd.detectChanges();
          this.scrollToBottom();
        } catch (err) {
          console.error('Failed to parse render spec:', err, e.data);
        }
      });
    });

    es.onmessage = (e) => {
      hasReceivedValidEvent = true;
      if (!e.data) return;

      setTimeout(() => {
        this.zone.run(() => {
          this.streamingText += e.data;
          // Render streaming text with the same formatter used for final content
          // so layout does not jump when the stream completes.
          this.streamingHtml = this.renderNarrativeHtml(this.streamingText);
          this.cd.detectChanges();
          this.scrollToBottom();
        });
      }, 0);
    };

    es.addEventListener('done', () => {
      hasReceivedValidEvent = true;
      setTimeout(() => {
        this.zone.run(() => {
          this.finishStreaming(es);
        });
      }, 0);
    });

    es.onerror = (err) => {
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

  private finishStreaming(es: EventSource) {
    es.close();

    if (this.streamingText.trim()) {
      const parsed = this.parseChartSpecs(this.streamingText);
      const hasUsableChart = parsed.charts.some((c) => Array.isArray(c?.data) && c.data.length > 0);
      const fallbackSpec = !hasUsableChart
        ? this.buildFallbackChartSpec(this.lastRenderSpecForNarrative)
        : null;
      const chartSpecs = fallbackSpec ? [fallbackSpec] : parsed.charts;
      this.messages.push({
        role: 'bot',
        text: parsed.cleanContent,
        textHtml: this.renderNarrativeHtml(parsed.cleanContent),
        analysisCharts: chartSpecs.map((c) => this.toUiChartModel(c)),
        queryText: this.lastUserMessage
      });
    }

    this.streamingText = '';
    this.streamingHtml = '';
    this.isStreaming = false;

    this.cd.detectChanges();
    this.scrollToBottom();
  }

  private scrollToBottom() {
    setTimeout(() => {
      if (this.chatWindow) {
        const el = this.chatWindow.nativeElement;
        el.scrollTop = el.scrollHeight;
      }
    }, 20);
  }

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
      if (k.value >= 1900 && k.value <= 2100) {
        return String(k.value);
      }

      return k.value.toLocaleString();
    }

    return k.value;
  }

  getNarrativeHtml(text: string): string {
    if (!text) return '';
    return this.renderNarrativeHtml(text);
  }

  private renderNarrativeHtml(text: string): string {
    const cleanText = this.parseChartSpecs(text).cleanContent;
    const normalized = this.normalizeMarkdown(cleanText);
    const html = marked.parse(normalized) as string;
    return html
      .replace(/<h1\b/gi, '<h3')
      .replace(/<\/h1>/gi, '</h3>')
      .replace(/<h[1-6][^>]*>\s*<\/h[1-6]>/gi, '')
      .replace(/<h([1-6])([^>]*)>\s*#\s*/gi, '<h$1$2>')
      .replace(/<h4>([^<]*?)-\s*<\/h4>/gi, '<h4>$1</h4>')
      .replace(/<p>\s*s\s*<\/p>/gi, '')
      .replace(/<li>\s*<strong>\s*([^<]+?)\s*<\/strong>\s*<\/li>/gi, '<h4>$1</h4>')
      .replace(/<li>\s*<\/li>/gi, '')
      .replace(/<li>\s*<p>\s*<\/p>\s*<\/li>/gi, '')
      .replace(/<li>\s*[•\-*]\s*<\/li>/gi, '')
      .replace(/<li[^>]*>\s*<\/li>/gi, '')
      .replace(/<li[^>]*>\s*<h[1-6][^>]*>\s*<\/h[1-6]>\s*<\/li>/gi, '')
      .replace(/<ul>\s*<\/ul>/gi, '')
      .replace(/<ol>\s*<\/ol>/gi, '');
  }

  private escapeStreamingText(text: string): string {
    const escaped = (text || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    return escaped.replace(/\n/g, '<br/>');
  }

  shouldRenderTable(question: string): boolean {
    const q = (question || '').toLowerCase();
    return /\b(table|tabular|rows?|columns?|spreadsheet|csv|grid|list all|show table|display table)\b/.test(q);
  }

  private normalizeMarkdown(text: string): string {
    let out = text || '';
    out = out.replace(/\r\n/g, '\n');
    // Keep title hierarchy compact for chat bubbles.
    out = out.replace(/^#\s+/gm, '### ');
    out = out.replace(/^##\s+/gm, '### ');
    // Split glued heading/body words like "SummaryThis", "TakeawayConsider".
    out = out.replace(/([A-Za-z])([A-Z][a-z])/g, '$1 $2');
    out = out.replace(/^\s*Last\s*\n+\s*week's/im, "Last week's");
    // Split glued heading + body text when model outputs both on one heading line.
    out = out.replace(
      /^(#{1,6}\s+[A-Za-z][^\n]{2,90}?)\s+(In this|This|Overall|For this|Today|Here)\b/gim,
      '$1\n\n$2'
    );
    out = out.replace(
      /([A-Za-z]+\s+\d{1,2},\s+\d{4})(Today|This|Overall|In this|Here)\b/g,
      '$1 $2'
    );
    out = out.replace(/(Meta spend this month\s*\([^)]*\))\s*(Key finding\b)/gi, '$1\n\n### $2');
    out = out.replace(/^(#{1,6}\s+[^\n#]+?)\s+(Overall\b)/gim, '$1\n\n$2');
    out = out.replace(/(Last week's program performance\s*\([^)]*\))\s*(Overall\b)/gi, '$1\n\n$2');
    out = out.replace(/(Suggested takeaway\s*\/\s*next step)\s*([A-Z])/gi, '$1\n\n$2');
    out = out.replace(/(Summary)\s*(This|That|These|Those|Overall)\b/gi, '$1\n\n$2');
    out = out.replace(/(\b20\d{2})\s*(Overall performance\b)/gi, '$1\n\n### $2');
    out = out.replace(/^\s*[-*]\s*([A-Za-z0-9 ()-]*Overall performance[^.\n]*)\s*$/gim, '### $1');
    out = out.replace(/(Overview\s*\([^)]*\))\s*(Key finding\s*\([^)]*\)\s*:)/gi, '$1\n\n### $2');
    out = out.replace(/(Month-to-Date\s*\([^)]*\))\s*(Key finding\s*\([^)]*\)\s*:)/gi, '### $1\n\n### $2');
    out = out.replace(/([.!?]\s*)(A bit more detail\s*:)/gi, '$1\n\n### $2');
    out = out.replace(/([.!?]\s*)(Takeaway\s*:)/gi, '$1\n\n### $2');
    out = out.replace(/(Key finding\s*\([^)]*\)\s*:)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/(A bit more detail\s*:)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/(Takeaway\s*:)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/^\s*Key takeaway\s*s?\s*$/gim, '### Key takeaways');
    out = out.replace(/^\s*Key takeaway\s*\n\s*s\s*$/gim, '### Key takeaways');
    out = out.replace(/(Key Takeaways?)\s*([A-Z])/gi, '$1\n\n$2');
    out = out.replace(/(Suggested Takeaway)\s*([A-Z])/gi, '$1\n\n$2');
    out = out.replace(/\b(Takeaway)\s*(Overall)\b/gi, '$1\n\n$2');
    out = out.replace(/^(#{1,6})(\S)/gm, '$1 $2');
    out = out.replace(/^(#{1,6})\s+#\s+/gm, '$1 ');
    out = out.replace(/(Key\s*Takeaways)\s*(Last\s+week\b)/gi, '$1\n\n$2');
    out = out.replace(/(Key\s*Takeaways)\s*([A-Z][a-z])/g, '$1\n\n$2');
    out = out.replace(/([.!?])\s+(Takeaway\b)/gi, '$1\n\n### $2');
    out = out.replace(/([.!?])\s+(Last\s*Week\s*:\s*Key\s*Takeaways)/gi, '$1\n\n### $2');
    out = out.replace(/([.!?])\s+(Performance\s*Snapshot(?:\s*\([^)]*\))?)/gi, '$1\n\n### $2');
    out = out.replace(/([.!?])\s+(What\s*Stands\s*Out)/gi, '$1\n\n### $2');
    out = out.replace(/([^\n])(###\s)/g, '$1\n\n$2');
    out = out.replace(/([^\n])(##\s)/g, '$1\n\n$2');
    out = out.replace(/([^\n])(#{1}\s)/g, '$1\n\n$2');
    out = out.replace(/([^\n])(-\s+)/g, '$1\n$2');
    // Ensure numbered sections like "3. **META**" are on their own line.
    out = out.replace(/([^\n])(\d+\.\s+\*\*)/g, '$1\n$2');
    out = out.replace(/(<br\s*\/?>)\s*(\d+\.\s+\*\*)/gi, '\n\n$2');
    // Ensure platform labels render as separate blocks.
    out = out.replace(/([^\n])(\*\*[A-Za-z0-9 ()&/.-]{2,40}\*\*:)/g, '$1\n\n$2');

    // Break out section labels that are glued to the previous sentence.
    out = out.replace(/([^\n])\s+(What Stands Out\b|Key Findings\b|Detailed Insights\b)/gi, '$1\n\n$2');
    out = out.replace(/([^\n])\s+(Performance Snapshot\b)/gi, '$1\n\n$2');
    out = out.replace(/([^\n])\s+([A-Za-z][A-Za-z0-9' ()/&-]{0,60}:\s*Key Takeaways\b)/gi, '$1\n\n$2');
    out = out.replace(/([^\n])\s+(Scale\s*&\s*Spend:|Engagement\s*\/\s*Cost\s*Efficiency:|Enrollment outcomes:)/gi, '$1\n\n$2');
    out = out.replace(/([^\n])\s+((?:Click-Through Rate\s*\(CTR\)|CTR)\s*:\s*[0-9.,]+%?)\s+(What\s+Stands\s+Out\b)/gi, '$1\n$2\n\n$3');

    // Break out common metric labels if they are glued inline.
    out = out.replace(
      /([^\n])\s+((Total Spend|Total Clicks|Total Impressions|Click-Through Rate \(CTR\)|Spend|Impressions|Clicks|CTR)\s*:)/gi,
      '$1\n$2'
    );

    // Promote plain section labels to markdown headings.
    out = out.replace(/^\s*([A-Za-z][A-Za-z0-9' ()/&-]{0,60}:\s*Key Takeaways)\s*$/gim, '### $1');
    out = out.replace(/^\s*(What Stands Out|Key Takeaways|Performance Snapshot(?:\s*\([^)]*\))?|Key Findings|Detailed Insights)\s*$/gim, '### $1');
    out = out.replace(/^\s*(Scale\s*&\s*Spend|Engagement\s*\/\s*Cost\s*Efficiency|Enrollment outcomes)\s*:\s*$/gim, '#### $1');
    out = out.replace(/^\s*(Scale\s*&\s*spend|Engagement\s*\/\s*cost\s*efficiency|Enrollment\s*outcomes)\s*:\s*$/gim, '#### $1');
    out = out.replace(/^\s*[-*]\s*(Month-to-Date\s*\([^)]*\))\s*$/gim, '### $1');
    out = out.replace(/^\s*[-*]\s*(Key finding\s*\([^)]*\)\s*:)\s*$/gim, '### $1');
    out = out.replace(/^\s*[-*]\s*(A bit more detail\s*:)\s*$/gim, '### $1');
    out = out.replace(/^\s*[-*]\s*(Takeaway\s*:)\s*$/gim, '### $1');
    out = out.replace(/Top Campaigns by\s*\n+\s*[-*]\s*\*\*(Spend|Impressions|Clicks)\*\*:\s*-\s*/gim, '#### Top Campaigns by $1\n\n');
    out = out.replace(/Top Campaigns by\s*[-:]\s*\*\*(Spend|Impressions|Clicks)\*\*:\s*-\s*/gim, '#### Top Campaigns by $1\n\n');
    out = out.replace(/(Lowest Performing Campaigns)\s*[:\-]\s*(Several\b)/gi, '#### $1\n\n- $2');
    out = out.replace(/^\s*[-*]\s*\*\*([^*]+)\*\*\s*:?\s*$/gim, '#### $1');
    out = out.replace(/^\s*\d+\.\s+\*\*([^*]+)\*\*\s*:?/gim, '#### $1');
    out = out.replace(/^(####\s+.+?)\s*-\s*$/gim, '$1');

    // Break out subsection labels if inline.
    out = out.replace(/([^\n])\s+(Scale\s*&\s*Spend:)/gi, '$1\n\n$2');
    out = out.replace(/([^\n])\s+(Engagement\s*\/\s*Cost\s*Efficiency:)/gi, '$1\n\n$2');

    // Convert bare lines under sections into bullets and metric bullets.
    const lines = out.split('\n');
    let inInsightSection = false;
    let inMetricSection = false;
    let inSubSection = false;
    for (let i = 0; i < lines.length; i++) {
      const t = lines[i].trim();
      if (/^###\s+What Stands Out$/i.test(t)) {
        inInsightSection = true;
        inMetricSection = false;
        inSubSection = false;
        continue;
      }
      if (/^###\s+Detailed Insights$/i.test(t)) {
        inInsightSection = true;
        inMetricSection = false;
        inSubSection = false;
        continue;
      }
      if (/^###\s+Key Findings$/i.test(t)) {
        inInsightSection = false;
        inMetricSection = true;
        inSubSection = false;
        continue;
      }
      if (/^###\s+/.test(t) && !/^###\s+What Stands Out$/i.test(t)) {
        inInsightSection = false;
        inMetricSection = /^###\s+(.+:\s*Key Takeaways|Performance Snapshot)/i.test(t);
        inSubSection = false;
        continue;
      }
      if (/^####\s+/.test(t)) {
        inSubSection = true;
        inInsightSection = false;
        continue;
      }
      if (!t || t === '-' || t === '*' || t === '•' || t.startsWith('{')) continue;
      if (/^[-*]\s*$/.test(t) || /^[-*]\s+\*\*\s*$/.test(t)) continue;
      if (/^\d+\.\s*$/.test(t)) continue;
      if (t.startsWith('- ') || t.startsWith('* ') || t.startsWith('• ')) continue;

      if (inInsightSection) {
        lines[i] = `- ${t}`;
        continue;
      }

      if (inMetricSection || inSubSection) {
        const metricMatch = t.match(/^([A-Za-z][A-Za-z0-9\s()\/&-]{1,40}):\s*(.+)$/);
        if (metricMatch) {
          const label = metricMatch[1].trim();
          if (/(key takeaways|performance snapshot|what stands out|scale\s*&\s*spend|engagement\s*\/\s*cost\s*efficiency|enrollment outcomes)/i.test(label)) {
            continue;
          }
          lines[i] = `- **${label}:** ${metricMatch[2].trim()}`;
        }
      }
    }
    out = lines.join('\n');

    // Drop empty bullets introduced by broken formatting.
    out = out.replace(/^\s*[-*•]\s*$/gm, '');
    out = out.replace(/^\s*[-*]\s+\*\*\s*$/gm, '');
    out = out.replace(/^\s*[-*]\s*$/gm, '');
    out = out.replace(/^\s*\d+\.\s*$/gm, '');
    out = out.replace(/\n{3,}/g, '\n\n');
    return out;
  }

  formatTickValue(value: any): string {
    if (typeof value === 'number') {
      if (Math.abs(value) >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
      if (Math.abs(value) >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
      if (value % 1 !== 0) return value.toFixed(2);
      return value.toLocaleString();
    }
    if (typeof value === 'string' && value.length > 16) return value.slice(0, 14) + '...';
    return String(value);
  }

  formatTooltipValue(value: any): string {
    if (typeof value === 'number') {
      if (value % 1 !== 0) {
        return value.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        });
      }
      return value.toLocaleString();
    }
    return String(value);
  }

  parseChartSpecs(content: string): { cleanContent: string; charts: ChartSpec[] } {
    const charts: ChartSpec[] = [];
    let cleanContent = content;

    const chartRegex = /<CHART>([\s\S]*?)<\/CHART>/g;
    let match: RegExpExecArray | null;

    while ((match = chartRegex.exec(content)) !== null) {
      try {
        const raw = match[1].trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');
        const parsed = JSON.parse(raw);
        if (parsed.type && parsed.data && parsed.xKey && parsed.yKeys) {
          charts.push(parsed as ChartSpec);
        }
      } catch (e) {
        try {
          const raw = match[1].trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');
          const start = raw.indexOf('{');
          const end = raw.lastIndexOf('}');
          if (start !== -1 && end > start) {
            const parsed = JSON.parse(raw.slice(start, end + 1));
            if (parsed.type && parsed.data && parsed.xKey && parsed.yKeys) {
              charts.push(parsed as ChartSpec);
            }
          }
        } catch (inner) {
          console.error('Failed to parse chart spec:', inner);
        }
      }
    }

    // Tolerate incomplete stream where </CHART> may not have arrived in final chunk.
    if (!charts.length) {
      const openOnly = content.match(/<CHART>([\s\S]*)$/i);
      if (openOnly?.[1]) {
        try {
          const raw = openOnly[1].trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');
          const start = raw.indexOf('{');
          const end = raw.lastIndexOf('}');
          if (start !== -1 && end > start) {
            const parsed = JSON.parse(raw.slice(start, end + 1));
            if (parsed.type && parsed.data && parsed.xKey && parsed.yKeys) {
              charts.push(parsed as ChartSpec);
            }
          }
        } catch (e) {
          console.error('Failed to parse open chart spec:', e);
        }
      }
    }

    // Fallback: tolerate raw JSON chart specs not wrapped in <CHART> tags.
    if (!charts.length) {
      const looseJsonRegex = /\{[\s\S]*?"type"\s*:\s*"(bar|line|pie)"[\s\S]*?"data"\s*:\s*\[[\s\S]*?\][\s\S]*?"xKey"\s*:\s*"[^"]+"[\s\S]*?"yKeys"\s*:\s*\[[\s\S]*?\][\s\S]*?\}/g;
      const looseBlocks = content.match(looseJsonRegex) || [];
      for (const block of looseBlocks) {
        try {
          const parsed = JSON.parse(block.trim());
          if (parsed.type && parsed.data && parsed.xKey && parsed.yKeys) {
            charts.push(parsed as ChartSpec);
            cleanContent = cleanContent.replace(block, '');
          }
        } catch (e) {
          console.error('Failed to parse loose chart spec:', e);
        }
      }
    }

    cleanContent = cleanContent.replace(chartRegex, '').trim();
    cleanContent = cleanContent.replace(/<CHART>[\s\S]*$/i, '').trim();
    cleanContent = cleanContent.replace(/\{\s*"type"\s*:\s*"(bar|line|pie)"[\s\S]*$/i, '').trim();
    cleanContent = cleanContent.replace(/<CHART>\s*$/i, '').trim();
    return { cleanContent, charts };
  }

  private toUiChartModel(spec: ChartSpec): UiChartModel {
    const xKey = spec.xKey || 'label';
    const yKeys = spec.yKeys?.length ? spec.yKeys : [{ key: 'value', label: 'Value' }];
    const labels = spec.data.map((d) => String(d?.[xKey] ?? ''));
    const rotateTicks = labels.length > 6;

    const commonOptions: ChartOptions = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: yKeys.length > 1 || spec.type === 'pie' },
        tooltip: {
          callbacks: {
            label: (ctx: any) => {
              const dsLabel = ctx?.dataset?.label ? `${ctx.dataset.label}: ` : '';
              return dsLabel + this.formatTooltipValue(ctx?.parsed?.y ?? ctx?.parsed ?? ctx?.raw);
            }
          }
        }
      },
      scales:
        spec.type === 'pie'
          ? undefined
          : {
              x: {
                ticks: {
                  callback: (_value: any, index: number) => this.formatTickValue(labels[index] ?? ''),
                  maxRotation: rotateTicks ? 35 : 0,
                  minRotation: rotateTicks ? 35 : 0
                },
                offset: false
              },
              y: {
                ticks: {
                  callback: (value: any) => this.formatTickValue(Number(value))
                }
              }
            }
    };

    if (spec.type === 'pie') {
      const yKey = yKeys[0]?.key || 'value';
      return {
        type: 'pie',
        title: spec.title,
        data: {
          labels,
          datasets: [
            {
              data: spec.data.map((d) => Number(d?.[yKey] ?? 0)),
              backgroundColor: labels.map((_, i) => CHART_COLORS[i % CHART_COLORS.length])
            }
          ]
        },
        options: commonOptions
      };
    }

    return {
      type: spec.type,
      title: spec.title,
      data: {
        labels,
        datasets: yKeys.map((yk, i) => ({
          label: yk.label || yk.key,
          data: spec.data.map((d) => Number(d?.[yk.key] ?? 0)),
          backgroundColor: yk.color || CHART_COLORS[i % CHART_COLORS.length],
          borderColor: yk.color || CHART_COLORS[i % CHART_COLORS.length],
          borderWidth: spec.type === 'line' ? 2 : 1,
          borderRadius: spec.type === 'bar' ? 4 : 0,
          pointRadius: spec.type === 'line' ? 3 : 0,
          pointHoverRadius: spec.type === 'line' ? 5 : 0,
          tension: spec.type === 'line' ? 0.35 : 0,
          fill: spec.type !== 'line'
        }))
      },
      options: commonOptions
    };
  }

  private buildFallbackChartSpec(renderSpec: any): ChartSpec | null {
    if (!renderSpec) return null;

    const x = renderSpec?.chart?.x;
    const y = renderSpec?.chart?.y;
    if (Array.isArray(x) && Array.isArray(y) && x.length && y.length) {
      const data = x.slice(0, 20).map((label: any, i: number) => ({
        label: String(label),
        value: Number(y[i] ?? 0)
      }));
      return {
        type: 'bar',
        title: renderSpec?.title || 'Trend',
        data,
        xKey: 'label',
        yKeys: [{ key: 'value', label: 'Value' }]
      };
    }

    const columns = renderSpec?.table?.columns;
    const rows = renderSpec?.table?.rows;
    if (Array.isArray(columns) && columns.length >= 2 && Array.isArray(rows) && rows.length) {
      const data = rows.slice(0, 20).map((r: any[]) => ({
        label: String(r?.[0] ?? ''),
        value: Number(String(r?.[1] ?? '').replace(/[^0-9.-]/g, '') || 0)
      }));
      const valid = data.filter((d: any) => d.label && Number.isFinite(d.value));
      if (!valid.length) return null;
      return {
        type: 'bar',
        title: renderSpec?.title || columns[1],
        data: valid,
        xKey: 'label',
        yKeys: [{ key: 'value', label: String(columns[1] || 'Value') }]
      };
    }

    return null;
  }

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
              const formatted = this.currentRender?.chart?.y_formatted?.[index];
              return formatted ?? ctx.raw;
            }
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: (_value: any, index: number) => {
              const formatted = this.currentRender?.chart?.y_formatted?.[index];
              return formatted ?? _value;
            }
          }
        }
      }
    };
  }
}

