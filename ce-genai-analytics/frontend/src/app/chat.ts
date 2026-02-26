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
  minWidthPx?: number;
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
    let pushedStructuredRender = false;

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
          const hasStructuredNonNarrative =
            (Array.isArray(renderSpec?.kpis) && renderSpec.kpis.length > 0) ||
            (Array.isArray(renderSpec?.ranked_list) && renderSpec.ranked_list.length > 0) ||
            (Array.isArray(renderSpec?.bullets) && renderSpec.bullets.length > 0) ||
            (Array.isArray(renderSpec?.table?.rows) && renderSpec.table.rows.length > 0 && this.shouldRenderTable(last));

          if (hasStructuredNonNarrative) {
            pushedStructuredRender = true;
            this.messages.push({
              role: 'bot',
              render: renderSpec,
              queryText: last
            });
          }
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
          this.streamingHtml = this.renderNarrativeHtml(this.streamingText, this.lastUserMessage);
          this.cd.detectChanges();
          this.scrollToBottom();
        });
      }, 0);
    };

    es.addEventListener('done', () => {
      hasReceivedValidEvent = true;
      setTimeout(() => {
        this.zone.run(() => {
          this.finishStreaming(es, pushedStructuredRender);
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

  private finishStreaming(es: EventSource, pushedStructuredRender: boolean = false) {
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
        textHtml: this.renderNarrativeHtml(parsed.cleanContent, this.lastUserMessage),
        analysisCharts: chartSpecs.map((c) => this.toUiChartModel(c)),
        queryText: this.lastUserMessage
      });
    } else if (!pushedStructuredRender && this.lastRenderSpecForNarrative?.narrative) {
      // Fallback for cases where backend sends only render payload without stream text.
      this.messages.push({
        role: 'bot',
        text: this.lastRenderSpecForNarrative.narrative,
        textHtml: this.renderNarrativeHtml(this.lastRenderSpecForNarrative.narrative, this.lastUserMessage),
        analysisCharts: [],
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

  getNarrativeHtml(text: string, questionText: string = ''): string {
    if (!text) return '';
    return this.renderNarrativeHtml(text, questionText);
  }

  private renderNarrativeHtml(text: string, questionText: string = ''): string {
    const cleanText = this.parseChartSpecs(text).cleanContent;
    const normalized = this.normalizeMarkdown(cleanText);
    const html = marked.parse(normalized) as string;
    const cleaned = html
      .replace(/<h1\b/gi, '<h3')
      .replace(/<\/h1>/gi, '</h3>')
      .replace(/<strong>\s*Total\s*<br\s*\/?>\s*Spend:\s*<\/strong>/gi, '<strong>Total Spend:</strong>')
      .replace(/<strong>\s*Total\s*<br\s*\/?>\s*Impressions:\s*<\/strong>/gi, '<strong>Total Impressions:</strong>')
      .replace(/<strong>\s*Total\s*<br\s*\/?>\s*Clicks:\s*<\/strong>/gi, '<strong>Total Clicks:</strong>')
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
    const expandedNames = cleaned
      .replace(/\bSA360\b(?!\s*\()/g, 'SA360 (Search Ads 360)')
      .replace(/\bDV360\b(?!\s*\()/g, 'DV360 (Display & Video 360)')
      .replace(/\bMETA\b(?!\s*\()/g, 'META (Facebook/Instagram)');
    const normalizedHtml = this.postProcessNarrativeHtml(expandedNames);
    if (this.wantsExecutiveSummary(questionText)) {
      return this.toExecutiveSummaryHtml(normalizedHtml);
    }
    return normalizedHtml;
  }

  private wantsExecutiveSummary(questionText: string): boolean {
    const q = (questionText || '').toLowerCase();
    return /\b(executive summary|summary in paragraph|paragraph form|one paragraph summary|brief summary)\b/.test(q);
  }

  private toExecutiveSummaryHtml(html: string): string {
    const parser = new DOMParser();
    const doc = parser.parseFromString(`<div id="exec-root">${html}</div>`, 'text/html');
    const root = doc.getElementById('exec-root');
    if (!root) return html;

    const heading = root.querySelector('h1,h2,h3,h4,h5,h6');
    const title = 'Executive summary';

    const texts = Array.from(root.querySelectorAll('p,li'))
      .map((n) => (n.textContent || '').replace(/\s+/g, ' ').trim())
      .filter((t) => t && t.length > 2)
      .filter((t) => !/^(key takeaway|key takeaways|overall performance|platform-by-platform detail|chart analysis|suggested next step)$/i.test(t));

    const deduped: string[] = [];
    const seen = new Set<string>();
    for (const t of texts) {
      const key = t.toLowerCase().replace(/[^\w\s%$.-]/g, '');
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(t);
      if (deduped.length >= 8) break;
    }

    const paragraph = deduped.join(' ').trim();
    if (!paragraph) return html;

    return `<h3>${title}</h3><p>${paragraph}</p>`;
  }

  private postProcessNarrativeHtml(html: string): string {
    const parser = new DOMParser();
    const doc = parser.parseFromString(`<div id="narr-root">${html}</div>`, 'text/html');
    const root = doc.getElementById('narr-root');
    if (!root) return html;

    const sectionHeadingRegex =
      /^(Overview|Key takeaway|Key takeaways|Overall performance(?:\s*\(all platforms\))?|Platform-by-platform detail|Data quality notes|What stands out|Suggested next step|Suggested takeaway(?:\s*\/\s*next step)?|Suggested takeaway|Next step|Chart analysis)\s*:?\s*(.*)$/i;
    const narrativeStarterRegex = /\b(Overall|Today|This|In this|For this|Here)\b/;

    const cleanText = (value: string) => value.replace(/\s+/g, ' ').trim();
    const isMetricParagraph = (el: Element | null): el is HTMLParagraphElement => {
      if (!el || el.tagName !== 'P') return false;
      const text = cleanText(el.textContent || '');
      return /^[A-Za-z][A-Za-z0-9\s()/&%.-]{1,50}\s*:/.test(text);
    };
    const canonicalHeading = (value: string): string => {
      const t = cleanText(value).toLowerCase();
      if (/^key takeaway(s)?$/.test(t)) return 'Key takeaway';
      if (/^what stands out$/.test(t)) return 'What stands out';
      if (/^chart analysis$/.test(t)) return 'Chart analysis';
      if (/^platform-by-platform detail$/.test(t)) return 'Platform-by-platform detail';
      if (/^overall performance(\s*\(all platforms\))?$/.test(t)) return t.includes('(all platforms)') ? 'Overall performance (all platforms)' : 'Overall performance';
      if (/^data quality notes$/.test(t)) return 'Data quality notes';
      if (/^overview$/.test(t)) return 'Overview';
      if (/^suggested next step$/.test(t)) return 'Suggested next step';
      if (/^suggested takeaway(\/next step)?$/.test(t)) return 'Suggested takeaway / next step';
      return cleanText(value);
    };

    const removeEmptyNodes = () => {
      const removable = root.querySelectorAll('p, li, h1, h2, h3, h4, h5, h6, ul, ol');
      removable.forEach((node) => {
        const text = cleanText(node.textContent || '');
        if (!text || text === '-' || text === '*' || text === '•') {
          node.remove();
        }
      });
    };

    // Normalize paragraphs that accidentally contain section headers.
    root.querySelectorAll('p').forEach((p) => {
      const text = cleanText(p.textContent || '');
      const match = text.match(sectionHeadingRegex);
      if (!match) return;

      const h = doc.createElement('h3');
      h.textContent = match[1];
      p.parentNode?.insertBefore(h, p);

      const remainder = cleanText(match[2] || '');
      if (remainder) {
        const nextP = doc.createElement('p');
        nextP.textContent = remainder;
        p.parentNode?.insertBefore(nextP, p);
      }
      p.remove();
    });

    // Split glued heading + paragraph content.
    root.querySelectorAll('h1,h2,h3,h4,h5,h6').forEach((h) => {
      const text = cleanText(h.textContent || '').replace(/^#\s*/, '');
      if (!text) {
        h.remove();
        return;
      }

      const sectionMatch = text.match(sectionHeadingRegex);
      if (sectionMatch && sectionMatch[2]) {
        h.textContent = sectionMatch[1];
        const p = doc.createElement('p');
        p.textContent = cleanText(sectionMatch[2]);
        h.parentNode?.insertBefore(p, h.nextSibling);
        return;
      }

      if (text.length > 70) {
        const splitIdx = text.search(narrativeStarterRegex);
        if (splitIdx > 15) {
          const headingPart = cleanText(text.slice(0, splitIdx));
          const bodyPart = cleanText(text.slice(splitIdx));
          if (headingPart && bodyPart) {
            h.textContent = headingPart;
            const p = doc.createElement('p');
            p.textContent = bodyPart;
            h.parentNode?.insertBefore(p, h.nextSibling);
          }
        } else {
          h.textContent = text;
        }
      } else {
        h.textContent = text;
      }
      h.textContent = canonicalHeading(h.textContent || '');
    });

    // If list item is only a heading marker, move it out so list alignment stays valid.
    root.querySelectorAll('li').forEach((li) => {
      const children = Array.from(li.children);
      const first = children[0];
      if (!first || !/^H[1-6]$/.test(first.tagName)) return;

      const parentList = li.parentElement;
      if (!parentList) return;

      const heading = first.cloneNode(true) as HTMLElement;
      parentList.parentNode?.insertBefore(heading, parentList);

      const clone = li.cloneNode(true) as HTMLElement;
      const headingInClone = clone.querySelector(first.tagName.toLowerCase());
      headingInClone?.remove();
      const rest = cleanText(clone.textContent || '');
      if (rest) {
        const p = doc.createElement('p');
        p.textContent = rest;
        parentList.parentNode?.insertBefore(p, parentList);
      }
      li.remove();
    });

    // Flatten stray paragraphs inside list items (causes uneven spacing).
    root.querySelectorAll('li p').forEach((p) => {
      const text = cleanText(p.textContent || '');
      const li = p.parentElement;
      if (!li) return;
      const replacement = doc.createTextNode(text);
      li.replaceChild(replacement, p);
    });

    // Dedupe repeated bullets within the same list (common in streamed retries).
    root.querySelectorAll('ul,ol').forEach((list) => {
      const seen = new Set<string>();
      Array.from(list.children).forEach((child) => {
        if (child.tagName !== 'LI') return;
        const li = child as HTMLElement;
        const key = cleanText(li.textContent || '')
          .toLowerCase()
          .replace(/[^\w\s%$.-]/g, '');
        if (!key || key === '-' || key === '*') {
          li.remove();
          return;
        }
        if (seen.has(key)) {
          li.remove();
          return;
        }
        seen.add(key);
      });
    });

    // Remove empty lists left after cleanup.
    root.querySelectorAll('ul,ol').forEach((list) => {
      if (!list.querySelector('li')) {
        list.remove();
      }
    });

    // Group metric paragraphs under headings into a compact list for clearer hierarchy.
    // Supports an optional narrative intro paragraph before metrics.
    root.querySelectorAll('h3,h4').forEach((heading) => {
      const firstNext = heading.nextElementSibling;
      if (firstNext && firstNext.classList.contains('narrative-metric-list')) return;

      const metricPs: HTMLParagraphElement[] = [];
      let cursor = heading.nextElementSibling;
      if (cursor && cursor.tagName === 'P' && !isMetricParagraph(cursor)) {
        // keep one non-metric intro paragraph, then collect metric rows after it
        cursor = cursor.nextElementSibling;
      }
      while (isMetricParagraph(cursor)) {
        metricPs.push(cursor);
        cursor = cursor.nextElementSibling;
      }
      if (metricPs.length < 2) return;

      const ul = doc.createElement('ul');
      ul.className = 'narrative-metric-list';
      metricPs.forEach((p) => {
        const li = doc.createElement('li');
        li.innerHTML = (p.innerHTML || '').replace(/(<br\s*\/?>\s*)+$/gi, '').trim();
        ul.appendChild(li);
      });

      metricPs[0].parentNode?.insertBefore(ul, metricPs[0]);
      metricPs.forEach((p) => p.remove());
    });

    // Heal split heading cases like "Suggested" + "next step ...".
    root.querySelectorAll('h1,h2,h3,h4,h5,h6').forEach((h) => {
      const headingText = cleanText(h.textContent || '').toLowerCase();
      if (headingText !== 'suggested') return;

      const next = h.nextElementSibling;
      if (!next || next.tagName !== 'P') return;
      const body = cleanText(next.textContent || '');
      const m = body.match(/^next step\s*:?\s*(.*)$/i);
      if (!m) return;

      h.textContent = 'Suggested next step';
      next.textContent = cleanText(m[1] || '');
      if (!next.textContent) next.remove();
    });

    // Merge parenthetical-only paragraphs into previous heading: "Overview" + "(Feb 2026)".
    root.querySelectorAll('h1,h2,h3,h4,h5,h6').forEach((h) => {
      const next = h.nextElementSibling;
      if (!next || next.tagName !== 'P') return;
      const ptxt = cleanText(next.textContent || '');
      if (!/^\([^()]{2,80}\)$/.test(ptxt)) return;
      h.textContent = `${cleanText(h.textContent || '')} ${ptxt}`;
      next.remove();
    });

    // Merge parenthetical-only single-item UL into previous heading:
    // "Spend trend analysis (January 27" + "- February 20, 2026)".
    root.querySelectorAll('h1,h2,h3,h4,h5,h6').forEach((h) => {
      const next = h.nextElementSibling;
      if (!next || next.tagName !== 'UL') return;
      const items = Array.from(next.querySelectorAll('li'));
      if (items.length !== 1) return;
      const litxt = cleanText(items[0].textContent || '');
      if (!/^[A-Za-z]+\s+\d{1,2},\s+\d{4}\)$/.test(litxt)) return;
      h.textContent = `${cleanText(h.textContent || '')} ${litxt}`;
      next.remove();
    });

    // Convert single-item date bullet list under Overview into paragraph.
    root.querySelectorAll('h1,h2,h3,h4,h5,h6').forEach((h) => {
      if (!/^overview$/i.test(cleanText(h.textContent || ''))) return;
      const next = h.nextElementSibling;
      if (!next || next.tagName !== 'UL') return;
      const items = Array.from(next.querySelectorAll('li'));
      if (items.length !== 1) return;
      const txt = cleanText(items[0].textContent || '');
      if (!/^[A-Za-z]+\s+\d{1,2},\s+\d{4}$/.test(txt)) return;
      const p = doc.createElement('p');
      p.textContent = txt;
      next.parentNode?.insertBefore(p, next);
      next.remove();
    });

    // Remove empty section headers that are immediately followed by another header.
    root.querySelectorAll('h1,h2,h3,h4,h5,h6').forEach((h) => {
      const next = h.nextElementSibling;
      if (!next || !/^H[1-6]$/.test(next.tagName)) return;
      const txt = cleanText(h.textContent || '');
      if (/^(What stands out|Overview|Key takeaway|Key takeaways|Chart analysis|Data quality notes)$/i.test(txt)) {
        h.remove();
      }
    });

    removeEmptyNodes();
    removeEmptyNodes();
    return root.innerHTML;
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

  hasRenderableContent(render: any, queryText: string = ''): boolean {
    if (!render || !render.render_type) return false;

    if (render.render_type === 'mixed') {
      const hasNarrative = typeof render.narrative === 'string' && render.narrative.trim().length > 0;
      const hasTable = this.shouldRenderTable(queryText) && Array.isArray(render.table?.rows) && render.table.rows.length > 0;
      return hasNarrative || hasTable;
    }

    if (render.render_type === 'table') {
      const hasNarrative = typeof render.narrative === 'string' && render.narrative.trim().length > 0;
      const hasTable = this.shouldRenderTable(queryText) && Array.isArray(render.table?.rows) && render.table.rows.length > 0;
      return hasNarrative || hasTable;
    }

    if (render.render_type === 'ranked_list') {
      return (Array.isArray(render.ranked_list) && render.ranked_list.length > 0) ||
        (typeof render.narrative === 'string' && render.narrative.trim().length > 0);
    }

    if (render.render_type === 'narrative') {
      return typeof render.narrative === 'string' && render.narrative.trim().length > 0;
    }

    if (render.render_type === 'kpi') {
      return Array.isArray(render.kpis) && render.kpis.length > 0;
    }

    return true;
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
    out = out.replace(/(this month\s*\([^)]*\))\s*(Key finding\s*\([^)]*\)\s*:)/gi, '### $1\n\n### $2');
    out = out.replace(/(Month-to-Date\s*\([^)]*\))\s*(Key finding\s*\([^)]*\)\s*:)/gi, '### $1\n\n### $2');
    out = out.replace(/(\.\s*)(This amount reflects\b)/gi, '$1\n\n$2');
    out = out.replace(/(\.\s*)(\*\*Takeaway:\*\*|Takeaway:)/gi, '$1\n\n### Takeaway:\n\n');
    out = out.replace(/([.!?]\s*)(A bit more detail\s*:)/gi, '$1\n\n### $2');
    out = out.replace(/([.!?]\s*)(Takeaway\s*:)/gi, '$1\n\n### $2');
    out = out.replace(/(Key finding\s*\([^)]*\)\s*:)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/(A bit more detail\s*:)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/(Takeaway\s*:)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/(Overall performance \(all platforms\))\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/(What stands out)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/(Chart analysis)\s*([A-Z])/gi, '### $1\n\n$2');
    out = out.replace(/^\s*(Overview|Key takeaway|Platform-by-platform detail|Data quality notes|Suggested takeaway\s*\/\s*next step|Chart analysis)\s*$/gim, '### $1');
    out = out.replace(/([^\n])\s+(Overview|Key takeaway|Platform-by-platform detail|Data quality notes|Suggested takeaway\s*\/\s*next step|Chart analysis)\b/gi, '$1\n\n### $2');
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
    out = out.replace(
      /((?:Total Spend|Total Impressions|Total Clicks|Total Enrollments|Enrollment Rate|Cost per Enrollment|Spend|Impressions|Clicks)\s*:[^\n]+)\s+(?=(?:Total Spend|Total Impressions|Total Clicks|Total Enrollments|Enrollment Rate|Cost per Enrollment|Spend|Impressions|Clicks)\s*:)/gi,
      '$1\n\n'
    );

    // Break out common metric labels if they are glued inline.
    out = out.replace(
      /([^\n])\s+((Total Spend|Total Clicks|Total Impressions|Click-Through Rate \(CTR\)|CTR)\s*:)/gi,
      '$1\n$2'
    );

    // Promote plain section labels to markdown headings.
    out = out.replace(/^\s*([A-Za-z][A-Za-z0-9' ()/&-]{0,60}:\s*Key Takeaways)\s*$/gim, '### $1');
    out = out.replace(/^\s*(What Stands Out|What stands out|Key Takeaways|Key takeaway|Performance Snapshot(?:\s*\([^)]*\))?|Overall performance \(all platforms\)|Key Findings|Detailed Insights|Chart analysis)\s*$/gim, '### $1');
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

  private normalizePlatformLabel(label: string): string {
    const raw = (label || '').trim();
    const low = raw.toLowerCase();
    if (low === 'sa360' || low === 'google' || low === 'sa360 (google)') return 'SA360 (Search Ads 360)';
    if (low === 'dv360') return 'DV360 (Display & Video 360)';
    if (low === 'meta' || low === 'facebook' || low === 'meta (facebook)') return 'META (Facebook/Instagram)';
    return raw;
  }

  parseChartSpecs(content: string): { cleanContent: string; charts: ChartSpec[] } {
    const charts: ChartSpec[] = [];
    let cleanContent = content;

    const chartRegex = /<\s*CHART\s*>([\s\S]*?)<\s*\/\s*CHART\s*>/gi;
    let match: RegExpExecArray | null;

    while ((match = chartRegex.exec(content)) !== null) {
      const parsed = this.tryParseChartSpec(match[1]);
      if (parsed) {
        charts.push(parsed);
      }
    }

    // Tolerate incomplete stream where </CHART> may not have arrived in final chunk.
    if (!charts.length) {
      const openOnly = content.match(/<\s*CHART\s*>([\s\S]*)$/i);
      if (openOnly?.[1]) {
        const parsed = this.tryParseChartSpec(openOnly[1]);
        if (parsed) {
          charts.push(parsed);
        }
      }
    }

    // Fallback: tolerate raw JSON chart specs not wrapped in <CHART> tags.
    if (!charts.length) {
      const looseJsonRegex = /\{[\s\S]*?"type"\s*:\s*"(bar|line|pie)"[\s\S]*?"data"\s*:\s*\[[\s\S]*?\][\s\S]*?"xKey"\s*:\s*"[^"]+"[\s\S]*?"yKeys"\s*:\s*\[[\s\S]*?\][\s\S]*?\}/g;
      const looseBlocks = content.match(looseJsonRegex) || [];
      for (const block of looseBlocks) {
        const parsed = this.tryParseChartSpec(block);
        if (parsed) {
          charts.push(parsed);
          cleanContent = cleanContent.replace(block, '');
        }
      }
    }

    cleanContent = cleanContent.replace(chartRegex, '').trim();
    cleanContent = cleanContent.replace(/<\s*CHART\s*>[\s\S]*$/i, '').trim();
    cleanContent = cleanContent.replace(/\{\s*"type"\s*:\s*"(bar|line|pie)"[\s\S]*$/i, '').trim();
    cleanContent = cleanContent.replace(/<\s*CHART\s*>\s*$/i, '').trim();
    return { cleanContent, charts };
  }

  private tryParseChartSpec(rawContent: string): ChartSpec | null {
    const raw = (rawContent || '').trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');
    if (!raw) return null;

    const direct = this.safeParseJson(raw);
    if (this.isValidChartSpec(direct)) return direct as ChartSpec;

    const balanced = this.extractBalancedJson(raw);
    if (!balanced) return null;
    const parsed = this.safeParseJson(balanced);
    if (this.isValidChartSpec(parsed)) return parsed as ChartSpec;

    return null;
  }

  private safeParseJson(value: string): any | null {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }

  private isValidChartSpec(parsed: any): boolean {
    return !!(parsed && parsed.type && parsed.data && parsed.xKey && parsed.yKeys);
  }

  // Extract first balanced JSON object from a potentially incomplete stream.
  private extractBalancedJson(value: string): string | null {
    const start = value.indexOf('{');
    if (start === -1) return null;

    let depth = 0;
    let inString = false;
    let escape = false;

    for (let i = start; i < value.length; i++) {
      const ch = value[i];
      if (inString) {
        if (escape) {
          escape = false;
          continue;
        }
        if (ch === '\\') {
          escape = true;
          continue;
        }
        if (ch === '"') inString = false;
        continue;
      }

      if (ch === '"') {
        inString = true;
        continue;
      }

      if (ch === '{') depth++;
      if (ch === '}') {
        depth--;
        if (depth === 0) {
          return value.slice(start, i + 1);
        }
      }
    }

    return null;
  }

  private toUiChartModel(spec: ChartSpec): UiChartModel {
    const xKey = spec.xKey || 'label';
    const yKeys = spec.yKeys?.length ? spec.yKeys : [{ key: 'value', label: 'Value' }];
    const labels = spec.data.map((d) => this.normalizePlatformLabel(String(d?.[xKey] ?? '')));
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

    const barCount = labels.length || 1;
    const maxBarThickness =
      barCount >= 30 ? 8 :
      barCount >= 20 ? 12 :
      barCount >= 12 ? 18 : 28;
    const widthPerPoint = spec.type === 'bar' ? 58 : 42;
    const minWidthPx = Math.max(640, barCount * widthPerPoint);

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
          maxBarThickness: spec.type === 'bar' ? maxBarThickness : undefined,
          barPercentage: spec.type === 'bar' ? 0.72 : undefined,
          categoryPercentage: spec.type === 'bar' ? 0.74 : undefined,
          pointRadius: spec.type === 'line' ? 3 : 0,
          pointHoverRadius: spec.type === 'line' ? 5 : 0,
          tension: spec.type === 'line' ? 0.35 : 0,
          fill: spec.type !== 'line'
        }))
      },
      options: commonOptions,
      minWidthPx
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
