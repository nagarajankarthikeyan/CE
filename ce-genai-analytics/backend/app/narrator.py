from app.gpt_client import stream_chat_completion
import json
from datetime import date, datetime, timedelta


ANALYSIS_SYSTEM = """You are AskConnie, an expert marketing data analyst for Constellation. You just ran a SQL query for a marketer and got results back. Your job is to present the findings in clear, conversational prose that a non-technical marketer can understand.

## Rules for your analysis
1. Write in natural language - do NOT output raw tables or pipe-delimited data.
2. Start with one short scope sentence on what window/slice you summarized.
3. Use this dynamic response pattern (adapt wording to the question):
   - "<time window/topic> — Key takeaways"
   - "Overall performance (all platforms)" when platform/source fields exist, otherwise "Performance snapshot (<time/topic>)"
   - "Platform-by-platform detail" when platform/source fields exist
   - "Data quality notes (important)" when null/unknown/zero-only artifacts appear
   - "Suggested takeaway / next step"
4. Summarize key findings up front, then provide detail.
4a. In "Key takeaways", include at least 4 bullets when data supports it (totals, efficiency, leading driver, and one additional insight).
5. Use markdown formatting with proper headings and bullet lists on separate lines.
6. Format numbers in a human-friendly way: use dollar signs for money ($9,096.49), percentages for rates (12.3%), and abbreviations for large numbers (1.2M).
7. Format dates in a readable way (e.g., "February 21, 2026" not "2026-02-21").
8. If there are trends, comparisons, or outliers, call them out.
9. If data is small (few rows), mention each key point; if large, summarize patterns and highlight top/bottom performers.
10. Only mention metrics present in the query result. Do not invent values.
11. End with a brief takeaway or suggestion if appropriate.
12. Do not output markdown tables.
13. Avoid generic statements; tie every claim to a metric from results.
14. For relative periods like "last week", "this week", "last month", include the explicit date range in the first heading using readable dates.
15. Preferred heading style example: "Last week's program performance (February 16-22, 2026) - Energy".
16. If you include a chart, also include a short "Chart analysis" subsection in prose (2-4 bullets) that explains the top performer, lowest performer, and concentration/share pattern.
17. For single-row or single-metric answers (for example: "how much was spend on meta this month"), use a compact format:
   - one brief lookup sentence
   - "Key finding (month-to-date)" (or matching period)
   - optional "A bit more detail"
   - "Takeaway"
18. Include explicit date ranges for relative periods in prose, like "February 1, 2026 through February 25, 2026".
19. For campaign-list performance questions (for example campaigns with spend/impressions/clicks), summarize patterns: top spenders, impression leaders, click leaders, and notable outliers.
20. For trend-over-time questions, include a short breakdown of how each major group (platform/source/channel) moved over the period.
21. For spend-only questions with grouped rows, use this structure:
   - "Key finding (<period>)" with total spend
   - "A bit more detail"
   - one total line plus per-group amount lines
   - "Platform Breakdown" (or "<group> Breakdown") with share %
   - "Suggested Takeaway"
   - Include a detailed breakup list across returned groups (not just a single top group) when group rows are available.
   - If grouped rows are present, the breakdown section is mandatory; do not return only total spend.
22. For program-performance questions with a weekly/monthly window, use this structure:
   - "Key takeaways"
   - "Overall totals (all platforms)"
   - "Platform-by-platform detail"
   - "What stands out"
   - "Suggested next step"
   Include enrollment component breakout (online completes, call enrollments, view-based enrollments) when those fields exist.

## Important Metric Rules
- For program performance questions, prioritize: spend, impressions, clicks, CTR, CPC, CPM, total enrollments, cost per enrollment (CPE), enrollment rate.
- If both totals and platform/source rows are present, include both.
- If grouped rows are present (2+ groups), include a concise breakdown subsection with one bullet per major group.
- For "how much spend" style questions, explicitly include:
  - one total amount line, and
  - one breakdown subsection by the grouped property returned by SQL.
- If datasource/platform is null/unknown or has impossible combinations (e.g., enrollments with zero spend/clicks), explicitly call this out in data quality notes.
- When rates are already percentages in data, do NOT multiply by 100 again.
- For "last week", assume Monday-Sunday week boundaries.
- For "this month", assume month-to-date from first day of month through today.
- ROAS = revenue / ad_spend.
- In SA360, campaigns containing 'NB' are non-brand (incremental growth); they can be more expensive than branded campaigns.
- Call tracking conversions are credited to SA360 and should be treated as supplemental, not as missing-data issues.
- Total enrollments should include enrollment_completes + call_enrollments + enrollment_completes_views when available.
- For Home Services outcomes, include hs_request_estimate_submit + hs_request_estimate_submit_views + hs_schedule_service_submit when available.
- For platform/source breakdown responses, include both:
  - scale metrics (spend, impressions, clicks), and
  - efficiency metrics (CTR, CPC, CPM, CPE, enrollment_rate) whenever present.
- If CTR is present in results and the question is about performance, trend, traffic, or efficiency, include CTR explicitly
  in overall totals and in grouped breakdown sections.
- If CTR is not directly present but clicks and impressions are present, compute CTR as SAFE_DIVIDE(clicks, impressions) * 100
  and include it for performance/trend/traffic/efficiency questions.
- When enrollment components are present, explicitly break them out (completes, calls, view-through).

## Snapshot Grouping Guidance
Use only groups relevant to available metrics:
- Scale & Spend: spend, impressions, clicks, reach.
- Engagement / Cost Efficiency: CTR, CPC, CPM, CPA, CPV, frequency, cost per enrollment.
- Outcome Metrics: total enrollments, enrollment rate, conversions, calls, leads, revenue.

## Data Visualization
When the data would benefit from a chart (comparisons, trends over time, distributions, rankings), include a chart specification. Place it AFTER your prose analysis using a <CHART> tag with JSON inside.

Chart JSON format:
{
  "type": "bar" | "line" | "pie",
  "title": "Chart title",
  "data": [ { "label": "...", "value": 123 }, ... ],
  "xKey": "label",
  "yKeys": [ { "key": "value", "label": "Human-readable label" } ]
}

Guidelines for chart type:
- bar: comparing categories and rankings.
- line: trends over time.
- pie: proportions/share of total (only when 2-7 categories and composition-focused).

Rules:
- Include a chart whenever there are 2+ meaningful numeric data points.
- Keep labels clean and readable (e.g., "Feb 21").
- Use numeric values (not strings) for y-axis series.
- Limit to 20 points max.
- Only one chart per response.
- Prefer bar chart for platform/source comparison; line for time trends.
- The <CHART> tag must contain valid JSON and nothing else.
- The "Chart analysis" prose must appear before the <CHART> block.
- For trend-over-time questions, chart analysis must call out trend direction, major spikes/dips, and which group drove the movement.
- Use the exact heading text: "Chart analysis".
- If a <CHART> block is provided, always include a non-empty "Chart analysis" section.
"""


def json_safe(obj):
    """
    Makes BigQuery date/datetime JSON serializable.
    """
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return str(obj)


def format_results_for_llm(rows: list, total_rows: int) -> str:
    """
    Formats query rows for analysis context while keeping deterministic structure.
    This is for LLM input only; user-facing output should remain narrative.
    """
    if not rows:
        return "The query returned no results."

    headers = list(rows[0].keys())
    text = f"Query returned {total_rows} row{'s' if total_rows != 1 else ''}. Here is the data:\n\n"
    text += "| " + " | ".join(headers) + " |\n"
    text += "| " + " | ".join(["---"] * len(headers)) + " |\n"

    for row in rows[:100]:
        values = []
        for h in headers:
            val = row.get(h)
            if val is None:
                values.append("null")
            elif isinstance(val, dict) and "value" in val:
                values.append(str(val["value"]))
            else:
                values.append(str(val))
        text += "| " + " | ".join(values) + " |\n"

    if total_rows > 100:
        text += f"\n(Showing first 100 of {total_rows} rows)\n"

    return text


def _safe_float(val):
    if val is None:
        return None
    try:
        if isinstance(val, str):
            cleaned = val.replace("$", "").replace("%", "").replace(",", "").strip()
            if cleaned == "":
                return None
            return float(cleaned)
        return float(val)
    except Exception:
        return None


def _find_key(row: dict, candidates: list[str]) -> str | None:
    lowered = {str(k).strip().lower(): k for k in row.keys()}
    for c in candidates:
        if c in lowered:
            return lowered[c]
    return None


def _fmt_long_date(d: date) -> str:
    return f"{d.strftime('%B')} {d.day}, {d.year}"


def build_period_facts(question: str) -> str:
    q = (question or "").lower()
    today = date.today()
    facts = []

    if "last week" in q:
        week_start = today - timedelta(days=today.weekday() + 7)
        week_end = week_start + timedelta(days=6)
        facts.append(
            f"- Last week date range (Monday-Sunday): {_fmt_long_date(week_start)} through {_fmt_long_date(week_end)}"
        )

    if "this month" in q or "month-to-date" in q or "mtd" in q:
        month_start = date(today.year, today.month, 1)
        facts.append(
            f"- This month-to-date range: {_fmt_long_date(month_start)} through {_fmt_long_date(today)}"
        )
    if "yesterday" in q:
        y = today - timedelta(days=1)
        facts.append(f"- Yesterday date: {_fmt_long_date(y)}")

    if any(k in q for k in ["meta", "facebook", "instagram"]):
        facts.append("- Meta scope: include Meta/Facebook/Instagram platform labels when applicable.")

    if not facts:
        return "- No explicit relative period detected."

    return "\n".join(facts)


def build_verified_facts(rows: list, render_spec: dict) -> str:
    facts = []

    # Pull KPI-level totals from render spec when available.
    kpis = render_spec.get("kpis") or []
    for k in kpis:
        label = str(k.get("label", "")).lower()
        value = str(k.get("value", "")).strip()
        if not value:
            continue
        if "spend" in label and ("total" in label or label == "spend"):
            facts.append(f"- Total spend: {value}")
        elif "enrollment" in label and "cost" not in label and "rate" not in label:
            facts.append(f"- Total enrollments: {value}")
        elif "cost per enrollment" in label or label == "cpe":
            facts.append(f"- Cost per enrollment (CPE): {value}")

    # Infer top platform/source by enrollments and clicks from row-level data.
    if rows:
        sample = rows[0]
        source_key = _find_key(sample, ["datasource", "platform", "source", "channel"])
        enroll_key = _find_key(sample, ["total_enrollments", "enrollments", "total_enrollment", "enrollment_count"])
        clicks_key = _find_key(sample, ["clicks", "total_clicks"])

        by_source = {}
        if source_key and (enroll_key or clicks_key):
            for r in rows:
                src = str(r.get(source_key, "")).strip()
                if not src or src.lower() in {"null", "none", "unknown", "(not set)"}:
                    continue
                if src not in by_source:
                    by_source[src] = {"enrollments": 0.0, "clicks": 0.0}
                if enroll_key:
                    v = _safe_float(r.get(enroll_key))
                    if v is not None:
                        by_source[src]["enrollments"] += v
                if clicks_key:
                    v = _safe_float(r.get(clicks_key))
                    if v is not None:
                        by_source[src]["clicks"] += v

        if by_source:
            if any(v["enrollments"] > 0 for v in by_source.values()):
                top_enroll = max(by_source.items(), key=lambda kv: kv[1]["enrollments"])
                facts.append(f"- Enrollment driver: {top_enroll[0]} ({top_enroll[1]['enrollments']:.0f} enrollments)")
            if any(v["clicks"] > 0 for v in by_source.values()):
                top_clicks = max(by_source.items(), key=lambda kv: kv[1]["clicks"])
                facts.append(f"- Click volume leader: {top_clicks[0]} ({top_clicks[1]['clicks']:.0f} clicks)")

    if not facts:
        return "- No pre-verified summary facts were derived."
    return "\n".join(dict.fromkeys(facts))


def build_breakdown_facts(rows: list) -> str:
    if not rows:
        return "- No breakdown facts derived."

    sample = rows[0]
    spend_key = _find_key(sample, ["spend", "total_spend", "amount_spent"])
    group_key = _find_key(sample, ["platform", "datasource", "source", "channel"])
    if not spend_key or not group_key:
        return "- No breakdown facts derived."

    grouped = {}
    total = 0.0

    for r in rows:
        g = str(r.get(group_key, "")).strip()
        if not g or g.lower() in {"total", "all"}:
            continue
        v = _safe_float(r.get(spend_key))
        if v is None:
            continue
        grouped[g] = grouped.get(g, 0.0) + v
        total += v

    if total <= 0 or not grouped:
        return "- No breakdown facts derived."

    ordered = sorted(grouped.items(), key=lambda kv: kv[1], reverse=True)
    # Keep detail rich but bounded.
    top = ordered[:10]
    lines = [f"- Breakdown dimension: {group_key}"]
    for g, amt in top:
        pct = (amt / total) * 100 if total else 0
        lines.append(f"- {g}: ${amt:,.2f} ({pct:.1f}% of grouped spend)")
    if len(ordered) > len(top):
        remainder = sum(v for _, v in ordered[len(top):])
        pct = (remainder / total) * 100 if total else 0
        lines.append(f"- Remaining groups combined: ${remainder:,.2f} ({pct:.1f}% of grouped spend)")
    return "\n".join(lines)


def build_program_performance_facts(rows: list) -> str:
    if not rows:
        return "- No program facts derived."

    sample = rows[0]
    platform_key = _find_key(sample, ["platform", "source", "channel", "datasource"])
    spend_key = _find_key(sample, ["spend", "total_spend"])
    impressions_key = _find_key(sample, ["impressions", "total_impressions"])
    clicks_key = _find_key(sample, ["clicks", "total_clicks"])
    enroll_total_key = _find_key(sample, ["total_enrollments", "enrollments", "total_enrollment"])
    enroll_comp_key = _find_key(sample, ["enrollment_completes", "total_enrollment_completes"])
    call_enroll_key = _find_key(sample, ["call_enrollments", "total_call_enrollments"])
    view_enroll_key = _find_key(sample, ["enrollment_completes_views", "total_enrollment_completes_views"])

    if not platform_key:
        return "- No program facts derived."

    lines = []
    total_row = None
    for r in rows:
        p = str(r.get(platform_key, "")).strip().lower()
        d = str(r.get(_find_key(r, ["datasource"]) or "", "")).strip().lower()
        if p in {"total", "all"} or d == "all":
            total_row = r
            break

    def pick_total(key_name: str | None):
        if not key_name:
            return None
        if total_row:
            return _safe_float(total_row.get(key_name))
        s = 0.0
        seen = False
        for r in rows:
            p = str(r.get(platform_key, "")).strip().lower()
            if p in {"total", "all"}:
                continue
            v = _safe_float(r.get(key_name))
            if v is not None:
                s += v
                seen = True
        return s if seen else None

    t_spend = pick_total(spend_key)
    t_impr = pick_total(impressions_key)
    t_clicks = pick_total(clicks_key)
    t_enroll = pick_total(enroll_total_key)
    t_comp = pick_total(enroll_comp_key)
    t_call = pick_total(call_enroll_key)
    t_view = pick_total(view_enroll_key)

    if t_spend is not None:
        lines.append(f"- Total spend: ${t_spend:,.2f}")
    if t_impr is not None:
        lines.append(f"- Total impressions: {t_impr:,.0f}")
    if t_clicks is not None:
        lines.append(f"- Total clicks: {t_clicks:,.0f}")
    if t_clicks is not None and t_impr is not None and t_impr > 0:
        lines.append(f"- Overall CTR: {((t_clicks / t_impr) * 100):.2f}%")
    if t_enroll is not None:
        lines.append(f"- Total enrollments: {t_enroll:,.0f}")
    if t_comp is not None or t_call is not None or t_view is not None:
        parts = []
        if t_comp is not None:
            parts.append(f"{t_comp:,.0f} online completes")
        if t_call is not None:
            parts.append(f"{t_call:,.0f} call enrollments")
        if t_view is not None:
            parts.append(f"{t_view:,.0f} view-based completes")
        if parts:
            lines.append("- Enrollment components: " + ", ".join(parts))

    if spend_key and enroll_total_key:
        grouped = []
        for r in rows:
            p = str(r.get(platform_key, "")).strip()
            if not p or p.lower() in {"total", "all"}:
                continue
            s = _safe_float(r.get(spend_key))
            e = _safe_float(r.get(enroll_total_key))
            c = _safe_float(r.get(clicks_key)) if clicks_key else None
            if s is None and e is None and c is None:
                continue
            grouped.append((p, s or 0.0, e or 0.0, c or 0.0))
        if grouped and t_enroll and t_enroll > 0:
            top_enroll = max(grouped, key=lambda x: x[2])
            lines.append(f"- Top enrollment driver: {top_enroll[0]} ({top_enroll[2]:,.0f}, {((top_enroll[2]/t_enroll)*100):.1f}% share)")
        if grouped and t_clicks and t_clicks > 0:
            top_click = max(grouped, key=lambda x: x[3])
            lines.append(f"- Top click driver: {top_click[0]} ({top_click[3]:,.0f}, {((top_click[3]/t_clicks)*100):.1f}% share)")

    return "\n".join(lines) if lines else "- No program facts derived."


async def stream_narrative(question: str, rows: list, render_spec: dict):
    """
    Async generator.
    Streams analysis tokens progressively.
    BigQuery-safe JSON serialization.
    """

    safe_rows = json.loads(json.dumps(rows[:100], default=json_safe))
    safe_render = json.loads(json.dumps(render_spec, default=json_safe))
    query_result_text = format_results_for_llm(safe_rows, len(rows))
    verified_facts = build_verified_facts(safe_rows, safe_render)
    period_facts = build_period_facts(question)
    breakdown_facts = build_breakdown_facts(safe_rows)
    program_facts = build_program_performance_facts(safe_rows)

    prompt = f"""
The user asked:
{question}

Today's date:
{date.today().isoformat()}

Verified facts (use these exact values when referenced):
{verified_facts}

Period facts (must be reflected when applicable):
{period_facts}

Breakdown facts (if present, use in breakdown section):
{breakdown_facts}

Program performance facts (if present, use for weekly/monthly performance responses):
{program_facts}

Here are the query results:
{query_result_text}

Structured result:
{json.dumps(safe_render, indent=2)}

Sample rows:
{json.dumps(safe_rows, indent=2)}

Generate a dynamic business summary that follows the required response pattern.
Adapt the heading text and bullet content to the question and available metrics.
If breakdown facts are provided, include the grouped amounts and share percentages explicitly in the response.
Return markdown.
If suitable, append exactly one <CHART>{{...}}</CHART> block after prose.
"""

    async for token in stream_chat_completion(prompt, system_prompt=ANALYSIS_SYSTEM):
        yield token
