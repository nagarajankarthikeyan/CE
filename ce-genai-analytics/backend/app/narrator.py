from app.gpt_client import stream_chat_completion
import json
from datetime import date, datetime, timedelta
from app.time_frame_extractor import extract_time_frame_from_sql, extract_time_frame_from_question

SESSIONS = {}


ANALYSIS_SYSTEM = """You are AskConnie, an expert marketing data analyst for Constellation. You just ran a SQL query for a marketer and got results back. Your job is to present the findings in clear, conversational prose that a non-technical marketer can understand.

## Rules for your analysis
1. Write in natural language - do NOT output raw tables or pipe-delimited data.
2. CRITICAL: Start EVERY response with the date range in the first line. Format: "For [specific dates with month, day, year], ..." or "[Metric] for [specific dates]: [value]". Use the Period facts section to get exact dates.
3. Start with one short scope sentence on what window/slice you summarized.
3. Use this dynamic response pattern (adapt wording to the question):
   - "<time window/topic> — Key takeaways"
   - "Overall performance (all platforms)" when platform/source fields exist, otherwise "Performance snapshot (<time/topic>)"
   - "Platform-by-platform detail" when platform/source fields exist
   - "Data quality notes (important)" when null/unknown/zero-only artifacts appear
   - "Suggested takeaway / next step"
4. Summarize key findings up front, then provide detail.
4a. In "Key takeaways", include at least 4 bullets when data supports it (totals, efficiency, leading driver, and one additional insight).
4b. When available, "Key takeaways" must explicitly include Cost per Enrollment (CPE) before CTR.
5. Use markdown formatting with proper headings and bullet lists on separate lines.
5a. Override for explicit user formatting requests: if the user asks for an "executive summary" in "paragraph form"/"single paragraph"/"one paragraph", return exactly one dense paragraph (no bullets/headings), include the explicit date range in that paragraph, and include key totals + efficiency metrics available in the data.
5c. For executive-summary paragraph responses on relative periods (for example "last week"), start the paragraph with a complete scope clause like: "For the week of <Month Day, Year> through <Month Day, Year>, ...". Do not start with a fragment, heading label, or dangling parenthetical.
5b. For executive-summary paragraph responses on grouped program data (platform/source/channel rows), include concise contribution detail for major groups: each group's spend, enrollments, clicks, impressions, and share of total where computable; explicitly identify primary vs secondary outcome drivers.
6. Format numbers in a human-friendly way: use dollar signs for money ($9,096.49), percentages for rates (12.3%), and abbreviations for large numbers (1.2M).
7. Format dates in a readable way (e.g., "February 21, 2026" not "2026-02-21").
8. If there are trends, comparisons, or outliers, call them out.
9. If data is small (few rows), mention each key point; if large, summarize patterns and highlight top/bottom performers.
10. Only mention metrics present in the query result. Do not invent values.
11. End with a brief takeaway or suggestion if appropriate.
12. Do not output markdown tables.
13. Avoid generic statements; tie every claim to a metric from results.
14. For relative periods like "last week", "this week", "last month", include the explicit date range in the first heading using readable dates.
14a. Use "through" between start and end dates (for example: "February 23, 2026 through March 1, 2026"), not a dash-only date pair.
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
23. Use consistent full platform/source naming everywhere in the response (not just first mention):
   - SA360 -> "SA360 (Search Ads 360)"
   - DV360 -> "DV360 (Display & Video 360)"
   - META -> "META (Facebook/Instagram)"
   - Facebook -> "META (Facebook/Instagram)"
   - Google (when used as paid search source) -> "SA360 (Search Ads 360)"
   Never output bare abbreviations alone in prose sections (including "Chart analysis").
24. For CTR-by-campaign style questions, use this response structure dynamically (adapt labels/period/business line from data):
   - "<Metric> by <dimension> (<period>) — <business line/topic>"
   - "Key takeaways"
   - "Overall performance (all campaigns)"
   - "Campaign-by-campaign highlights"
   - "Highest reach (largest impression drivers)"
   - "Best CTRs (engagement leaders)"
   - platform-specific subsection when present (for example "Meta (Facebook/Instagram) performance")
   - "Lowest CTRs (potential creative/placement mismatch)" when low-CTR rows exist
   - "Practical takeaway"
   Keep bullets concise and metric-backed (impressions, clicks, CTR, spend).

## Important Metric Rules
- For program performance questions, prioritize in this order: spend, total enrollments, cost per enrollment (CPE), enrollment rate, clicks, impressions, CTR, CPC, CPM.
- When CPE is computable, treat it as a primary efficiency KPI and discuss it ahead of CTR.
- For campaign contribution/growth questions, ALWAYS include CPE in the Overall performance section when enrollment data is available.
- When available in query results, always include all three in the response:
- When available in query results, always include all three in the response:
  - Clicks
  - Total Enrollments
  - Enrollment Rate
- Enrollment Rate is mandatory in the final response:
  - include an explicit "Enrollment Rate" line in Key takeaways and overall section,
  - include per-group enrollment rate in breakdown sections when grouped rows exist,
  - if enrollment rate is not directly provided, compute as total_enrollments / clicks * 100 when possible,
  - if it cannot be computed from available fields, still show "Enrollment Rate: N/A" (do not omit it).
- Include enrollment rate in every relevant section:
  - Key takeaways
  - Overall totals/performance
  - Each grouped/platform breakdown row when grouped rows exist.
- Include CPE in every relevant section when computable:
  - Key takeaways
  - Overall totals/performance
  - Each grouped/platform breakdown row when grouped rows exist.
  - If CPE cannot be computed from available fields, show "Cost per Enrollment (CPE): N/A".
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
- For "<metric> by <dimension>" responses (for example CTR by campaign), provide a clear breakout list by that dimension
  and include the metric plus its base drivers when applicable (CTR with clicks/impressions).
- For CTR-by-campaign responses specifically:
  - include overall totals (impressions, clicks, CTR, spend) near the top,
  - include at least one "highest reach" and one "best CTR" breakout list when data supports it,
  - call out lowest-CTR campaign patterns when present,
  - end with a practical recommendation tied to objective (engagement vs awareness).

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
        # Derive robust totals from row-level metric columns so narrative has concrete values
        # even when render_spec has no KPI block (for example, campaign-level result sets).
        spend_key = _find_key(sample, ["total_spend", "spend", "amount_spent", "ad_spend"])
        clicks_key = _find_key(sample, ["total_clicks", "clicks"])
        impr_key = _find_key(sample, ["total_impressions", "impressions"])
        ctr_key = _find_key(sample, ["ctr", "overall_ctr"])
        cpc_key = _find_key(sample, ["cpc", "cost_per_click"])
        cpm_key = _find_key(sample, ["cpm", "cost_per_thousand"])
        enroll_key_any = _find_key(sample, ["total_enrollments", "enrollments", "total_enrollment", "enrollment_count"])

        label_key = _find_key(sample, ["campaign_name", "platform", "datasource", "source", "channel", "dimension", "group_name"])
        total_rows = []
        detail_rows = rows
        if label_key:
            total_rows = [
                r for r in rows
                if str(r.get(label_key, "")).strip().lower() == "total"
            ]
            detail_rows = [
                r for r in rows
                if str(r.get(label_key, "")).strip().lower() != "total"
            ]

        def sum_col(key: str | None):
            if not key:
                return None
            # Prefer explicit TOTAL row when present to avoid double-counting
            # UNION ALL outputs (TOTAL + detail rows).
            for tr in total_rows:
                v = _safe_float(tr.get(key))
                if v is not None:
                    return v
            total = 0.0
            seen = False
            source_rows = detail_rows if total_rows and detail_rows else rows
            for r in source_rows:
                v = _safe_float(r.get(key))
                if v is None:
                    continue
                total += v
                seen = True
            return total if seen else None

        t_spend = sum_col(spend_key)
        t_clicks = sum_col(clicks_key)
        t_impr = sum_col(impr_key)
        t_enroll = sum_col(enroll_key_any)

        if t_spend is not None:
            facts.append(f"- Total spend: ${t_spend:,.2f}")
        if t_spend is not None and t_enroll is not None and t_enroll > 0:
            facts.append(f"- Cost per enrollment (CPE): ${(t_spend / t_enroll):,.2f}")
        if t_clicks is not None:
            facts.append(f"- Total clicks: {t_clicks:,.0f}")
        if t_impr is not None:
            facts.append(f"- Total impressions: {t_impr:,.0f}")
        if t_clicks is not None and t_impr is not None and t_impr > 0:
            facts.append(f"- Overall CTR: {((t_clicks / t_impr) * 100):.2f}%")
        elif ctr_key:
            ctr_total = sum_col(ctr_key)
            if ctr_total is not None:
                facts.append(f"- Overall CTR: {ctr_total:.2f}%")
        cpc_total = sum_col(cpc_key)
        if cpc_total is not None and cpc_key and cpc_key.lower() == "cpc":
            # Prefer weighted CPC when spend and clicks exist.
            if t_spend is not None and t_clicks is not None and t_clicks > 0:
                facts.append(f"- Overall CPC: ${(t_spend / t_clicks):,.2f}")
            else:
                facts.append(f"- Overall CPC: ${cpc_total:,.2f}")
        cpm_total = sum_col(cpm_key)
        if cpm_total is not None and cpm_key and cpm_key.lower() == "cpm":
            if t_spend is not None and t_impr is not None and t_impr > 0:
                facts.append(f"- Overall CPM: ${((t_spend / t_impr) * 1000):,.2f}")
            else:
                facts.append(f"- Overall CPM: ${cpm_total:,.2f}")
        if t_enroll is not None:
            facts.append(f"- Total enrollments: {t_enroll:,.0f}")
            if t_clicks is not None and t_clicks > 0:
                facts.append(f"- Overall enrollment rate: {((t_enroll / t_clicks) * 100):.2f}%")

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
    if t_spend is not None and t_enroll is not None and t_enroll > 0:
        lines.append(f"- Overall cost per enrollment (CPE): ${(t_spend / t_enroll):,.2f}")
    if t_impr is not None:
        lines.append(f"- Total impressions: {t_impr:,.0f}")
    if t_clicks is not None:
        lines.append(f"- Total clicks: {t_clicks:,.0f}")
    if t_clicks is not None and t_impr is not None and t_impr > 0:
        lines.append(f"- Overall CTR: {((t_clicks / t_impr) * 100):.2f}%")
    if t_enroll is not None:
        lines.append(f"- Total enrollments: {t_enroll:,.0f}")
    if t_enroll is not None and t_clicks is not None and t_clicks > 0:
        lines.append(f"- Overall enrollment rate: {((t_enroll / t_clicks) * 100):.2f}%")
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

    # Add rich per-group contribution facts so follow-up executive summaries can be dense and specific.
    grouped_rows = []
    for r in rows:
        p = str(r.get(platform_key, "")).strip() if platform_key else ""
        if not p or p.lower() in {"total", "all"}:
            continue
        s = _safe_float(r.get(spend_key)) if spend_key else None
        i = _safe_float(r.get(impressions_key)) if impressions_key else None
        c = _safe_float(r.get(clicks_key)) if clicks_key else None
        e = _safe_float(r.get(enroll_total_key)) if enroll_total_key else None
        grouped_rows.append(
            {
                "group": p,
                "spend": s or 0.0,
                "impressions": i or 0.0,
                "clicks": c or 0.0,
                "enrollments": e or 0.0,
                "cpe": ((s or 0.0) / (e or 0.0)) if (e or 0.0) > 0 else None,
            }
        )

    if grouped_rows:
        ordered = sorted(grouped_rows, key=lambda x: x["enrollments"], reverse=True)[:6]
        lines.append("- Group contribution detail:")
        for g in ordered:
            spend_share = (g["spend"] / t_spend * 100) if t_spend and t_spend > 0 else None
            enroll_share = (g["enrollments"] / t_enroll * 100) if t_enroll and t_enroll > 0 else None
            click_share = (g["clicks"] / t_clicks * 100) if t_clicks and t_clicks > 0 else None
            impr_share = (g["impressions"] / t_impr * 100) if t_impr and t_impr > 0 else None

            parts = [
                f"{g['group']}: spend ${g['spend']:,.2f}",
                f"impressions {g['impressions']:,.0f}",
                f"clicks {g['clicks']:,.0f}",
                f"enrollments {g['enrollments']:,.0f}",
            ]
            if g["cpe"] is not None:
                parts.append(f"CPE ${g['cpe']:,.2f}")
            else:
                parts.append("CPE N/A")
            if spend_share is not None:
                parts.append(f"spend share {spend_share:.1f}%")
            if enroll_share is not None:
                parts.append(f"enrollment share {enroll_share:.1f}%")
            if click_share is not None:
                parts.append(f"click share {click_share:.1f}%")
            if impr_share is not None:
                parts.append(f"impression share {impr_share:.1f}%")
            lines.append("- " + "; ".join(parts))

    return "\n".join(lines) if lines else "- No program facts derived."


def build_data_availability_facts(rows: list) -> str:
    if not rows:
        return "- Data availability: no rows returned."

    metric_keys = set()
    for r in rows:
        for k in r.keys():
            lk = str(k).strip().lower()
            if any(t in lk for t in ["spend", "click", "impression", "enroll", "ctr", "cpc", "cpm", "cost"]):
                metric_keys.add(k)

    if not metric_keys:
        return f"- Data availability: {len(rows)} row(s) returned, but no recognized metric columns found."

    rows_with_values = 0
    non_null_cells = 0
    for r in rows:
        row_has_metric = False
        for k in metric_keys:
            v = _safe_float(r.get(k))
            if v is not None:
                non_null_cells += 1
                row_has_metric = True
        if row_has_metric:
            rows_with_values += 1

    if rows_with_values > 0:
        return (
            f"- Data availability: metrics are present. "
            f"{rows_with_values} of {len(rows)} row(s) contain non-null metric values "
            f"across {len(metric_keys)} metric column(s)."
        )

    return (
        f"- Data availability: {len(rows)} row(s) returned but all detected metric fields are null."
    )


async def stream_narrative(
    session_id: str,
    question: str,
    rows: list,
    render_spec: dict,
    conversation_history: list[dict] | None = None,
    last_sql: str | None = None,
):
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
    availability_facts = build_data_availability_facts(safe_rows)
    
    # Extract time frame from SQL or question
    time_frame_info = ""
    if last_sql:
        tf = extract_time_frame_from_sql(last_sql)
        if tf and tf.get("description"):
            time_frame_info = f"Time frame: {tf['description']}"
    if not time_frame_info:
        tf_q = extract_time_frame_from_question(question)
        if tf_q and tf_q.get("description"):
            time_frame_info = f"Time frame: {tf_q['description']}"
    
    session = SESSIONS.get(session_id, {})
    if not isinstance(session, dict):
        session = {}
    history = session.get("history", [])
    if not isinstance(history, list):
        history = []
    merged_history = history.copy()
    if isinstance(conversation_history, list):
        for m in conversation_history[-12:]:
            if not isinstance(m, dict):
                continue
            role = m.get("role")
            content = (m.get("content") or "").strip()
            if role in {"user", "assistant"} and content:
                merged_history.append(f"{role.capitalize()}: {content}")
    history_text = "\n".join(merged_history[-6:])  # last 3 turns
    previous_question = session.get("last_question", "")
    if not isinstance(previous_question, str):
        previous_question = ""
    previous_rows = session.get("last_rows", [])
    if not isinstance(previous_rows, list):
        previous_rows = []

    prompt = f"""
The user asked:
{question}

Today's date:
{date.today().isoformat()}

{time_frame_info}

Verified facts (use these exact values when referenced):
{verified_facts}

Period facts (must be reflected when applicable):
{period_facts}

Breakdown facts (if present, use in breakdown section):
{breakdown_facts}

Program performance facts (if present, use for weekly/monthly performance responses):
{program_facts}

Data availability facts (must be respected):
{availability_facts}

Conversation history (for follow-up context):
{history_text}

Previous question:
{previous_question}

Previous result summary:
{json.dumps(previous_rows[:5], default=json_safe)}

Previous SQL (for follow-up context):
{last_sql or ""}

Here are the query results:
{query_result_text}

Structured result:
{json.dumps(safe_render, indent=2)}

Sample rows:
{json.dumps(safe_rows, indent=2)}

Generate a dynamic business summary that follows the required response pattern.
CRITICAL FIRST LINE REQUIREMENT: Your response MUST start with the date range. Examples:
- "For January 1, 2024 through January 31, 2024, total spend was $X..."
- "Total spend for February 1, 2024 through February 7, 2024: $X"
- "For the week of March 4, 2024 through March 10, 2024, ..."
Use the Period facts section above to extract the exact date range. If Period facts shows a date range, use those exact dates in your first line.
Adapt the heading text and bullet content to the question and available metrics.
If breakdown facts are provided, include the grouped amounts and share percentages explicitly in the response.
Mandatory final check before responding: include "Enrollment Rate" explicitly in the output (or "Enrollment Rate: N/A" if not computable).
Mandatory style check for executive-summary requests:
- Begin with a full sentence starting "For the week/month/period ...".
- Include explicit start and end dates using "through".
- Do not start with a heading label like "Executive summary".
Mandatory consistency check:
- If data availability says metrics are present, do not claim "no data", "all null", or "not available".
- If data availability says all metrics are null, explicitly state the limitation.
IMPORTANT: When answering metric questions (like "what is the total spend?"), ALWAYS mention the time frame with SPECIFIC DATES in your response.
If time frame information is provided above, include the exact date range prominently in your answer (e.g., "February 1, 2024 through February 7, 2024").
For follow-up questions without explicit time context, use the period facts to determine and state the date range.
Return markdown.
If suitable, append exactly one <CHART>{{...}}</CHART> block after prose.
"""

    assistant_chunks = []
    async for token in stream_chat_completion(prompt, system_prompt=ANALYSIS_SYSTEM):
        assistant_chunks.append(token)
        yield token

    # Update in-memory session context for next follow-up turn.
    entry = SESSIONS.get(session_id, {})
    if not isinstance(entry, dict):
        entry = {}
    entry_history = entry.get("history", [])
    if not isinstance(entry_history, list):
        entry_history = []
    entry_history.append(f"User: {question}")
    assistant_text = "".join(assistant_chunks).strip()
    if assistant_text:
        entry_history.append(f"Assistant: {assistant_text}")
    if len(entry_history) > 12:
        entry_history = entry_history[-12:]
    entry["history"] = entry_history
    entry["last_question"] = question
    entry["last_rows"] = safe_rows[:20]
    SESSIONS[session_id] = entry
